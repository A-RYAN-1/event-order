//go:build integration

package main

// Real-server tests. They need Redis, for example
//
//	docker run -d --name redis -p 6379:6379 redis:7
//	REDIS_ADDR=localhost:6379 go test -tags integration -count=1 -v -run Integration .
//
// CI runs them against the same image.

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

func client(t *testing.T) *redis.Client {
	t.Helper()
	addr := os.Getenv("REDIS_ADDR")
	if addr == "" {
		t.Skip("REDIS_ADDR not set")
	}
	rdb := redis.NewClient(&redis.Options{Addr: addr})
	t.Cleanup(func() { _ = rdb.Close() })
	return rdb
}

// fill adds n readings shuffled inside blocks of four seconds, with every tenth
// one added twice, and returns the stream key.
func fill(t *testing.T, rdb *redis.Client, n int, seed int64) string {
	t.Helper()
	ctx := context.Background()
	key := fmt.Sprintf("readings-%d-%d", seed, time.Now().UnixNano())
	t.Cleanup(func() { rdb.Del(ctx, key) })
	r := rand.New(rand.NewSource(seed))
	add := func(sec int) {
		e := ent(0, fmt.Sprintf("r-%d", sec), sec)
		if err := rdb.XAdd(ctx, &redis.XAddArgs{Stream: key, Values: map[string]any{"data": e.Data}}).Err(); err != nil {
			t.Fatal(err)
		}
	}
	for blk := 0; blk < n; blk += 4 {
		for _, i := range r.Perm(4) {
			add(blk + i)
			if (blk+i)%10 == 0 {
				add(blk + i)
			}
		}
	}
	return key
}

func newGroup(t *testing.T, rdb *redis.Client, key string) *group {
	t.Helper()
	if err := rdb.XGroupCreateMkStream(context.Background(), key, "g", "0").Err(); err != nil {
		t.Fatal(err)
	}
	return &group{rdb: rdb, stream: key, group: "g", consumer: "c1", block: 200 * time.Millisecond, count: 7, pending: true}
}

func pendingCount(t *testing.T, rdb *redis.Client, key string) int64 {
	t.Helper()
	p, err := rdb.XPending(context.Background(), key, "g").Result()
	if err != nil {
		t.Fatal(err)
	}
	return p.Count
}

// stopAfterIdle ends consume cleanly once the stream has been read to the end.
type stopAfterIdle struct {
	*group
	cancel context.CancelFunc
}

func (s stopAfterIdle) Read(ctx context.Context) ([]entry, error) {
	wasPending := s.group.pending
	es, err := s.group.Read(ctx)
	if err == nil && len(es) == 0 && !wasPending { // a quiet live read: the stream is read to the end
		s.cancel()
	}
	return es, err
}

func TestIntegrationOrdersDedupsAndAcknowledges(t *testing.T) {
	rdb := client(t)
	const n = 200
	key := fill(t, rdb, n, 1)
	g := newGroup(t, rdb, key)

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	var got []string
	var last time.Time
	err := consume(ctx, stopAfterIdle{g, cancel}, config{maxDelay: 5 * time.Second, window: time.Hour, idle: true}, func(rd Reading) {
		if rd.Time.Before(last) {
			t.Errorf("%s handled after a later reading", rd.ID)
		}
		last = rd.Time
		got = append(got, rd.ID)
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != n {
		t.Fatalf("handled %d readings, want %d (duplicates dropped)", len(got), n)
	}
	if c := pendingCount(t, rdb, key); c != 0 {
		t.Fatalf("%d entries still pending after a clean stop", c)
	}
}

// failAfter makes consume fail after reads reads, as a crash would.
type failAfter struct {
	*group
	reads int
}

func (f *failAfter) Read(ctx context.Context) ([]entry, error) {
	if f.reads == 0 {
		return nil, errors.New("crash")
	}
	f.reads--
	return f.group.Read(ctx)
}

func TestIntegrationRestartSeesEveryReading(t *testing.T) {
	rdb := client(t)
	const n = 200
	key := fill(t, rdb, n, 2)
	g := newGroup(t, rdb, key)
	seen := map[string]int{}
	cfg := config{maxDelay: 5 * time.Second, window: time.Hour, idle: true}

	err := consume(context.Background(), &failAfter{group: g, reads: 9}, cfg, func(rd Reading) { seen[rd.ID]++ })
	if err == nil || err.Error() != "crash" {
		t.Fatalf("first run ended with %v", err)
	}
	if pendingCount(t, rdb, key) == 0 {
		t.Fatal("nothing pending after the crash: the held readings were acknowledged")
	}

	// A new consumer process: same group, pending list first.
	g2 := &group{rdb: rdb, stream: key, group: "g", consumer: "c1", block: 200 * time.Millisecond, count: 7, pending: true}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	if err := consume(ctx, stopAfterIdle{g2, cancel}, cfg, func(rd Reading) { seen[rd.ID]++ }); err != nil {
		t.Fatal(err)
	}
	for s := 0; s < n; s++ {
		if id := fmt.Sprintf("r-%d", s); seen[id] == 0 {
			t.Fatalf("lost %s", id)
		}
	}
	if c := pendingCount(t, rdb, key); c != 0 {
		t.Fatalf("%d entries still pending after the second run", c)
	}
}
