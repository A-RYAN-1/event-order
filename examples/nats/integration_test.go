//go:build integration

package main

// Real-server tests. They need NATS with JetStream, for example
//
//	docker run -d --name nats -p 4222:4222 nats:2 -js
//	NATS_URL=nats://localhost:4222 go test -tags integration -count=1 -v -run Integration .
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

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

const ackWait = 2 * time.Second

// setup connects, creates a fresh stream and returns a durable pull consumer.
func setup(t *testing.T, n int, seed int64) (jetstream.JetStream, jetstream.Consumer, string) {
	t.Helper()
	url := os.Getenv("NATS_URL")
	if url == "" {
		t.Skip("NATS_URL not set")
	}
	nc, err := nats.Connect(url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(nc.Close)
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	name := fmt.Sprintf("READINGS%d%d", seed, time.Now().UnixNano())
	if _, err := js.CreateStream(ctx, jetstream.StreamConfig{Name: name, Subjects: []string{name + ".>"}}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = js.DeleteStream(ctx, name) })

	// n readings shuffled inside blocks of four seconds, every tenth sent twice.
	r := rand.New(rand.NewSource(seed))
	pub := func(sec int) {
		m := nats.NewMsg(name + ".r")
		m.Header.Set("Reading-Id", fmt.Sprintf("r-%d", sec))
		m.Header.Set("Event-Time", t0.Add(time.Duration(sec)*time.Second).Format(time.RFC3339))
		m.Data = []byte(fmt.Sprintf(`{"v":%d}`, sec))
		if _, err := js.PublishMsg(ctx, m); err != nil {
			t.Fatal(err)
		}
	}
	for blk := 0; blk < n; blk += 4 {
		for _, i := range r.Perm(4) {
			pub(blk + i)
			if (blk+i)%10 == 0 {
				pub(blk + i)
			}
		}
	}
	cons, err := js.CreateOrUpdateConsumer(ctx, name, jetstream.ConsumerConfig{
		Durable: "g", AckPolicy: jetstream.AckExplicitPolicy, AckWait: ackWait,
	})
	if err != nil {
		t.Fatal(err)
	}
	return js, cons, name
}

func unacked(t *testing.T, cons jetstream.Consumer) int {
	t.Helper()
	info, err := cons.Info(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	return info.NumAckPending + int(info.NumPending)
}

// stopWhenQuiet ends consume cleanly after a fetch finds nothing.
type stopWhenQuiet struct {
	*pull
	cancel context.CancelFunc
}

func (s stopWhenQuiet) Fetch(ctx context.Context) ([]message, error) {
	ms, err := s.pull.Fetch(ctx)
	if err == nil && len(ms) == 0 {
		s.cancel()
	}
	return ms, err
}

func TestIntegrationOrdersDedupsAndAcknowledges(t *testing.T) {
	const n = 200
	_, cons, _ := setup(t, n, 1)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	var got []string
	var last time.Time
	src := stopWhenQuiet{&pull{cons: cons, batch: 7, wait: 500 * time.Millisecond}, cancel}
	err := consume(ctx, src, config{maxDelay: 5 * time.Second, window: time.Hour, idle: true}, func(rd Reading) {
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
	if c := unacked(t, cons); c != 0 {
		t.Fatalf("%d messages unacknowledged or undelivered after a clean stop", c)
	}
}

// failAfter makes consume fail after fetches fetches, as a crash would.
type failAfter struct {
	*pull
	fetches int
}

func (f *failAfter) Fetch(ctx context.Context) ([]message, error) {
	if f.fetches == 0 {
		return nil, errors.New("crash")
	}
	f.fetches--
	return f.pull.Fetch(ctx)
}

func TestIntegrationRestartSeesEveryReading(t *testing.T) {
	const n = 200
	_, cons, _ := setup(t, n, 2)
	seen := map[string]int{}
	cfg := config{maxDelay: 5 * time.Second, window: time.Hour, idle: true}

	err := consume(context.Background(), &failAfter{pull: &pull{cons: cons, batch: 7, wait: 500 * time.Millisecond}, fetches: 9}, cfg, func(rd Reading) { seen[rd.ID]++ })
	if err == nil || err.Error() != "crash" {
		t.Fatalf("first run ended with %v", err)
	}
	if unacked(t, cons) == 0 {
		t.Fatal("nothing unacknowledged after the crash: the held readings were acknowledged")
	}

	// A new consumer process: the unacknowledged messages come back after AckWait.
	time.Sleep(ackWait + time.Second)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	src := stopWhenQuiet{&pull{cons: cons, batch: 7, wait: 500 * time.Millisecond}, cancel}
	if err := consume(ctx, src, cfg, func(rd Reading) { seen[rd.ID]++ }); err != nil {
		t.Fatal(err)
	}
	for s := 0; s < n; s++ {
		if id := fmt.Sprintf("r-%d", s); seen[id] == 0 {
			t.Fatalf("lost %s", id)
		}
	}
	if c := unacked(t, cons); c != 0 {
		t.Fatalf("%d messages unacknowledged after the second run", c)
	}
}
