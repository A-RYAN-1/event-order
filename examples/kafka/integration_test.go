//go:build integration

package main

// Real-broker tests. They need a Kafka broker, for example
//
//	docker run -d --name kafka -p 9092:9092 apache/kafka:3.9.0
//	KAFKA_BROKERS=localhost:9092 go test -tags integration -count=1 -v .
//
// CI runs them against the same image.

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"net"
	"os"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"
)

func brokers(t *testing.T) string {
	t.Helper()
	b := os.Getenv("KAFKA_BROKERS")
	if b == "" {
		t.Skip("KAFKA_BROKERS not set")
	}
	return b
}

func createTopic(t *testing.T, broker, topic string, partitions int) {
	t.Helper()
	conn, err := kafka.Dial("tcp", broker)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	ctrl, err := conn.Controller()
	if err != nil {
		t.Fatal(err)
	}
	cc, err := kafka.Dial("tcp", net.JoinHostPort(ctrl.Host, strconv.Itoa(ctrl.Port)))
	if err != nil {
		t.Fatal(err)
	}
	defer cc.Close()
	if err := cc.CreateTopics(kafka.TopicConfig{Topic: topic, NumPartitions: partitions, ReplicationFactor: 1}); err != nil {
		t.Fatal(err)
	}
	// Creation returns before every partition has a leader; wait for them.
	waitFor(t, "topic partitions", 30*time.Second, func() bool {
		ps, err := conn.ReadPartitions(topic)
		if err != nil || len(ps) != partitions {
			return false
		}
		for _, p := range ps {
			if p.Leader.ID < 0 || p.Error != nil {
				return false
			}
		}
		return true
	})
}

// produce writes readings shuffled inside blocks of 4 seconds, round robin over
// the partitions, with some readings sent twice.
func produce(t *testing.T, broker, topic string, parts, perPart, from int, r *rand.Rand) {
	t.Helper()
	w := &kafka.Writer{
		Addr:                   kafka.TCP(broker),
		Topic:                  topic,
		Balancer:               &kafka.Hash{},
		RequiredAcks:           kafka.RequireAll,
		AllowAutoTopicCreation: false,
	}
	defer w.Close()
	var msgs []kafka.Message
	for p := 0; p < parts; p++ {
		for blk := from; blk < from+perPart; blk += 4 {
			for _, i := range r.Perm(4) {
				sec := blk + i
				v, _ := json.Marshal(Reading{ID: fmt.Sprintf("p%d-%d", p, sec), Time: t0.Add(time.Duration(sec) * time.Second), Value: float64(sec)})
				msgs = append(msgs, kafka.Message{Key: []byte(keyFor(p, parts)), Value: v})
				if r.Intn(8) == 0 {
					msgs = append(msgs, kafka.Message{Key: []byte(keyFor(p, parts)), Value: v}) // delivered twice
				}
			}
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	// A fresh topic can answer "unknown topic" for a moment after creation.
	var err error
	for i := 0; i < 20; i++ {
		if err = w.WriteMessages(ctx, msgs...); err == nil {
			return
		}
		time.Sleep(500 * time.Millisecond)
	}
	t.Fatal(err)
}

// keyFor returns a key the Hash balancer sends to partition p of n.
func keyFor(p, n int) string {
	h := &kafka.Hash{}
	parts := make([]int, n)
	for i := range parts {
		parts[i] = i
	}
	for i := 0; ; i++ {
		k := "k" + strconv.Itoa(i)
		if h.Balance(kafka.Message{Key: []byte(k)}, parts...) == p {
			return k
		}
	}
}

type tally struct {
	mu   sync.Mutex
	seen map[string]int
}

func (ta *tally) add(rd Reading) {
	ta.mu.Lock()
	ta.seen[rd.ID]++
	ta.mu.Unlock()
}

func (ta *tally) count() int {
	ta.mu.Lock()
	defer ta.mu.Unlock()
	return len(ta.seen)
}

// worker consumes until ctx ends, reconnecting after an error the way a real
// service does: a rebalance can fail a commit, and the next reader resumes
// from the committed offset.
func worker(ctx context.Context, broker, topic, group string, ta *tally, cfg config) {
	for ctx.Err() == nil {
		r := kafka.NewReader(kafka.ReaderConfig{
			Brokers:           []string{broker},
			Topic:             topic,
			GroupID:           group,
			MinBytes:          1,
			MaxBytes:          1 << 20,
			MaxWait:           200 * time.Millisecond,
			StartOffset:       kafka.FirstOffset,
			CommitInterval:    0,
			SessionTimeout:    6 * time.Second,
			HeartbeatInterval: time.Second,
			RebalanceTimeout:  6 * time.Second,
		})
		_ = consume(ctx, r, cfg, func(_ int, rd Reading) { ta.add(rd) })
		r.Close()
	}
}

func waitFor(t *testing.T, what string, d time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

// A consumer reads part of the topic and stops without flushing; a second one in
// the same group continues from the committed offsets. Every reading shows up.
func TestIntegrationRestartFromCommittedOffsets(t *testing.T) {
	broker := brokers(t)
	topic := fmt.Sprintf("it-restart-%d", time.Now().UnixNano())
	const parts, per = 2, 40
	createTopic(t, broker, topic, parts)
	produce(t, broker, topic, parts, per, 0, rand.New(rand.NewSource(1)))

	cfg := config{maxDelay: 5 * time.Second, window: time.Hour, idle: 2 * time.Second}
	ta := &tally{seen: map[string]int{}}
	group := topic + "-g"

	// First consumer: a reader wrapper that fails after n messages, no flush.
	r := kafka.NewReader(kafka.ReaderConfig{Brokers: []string{broker}, Topic: topic, GroupID: group, SessionTimeout: 6 * time.Second, HeartbeatInterval: time.Second, RebalanceTimeout: 6 * time.Second, MaxBytes: 1 << 20, MaxWait: 200 * time.Millisecond, StartOffset: kafka.FirstOffset})
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	start := time.Now()
	err := consume(ctx, &limited{reader: r, left: 30}, cfg, func(_ int, rd Reading) { ta.add(rd) })
	t.Logf("first consumer ended after %v with %v, %d readings handled", time.Since(start), err, ta.count())
	r.Close()
	t.Logf("closed after %v", time.Since(start))
	if err == nil {
		t.Fatal("first consumer was expected to crash")
	}
	if ta.count() >= parts*per {
		t.Fatal("crash came too late to test anything")
	}

	cctx, ccancel := context.WithCancel(ctx)
	go worker(cctx, broker, topic, group, ta, cfg)
	waitFor(t, "every reading after the restart", 60*time.Second, func() bool { return ta.count() == parts*per })
	ccancel()
}

// Two consumers of one group: the second joins while the first is reading, the
// partitions are rebalanced between them, and every reading is still handled.
func TestIntegrationRebalanceLosesNothing(t *testing.T) {
	broker := brokers(t)
	topic := fmt.Sprintf("it-rebalance-%d", time.Now().UnixNano())
	const parts, per = 2, 80
	createTopic(t, broker, topic, parts)
	r := rand.New(rand.NewSource(2))
	produce(t, broker, topic, parts, per/2, 0, r)

	cfg := config{maxDelay: 5 * time.Second, window: time.Hour, idle: 2 * time.Second}
	ta := &tally{seen: map[string]int{}}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	group := topic + "-g"

	actx, acancel := context.WithCancel(ctx)
	go worker(actx, broker, topic, group, ta, cfg)
	waitFor(t, "first consumer to start reading", 30*time.Second, func() bool { return ta.count() > 0 })

	bctx, bcancel := context.WithCancel(ctx)
	go worker(bctx, broker, topic, group, ta, cfg) // joins: rebalance
	time.Sleep(3 * time.Second)
	produce(t, broker, topic, parts, per/2, per/2, r)

	waitFor(t, "every reading across the rebalance", 70*time.Second, func() bool { return ta.count() == parts*per })
	acancel()
	bcancel()
}

// limited fails FetchMessage after left messages, as a killed process would.
type limited struct {
	reader *kafka.Reader
	left   int
}

func (l *limited) FetchMessage(ctx context.Context) (kafka.Message, error) {
	if l.left == 0 {
		return kafka.Message{}, errCrash
	}
	l.left--
	return l.reader.FetchMessage(ctx)
}

func (l *limited) CommitMessages(ctx context.Context, msgs ...kafka.Message) error {
	return l.reader.CommitMessages(ctx, msgs...)
}
