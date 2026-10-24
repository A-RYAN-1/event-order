// Command kafka reads readings from a Kafka topic, drops repeats, puts each
// partition's readings back in event-time order and commits only offsets whose
// readings have been processed, so a crash loses nothing.
//
//	go run . -brokers localhost:9092 -topic readings -group demo
//
// Each message value is JSON: {"id":"r-1","t":"2026-01-01T12:00:03Z","v":21.5}.
// The consumer logic lives in consume, which takes a small interface instead of
// a *kafka.Reader so the tests run without a broker.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"time"

	eventorder "github.com/A-RYAN-1/event-order"
	"github.com/segmentio/kafka-go"
)

// Reading is the message payload.
type Reading struct {
	ID    string    `json:"id"`
	Time  time.Time `json:"t"`
	Value float64   `json:"v"`
}

// reader is the part of *kafka.Reader that consume uses.
type reader interface {
	FetchMessage(ctx context.Context) (kafka.Message, error)
	CommitMessages(ctx context.Context, msgs ...kafka.Message) error
}

// held is what the buffer keeps per event: the reading and where it came from.
type held struct {
	offset  int64
	reading Reading
}

type config struct {
	maxDelay time.Duration
	window   time.Duration
}

// consume runs until ctx is cancelled or the reader fails. handle receives
// each reading once, in event-time order per partition. On a clean stop it
// flushes what it holds, handles it and commits; on a crash the commit stays
// at the first reading not yet handled.
func consume(ctx context.Context, r reader, cfg config, handle func(partition int, rd Reading)) error {
	type part struct {
		topic     string
		committed int64
	}
	buffers := eventorder.NewKeyed[int, held](eventorder.WithMaxDelay(cfg.maxDelay))
	tracker := eventorder.NewKeyedTracker[int]()
	dedup := eventorder.NewDedup[string](cfg.window, 100_000)
	parts := map[int]*part{}

	// A reading that arrives after the watermark is dropped; it is finished.
	buffers.OnLate(func(p int, e eventorder.Event[held]) { tracker.Done(p, e.Value.offset) })

	commit := func(p int) error {
		next, ok := tracker.Commit(p)
		st := parts[p]
		if !ok || next <= st.committed {
			return nil
		}
		// kafka-go commits Offset+1 of the message it is given.
		if err := r.CommitMessages(ctx, kafka.Message{Topic: st.topic, Partition: p, Offset: next - 1}); err != nil {
			return err
		}
		st.committed = next
		return nil
	}
	finish := func(p int, evs []eventorder.Event[held]) {
		for _, e := range evs {
			handle(p, e.Value.reading)
			tracker.Done(p, e.Value.offset)
		}
	}

	for {
		msg, err := r.FetchMessage(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				break
			}
			return err
		}
		p := msg.Partition
		if parts[p] == nil {
			parts[p] = &part{topic: msg.Topic, committed: -1}
		}
		tracker.Track(p, msg.Offset)
		var rd Reading
		if err := json.Unmarshal(msg.Value, &rd); err != nil || dedup.Seen(rd.Time, rd.ID) {
			tracker.Done(p, msg.Offset) // unreadable or repeated: finished
		} else {
			finish(p, buffers.Add(p, rd.Time, held{offset: msg.Offset, reading: rd}))
		}
		if err := commit(p); err != nil {
			return err
		}
	}

	// Clean stop: process everything still held, then commit.
	buffers.FlushAll(func(p int, evs []eventorder.Event[held]) {
		finish(p, evs)
	})
	for p := range parts {
		if err := commit(p); err != nil {
			return err
		}
	}
	return nil
}

func main() {
	brokers := flag.String("brokers", "localhost:9092", "comma separated brokers")
	topic := flag.String("topic", "readings", "topic")
	group := flag.String("group", "event-order-demo", "consumer group")
	delay := flag.Duration("delay", 5*time.Second, "how far out of order readings may arrive")
	window := flag.Duration("dedup-window", time.Minute, "how long ids are remembered")
	flag.Parse()

	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers: strings.Split(*brokers, ","),
		Topic:   *topic,
		GroupID: *group,
	})
	defer r.Close()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	err := consume(ctx, r, config{maxDelay: *delay, window: *window}, func(p int, rd Reading) {
		fmt.Printf("partition %d  %s  %s  %.1f\n", p, rd.Time.Format(time.RFC3339), rd.ID, rd.Value)
	})
	if err != nil {
		log.Fatal(err)
	}
}
