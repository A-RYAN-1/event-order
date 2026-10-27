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

type config struct {
	maxDelay time.Duration
	window   time.Duration
	// idle releases everything held after this long without a message, so the
	// last readings of a quiet topic are not stranded. Zero never does.
	idle time.Duration
}

// consume runs until ctx is cancelled or the reader fails. handle receives
// each reading once, in event-time order per partition. On a clean stop it
// flushes what it holds, handles it and commits; on a crash the commit stays
// at the first reading not yet handled.
func consume(ctx context.Context, r reader, cfg config, handle func(partition int, rd Reading)) error {
	stream := eventorder.NewStream[int, Reading](
		eventorder.WithBuffer(eventorder.WithMaxDelay(cfg.maxDelay)),
		eventorder.WithDedup(cfg.window, 100_000))
	topics := map[int]string{}
	committed := map[int]int64{}

	commit := func(p int) error {
		next, ok := stream.Commit(p)
		if !ok || next <= committed[p] {
			return nil
		}
		// kafka-go commits Offset+1 of the message it is given.
		if err := r.CommitMessages(ctx, kafka.Message{Topic: topics[p], Partition: p, Offset: next - 1}); err != nil {
			return err
		}
		committed[p] = next
		return nil
	}
	finish := func(p int, recs []eventorder.Record[Reading]) {
		for _, rec := range recs {
			handle(p, rec.Value)
			stream.Done(p, rec.Offset)
		}
	}

	// drain handles everything still held and commits it.
	drain := func() error {
		stream.Flush(func(p int, rec eventorder.Record[Reading]) { handle(p, rec.Value) })
		for p := range topics {
			if err := commit(p); err != nil {
				return err
			}
		}
		return nil
	}

	for {
		fctx, cancel := ctx, context.CancelFunc(func() {})
		if cfg.idle > 0 {
			fctx, cancel = context.WithTimeout(ctx, cfg.idle)
		}
		msg, err := r.FetchMessage(fctx)
		cancel()
		if err != nil {
			if ctx.Err() == nil && errors.Is(err, context.DeadlineExceeded) {
				if err := drain(); err != nil { // idle: nothing arrived for cfg.idle
					return err
				}
				continue
			}
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				break
			}
			return err
		}
		p := msg.Partition
		if _, seen := topics[p]; !seen {
			topics[p] = msg.Topic
		}
		var rd Reading
		if err := json.Unmarshal(msg.Value, &rd); err != nil {
			stream.Skip(p, msg.Offset) // unreadable: finished, so the commit moves past it
		} else {
			finish(p, stream.Ingest(p, msg.Offset, rd.Time, rd.ID, rd))
		}
		if err := commit(p); err != nil {
			return err
		}
	}

	// Clean stop: process everything still held, then commit.
	return drain()
}

func main() {
	brokers := flag.String("brokers", "localhost:9092", "comma separated brokers")
	topic := flag.String("topic", "readings", "topic")
	group := flag.String("group", "event-order-demo", "consumer group")
	delay := flag.Duration("delay", 5*time.Second, "how far out of order readings may arrive")
	window := flag.Duration("dedup-window", time.Minute, "how long ids are remembered")
	idle := flag.Duration("idle", 30*time.Second, "release held readings after this much silence")
	flag.Parse()

	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers: strings.Split(*brokers, ","),
		Topic:   *topic,
		GroupID: *group,
	})
	defer func() { _ = r.Close() }()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	err := consume(ctx, r, config{maxDelay: *delay, window: *window, idle: *idle}, func(p int, rd Reading) {
		fmt.Printf("partition %d  %s  %s  %.1f\n", p, rd.Time.Format(time.RFC3339), rd.ID, rd.Value)
	})
	if err != nil {
		log.Fatal(err)
	}
}
