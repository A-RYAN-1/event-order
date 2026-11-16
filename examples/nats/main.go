// Command nats reads readings from a NATS JetStream stream with a pull
// consumer, drops repeats, puts them back in event-time order and acknowledges
// only messages whose readings have been processed, so a crash loses nothing:
// unacknowledged messages are delivered again after the consumer's AckWait.
//
//	go run . -url nats://localhost:4222 -stream READINGS -consumer demo
//
// Each message carries its identity and event time in headers, Reading-Id and
// Event-Time (RFC 3339), and the value as the body: {"v":21.5}. The stream
// sequence number is the offset: it grows with every message and stays the
// same when JetStream delivers a message again.
//
// The consumer logic lives in consume, which takes a small interface instead
// of a jetstream.Consumer so the tests run without a server.
//
// Set the consumer's AckWait above the buffer's maximum delay plus the idle
// time. A message held longer than AckWait is delivered again; consume
// recognises it by its sequence number and keeps one copy, but the server
// counts the extra delivery.
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
	"time"

	eventorder "github.com/A-RYAN-1/event-order"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// Reading is the decoded message.
type Reading struct {
	ID    string
	Time  time.Time
	Value float64
}

// message is the part of a JetStream message that consume uses.
type message interface {
	Seq() uint64
	Header(name string) string
	Data() []byte
	// Ack returns once the server has recorded the acknowledgement.
	Ack(ctx context.Context) error
}

// source is the part of a pull consumer that consume uses.
type source interface {
	// Fetch returns the next messages, waiting up to the idle time; an empty
	// result with a nil error means nothing arrived.
	Fetch(ctx context.Context) ([]message, error)
}

type config struct {
	maxDelay time.Duration
	window   time.Duration
	// idle releases everything held after a fetch that returns nothing, so the
	// last readings of a quiet stream are not stranded.
	idle bool
}

func decode(m message) (Reading, error) {
	t, err := time.Parse(time.RFC3339, m.Header("Event-Time"))
	if err != nil {
		return Reading{}, fmt.Errorf("event time: %w", err)
	}
	var body struct {
		V float64 `json:"v"`
	}
	if err := json.Unmarshal(m.Data(), &body); err != nil {
		return Reading{}, err
	}
	return Reading{ID: m.Header("Reading-Id"), Time: t, Value: body.V}, nil
}

// consume runs until ctx is cancelled or the source fails. handle receives
// each reading once, in event-time order. On a clean stop it flushes what it
// holds, handles it and acknowledges; on a crash the messages not yet handled
// stay unacknowledged and come back.
func consume(ctx context.Context, src source, cfg config, handle func(rd Reading)) error {
	const key = "stream" // one stream, one tracker key
	stream := eventorder.NewStream[string, Reading](
		eventorder.WithBuffer(eventorder.WithMaxDelay(cfg.maxDelay)),
		eventorder.WithDedup(cfg.window, 100_000))
	held := map[int64]message{} // stream sequence to its newest delivery, until acknowledged
	var acked int64             // sequences below this are acknowledged

	ack := func() error {
		upto, ok := stream.Commit(key)
		if !ok || upto <= acked {
			return nil
		}
		for seq, m := range held {
			if seq >= upto {
				continue
			}
			// An acknowledgement that has started finishes even when ctx is cancelled.
			if err := m.Ack(context.WithoutCancel(ctx)); err != nil {
				return err
			}
			delete(held, seq)
		}
		acked = upto
		return nil
	}
	drain := func() error {
		stream.Flush(func(_ string, rec eventorder.Record[Reading]) { handle(rec.Value) })
		return ack()
	}

	for {
		msgs, err := src.Fetch(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				break
			}
			return err
		}
		if len(msgs) == 0 {
			if cfg.idle {
				if err := drain(); err != nil {
					return err
				}
			}
			continue
		}
		for _, m := range msgs {
			off := int64(m.Seq())
			if _, again := held[off]; again {
				held[off] = m // delivered again while held: acknowledge through the newest delivery
				continue
			}
			held[off] = m
			rd, err := decode(m)
			if err != nil {
				stream.Skip(key, off) // unreadable: finished, so the ack moves past it
				continue
			}
			for _, rec := range stream.Ingest(key, off, rd.Time, rd.ID, rd) {
				handle(rec.Value)
				stream.Done(key, rec.Offset)
			}
		}
		if err := ack(); err != nil {
			return err
		}
	}

	// Clean stop: process everything still held, then acknowledge.
	return drain()
}

// pull fetches batches from a JetStream pull consumer.
type pull struct {
	cons  jetstream.Consumer
	batch int
	wait  time.Duration
}

type jsMessage struct {
	jetstream.Msg
	seq uint64
}

func (m jsMessage) Seq() uint64                   { return m.seq }
func (m jsMessage) Header(name string) string     { return m.Msg.Headers().Get(name) }
func (m jsMessage) Data() []byte                  { return m.Msg.Data() }
func (m jsMessage) Ack(ctx context.Context) error { return m.DoubleAck(ctx) }

func (p *pull) Fetch(ctx context.Context) ([]message, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	b, err := p.cons.Fetch(p.batch, jetstream.FetchMaxWait(p.wait))
	if err != nil {
		return nil, err
	}
	var out []message
	for m := range b.Messages() {
		md, err := m.Metadata()
		if err != nil {
			return nil, err
		}
		out = append(out, jsMessage{Msg: m, seq: md.Sequence.Stream})
	}
	// A fetch that finds nothing before MaxWait ends without messages; that is
	// the idle case, not an error.
	if err := b.Error(); err != nil && !errors.Is(err, nats.ErrTimeout) && !errors.Is(err, jetstream.ErrNoMessages) {
		return nil, err
	}
	return out, nil
}

func main() {
	url := flag.String("url", nats.DefaultURL, "NATS server URL")
	name := flag.String("stream", "READINGS", "JetStream stream name")
	cname := flag.String("consumer", "event-order-demo", "durable consumer name")
	delay := flag.Duration("delay", 5*time.Second, "how far out of order readings may arrive")
	window := flag.Duration("dedup-window", time.Minute, "how long ids are remembered")
	idle := flag.Duration("idle", 30*time.Second, "release held readings after this much silence")
	flag.Parse()

	nc, err := nats.Connect(*url)
	if err != nil {
		log.Fatal(err)
	}
	defer nc.Close()
	js, err := jetstream.New(nc)
	if err != nil {
		log.Fatal(err)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	cons, err := js.CreateOrUpdateConsumer(ctx, *name, jetstream.ConsumerConfig{
		Durable:    *cname,
		AckPolicy:  jetstream.AckExplicitPolicy,
		AckWait:    *delay + *idle + time.Minute,
		MaxDeliver: -1,
	})
	if err != nil {
		log.Fatal(err)
	}
	wait := *idle
	if wait <= 0 {
		wait = 30 * time.Second
	}
	err = consume(ctx, &pull{cons: cons, batch: 100, wait: wait}, config{maxDelay: *delay, window: *window, idle: *idle > 0}, func(rd Reading) {
		fmt.Printf("%s  %s  %.1f\n", rd.Time.Format(time.RFC3339), rd.ID, rd.Value)
	})
	if err != nil {
		log.Fatal(err)
	}
}
