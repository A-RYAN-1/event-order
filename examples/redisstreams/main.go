// Command redisstreams reads readings from a Redis Stream through a consumer
// group, drops repeats, puts them back in event-time order and acknowledges
// only entries whose readings have been processed, so a crash loses nothing:
// unacknowledged entries stay in the group's pending list and are delivered
// again on the next start.
//
//	go run . -addr localhost:6379 -stream readings -group demo -consumer c1
//
// Each entry has one field, data, holding JSON:
// {"id":"r-1","t":"2026-01-01T12:00:03Z","v":21.5}.
// The consumer logic lives in consume, which takes a small interface instead of
// a *redis.Client so the tests run without a server.
//
// Entry IDs ("1700000000000-0") are not integers, so each entry gets a local
// sequence number as its offset; the tracker works on those and consume maps
// them back to IDs when it acknowledges.
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
	"github.com/redis/go-redis/v9"
)

// Reading is the entry payload.
type Reading struct {
	ID    string    `json:"id"`
	Time  time.Time `json:"t"`
	Value float64   `json:"v"`
}

// entry is one stream entry: its ID and the data field.
type entry struct {
	ID   string
	Data string
}

// source is the part of a consumer-group client that consume uses.
type source interface {
	// Read returns the next entries, waiting up to the idle time; an empty
	// result with a nil error means nothing arrived.
	Read(ctx context.Context) ([]entry, error)
	// Ack acknowledges entries in the consumer group.
	Ack(ctx context.Context, ids ...string) error
}

type config struct {
	maxDelay time.Duration
	window   time.Duration
	// idle releases everything held after a read that returns nothing, so the
	// last readings of a quiet stream are not stranded.
	idle bool
}

// consume runs until ctx is cancelled or the source fails. handle receives
// each reading once, in event-time order. On a clean stop it flushes what it
// holds, handles it and acknowledges; on a crash the entries not yet handled
// stay pending in the group.
func consume(ctx context.Context, src source, cfg config, handle func(rd Reading)) error {
	const key = "stream" // one stream, one tracker key
	stream := eventorder.NewStream[string, Reading](
		eventorder.WithBuffer(eventorder.WithMaxDelay(cfg.maxDelay)),
		eventorder.WithDedup(cfg.window, 100_000))
	ids := map[int64]string{} // local offset to entry ID, until acknowledged
	var next, acked int64     // next offset to hand out, first offset not yet acknowledged

	ack := func() error {
		upto, ok := stream.Commit(key)
		if !ok || upto <= acked {
			return nil
		}
		batch := make([]string, 0, upto-acked)
		for o := acked; o < upto; o++ {
			batch = append(batch, ids[o])
		}
		// An acknowledgement that has started finishes even when ctx is cancelled.
		if err := src.Ack(context.WithoutCancel(ctx), batch...); err != nil {
			return err
		}
		for o := acked; o < upto; o++ {
			delete(ids, o)
		}
		acked = upto
		return nil
	}
	drain := func() error {
		stream.Flush(func(_ string, rec eventorder.Record[Reading]) { handle(rec.Value) })
		return ack()
	}

	for {
		entries, err := src.Read(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				break
			}
			return err
		}
		if len(entries) == 0 {
			if cfg.idle {
				if err := drain(); err != nil {
					return err
				}
			}
			continue
		}
		for _, e := range entries {
			off := next
			next++
			ids[off] = e.ID
			var rd Reading
			if err := json.Unmarshal([]byte(e.Data), &rd); err != nil {
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

// group reads one consumer group of a stream with go-redis.
type group struct {
	rdb      *redis.Client
	stream   string
	group    string
	consumer string
	block    time.Duration
	count    int64
	// pending is true until the consumer's own pending list has been read to
	// the end; entries delivered before a crash come back first. XREADGROUP
	// with an ID returns entries after it, so cursor moves along the list.
	pending bool
	cursor  string
}

func (g *group) Read(ctx context.Context) ([]entry, error) {
	id := ">"
	block := g.block
	if g.pending {
		if g.cursor == "" {
			g.cursor = "0"
		}
		id, block = g.cursor, -1 // -1: do not block on the pending list
	}
	res, err := g.rdb.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group: g.group, Consumer: g.consumer, Streams: []string{g.stream, id},
		Count: g.count, Block: block,
	}).Result()
	if errors.Is(err, redis.Nil) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var out []entry
	for _, s := range res {
		for _, m := range s.Messages {
			d, _ := m.Values["data"].(string)
			out = append(out, entry{ID: m.ID, Data: d})
		}
	}
	if g.pending {
		if len(out) == 0 {
			g.pending = false
		} else {
			g.cursor = out[len(out)-1].ID
		}
	}
	return out, nil
}

func (g *group) Ack(ctx context.Context, ids ...string) error {
	return g.rdb.XAck(ctx, g.stream, g.group, ids...).Err()
}

func main() {
	addr := flag.String("addr", "localhost:6379", "Redis address")
	name := flag.String("stream", "readings", "stream key")
	grp := flag.String("group", "event-order-demo", "consumer group")
	consumer := flag.String("consumer", "c1", "consumer name")
	delay := flag.Duration("delay", 5*time.Second, "how far out of order readings may arrive")
	window := flag.Duration("dedup-window", time.Minute, "how long ids are remembered")
	idle := flag.Duration("idle", 30*time.Second, "release held readings after this much silence")
	flag.Parse()

	rdb := redis.NewClient(&redis.Options{Addr: *addr})
	defer func() { _ = rdb.Close() }()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	// "0" starts a new group at the beginning of the stream; an existing group
	// answers BUSYGROUP, which is fine.
	if err := rdb.XGroupCreateMkStream(ctx, *name, *grp, "0").Err(); err != nil && !isBusyGroup(err) {
		log.Fatal(err)
	}
	g := &group{rdb: rdb, stream: *name, group: *grp, consumer: *consumer, block: *idle, count: 100, pending: true}
	err := consume(ctx, g, config{maxDelay: *delay, window: *window, idle: *idle > 0}, func(rd Reading) {
		fmt.Printf("%s  %s  %.1f\n", rd.Time.Format(time.RFC3339), rd.ID, rd.Value)
	})
	if err != nil {
		log.Fatal(err)
	}
}

func isBusyGroup(err error) bool {
	return err != nil && err.Error() == "BUSYGROUP Consumer Group name already exists"
}
