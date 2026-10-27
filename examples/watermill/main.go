// Command watermill shows event-order inside a Watermill router. Messages on
// the input topic arrive in any order; the handler holds them, and the output
// topic receives them in event-time order.
//
//	go run .
//
// The event time travels in the message metadata under the key event_time, in
// RFC 3339 format. The demo uses the in-memory gochannel pub/sub; replace it
// with Kafka, NATS, RabbitMQ or any other Watermill publisher and subscriber.
//
// Limit: Watermill acknowledges the input message when the handler returns, and
// delivers the next one only after that. A message the Reorderer is holding is
// therefore already acknowledged, and a crash loses it. Where that matters,
// consume with the broker's own client and use eventorder.Stream, which keeps
// the offset uncommitted until the event is processed (examples/kafka).
package main

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"time"

	eventorder "github.com/A-RYAN-1/event-order"
	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/ThreeDotsLabs/watermill/pubsub/gochannel"
)

// EventTimeKey is the metadata key that carries the event time.
const EventTimeKey = "event_time"

// Reorderer is a Watermill handler that releases messages in event-time order.
// It is safe for the concurrent handler calls the router may make.
type Reorderer struct {
	buf    *eventorder.Safe[*message.Message]
	logger watermill.LoggerAdapter
}

// NewReorderer returns a Reorderer that tolerates messages up to maxDelay out
// of order.
func NewReorderer(maxDelay time.Duration, logger watermill.LoggerAdapter) *Reorderer {
	if logger == nil {
		logger = watermill.NopLogger{}
	}
	return &Reorderer{
		buf:    eventorder.NewSafe[*message.Message](eventorder.WithMaxDelay(maxDelay)),
		logger: logger,
	}
}

// Handle is a message.HandlerFunc. It returns the messages that became ready,
// oldest first, for the router to publish. A message without a valid event time
// is dropped and logged: returning an error would make the broker redeliver it
// forever.
func (r *Reorderer) Handle(msg *message.Message) ([]*message.Message, error) {
	t, err := time.Parse(time.RFC3339Nano, msg.Metadata.Get(EventTimeKey))
	if err != nil {
		r.logger.Error("dropping message without a valid event_time", err, watermill.LogFields{"uuid": msg.UUID})
		return nil, nil
	}
	return toMessages(r.buf.Add(t, msg)), nil
}

// Drain publishes everything held to topic once no message has arrived for
// idle, so the last messages of a quiet topic are not stranded. It runs until
// ctx ends and then publishes what is left.
func (r *Reorderer) Drain(ctx context.Context, pub message.Publisher, topic string, idle time.Duration) {
	tick := time.NewTicker(idle / 2)
	defer tick.Stop()
	last := arrivals(r.buf.Stats())
	quiet := 0
	for {
		select {
		case <-ctx.Done():
			r.publish(pub, topic, r.buf.Flush())
			return
		case <-tick.C:
			// The arrival count changes while messages flow; no change over two
			// ticks with events still held means the topic went quiet.
			st := r.buf.Stats()
			if n := arrivals(st); st.Buffered == 0 || n != last {
				last, quiet = n, 0
				continue
			}
			if quiet++; quiet >= 2 {
				r.publish(pub, topic, r.buf.Flush())
				quiet = 0
			}
		}
	}
}

// arrivals is every message the buffer has seen: released, late or still held.
func arrivals(st eventorder.Stats) uint64 {
	return st.Released + st.Late + uint64(st.Buffered)
}

func (r *Reorderer) publish(pub message.Publisher, topic string, evs []eventorder.Event[*message.Message]) {
	for _, m := range toMessages(evs) {
		if err := pub.Publish(topic, m); err != nil {
			r.logger.Error("publishing drained message", err, watermill.LogFields{"uuid": m.UUID})
		}
	}
}

func toMessages(evs []eventorder.Event[*message.Message]) []*message.Message {
	if len(evs) == 0 {
		return nil
	}
	out := make([]*message.Message, len(evs))
	for i, e := range evs {
		// Publish a copy: the router acknowledges the input message itself.
		m := message.NewMessage(e.Value.UUID, e.Value.Payload)
		m.Metadata = e.Value.Metadata
		out[i] = m
	}
	return out
}

func main() {
	logger := watermill.NopLogger{}
	// Blocking publish keeps messages in publish order; by default gochannel
	// delivers each one from its own goroutine, which would reorder them.
	pubsub := gochannel.NewGoChannel(gochannel.Config{BlockPublishUntilSubscriberAck: true}, logger)
	router, err := message.NewRouter(message.RouterConfig{}, logger)
	if err != nil {
		log.Fatal(err)
	}
	reorderer := NewReorderer(5*time.Second, logger)
	router.AddHandler("reorder", "readings", pubsub, "readings.ordered", pubsub, reorderer.Handle)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	out, err := pubsub.Subscribe(ctx, "readings.ordered")
	if err != nil {
		log.Fatal(err)
	}
	go func() {
		if err := router.Run(ctx); err != nil {
			log.Print(err)
		}
	}()
	<-router.Running()
	go reorderer.Drain(ctx, pubsub, "readings.ordered", time.Second)

	base := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	r := rand.New(rand.NewSource(1))
	go func() {
		for blk := 0; blk < 12; blk += 4 {
			for _, i := range r.Perm(4) {
				m := message.NewMessage(watermill.NewUUID(), []byte(fmt.Sprintf("reading %d", blk+i)))
				m.Metadata.Set(EventTimeKey, base.Add(time.Duration(blk+i)*time.Second).Format(time.RFC3339Nano))
				if err := pubsub.Publish("readings", m); err != nil {
					log.Print(err)
				}
			}
		}
	}()

	for n := 0; n < 12; n++ {
		m := <-out
		fmt.Printf("%s  %s\n", m.Metadata.Get(EventTimeKey), m.Payload)
		m.Ack()
	}
}
