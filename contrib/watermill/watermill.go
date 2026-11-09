// Package watermill is a Watermill handler that publishes messages in
// event-time order, built on event-order. It lives in its own module so the
// root module keeps zero dependencies.
//
//	re := watermill.NewReorderer(5*time.Second, watermill.WithLogger(logger))
//	router.AddHandler("reorder", "in", sub, "out", pub, re.Handle)
//	go re.Drain(ctx, pub, "out", time.Second)
//
// The event time travels in message metadata under EventTimeKey (RFC 3339).
//
// Limit: Watermill acknowledges the input message when the handler returns, so
// a message the Reorderer is holding is already acknowledged, and a crash loses
// it. Where that matters, consume with the broker's own client and use
// eventorder.Stream, which keeps the offset uncommitted until the event is
// processed (examples/kafka).
package watermill

import (
	"context"
	"time"

	eventorder "github.com/A-RYAN-1/event-order"
	wm "github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
)

// EventTimeKey is the default metadata key that carries the event time.
const EventTimeKey = "event_time"

// Option configures a Reorderer.
type Option func(*Reorderer)

// WithLogger sets the logger; the default discards everything.
func WithLogger(l wm.LoggerAdapter) Option {
	return func(r *Reorderer) {
		if l != nil {
			r.logger = l
		}
	}
}

// WithTimeKey reads the event time from another metadata key.
func WithTimeKey(key string) Option { return func(r *Reorderer) { r.key = key } }

// WithBuffer passes options to the underlying event-order buffer, such as
// eventorder.WithMaxBuffered or eventorder.WithLatePolicy.
func WithBuffer(opts ...eventorder.Option) Option {
	return func(r *Reorderer) { r.bufOpts = append(r.bufOpts, opts...) }
}

// Reorderer is a Watermill handler that releases messages in event-time order.
// It is safe for the concurrent handler calls the router may make.
type Reorderer struct {
	buf     *eventorder.Safe[*message.Message]
	logger  wm.LoggerAdapter
	key     string
	bufOpts []eventorder.Option
}

// NewReorderer returns a Reorderer that tolerates messages up to maxDelay out
// of order.
func NewReorderer(maxDelay time.Duration, opts ...Option) *Reorderer {
	r := &Reorderer{logger: wm.NopLogger{}, key: EventTimeKey}
	for _, o := range opts {
		o(r)
	}
	all := append([]eventorder.Option{eventorder.WithMaxDelay(maxDelay)}, r.bufOpts...)
	r.buf = eventorder.NewSafe[*message.Message](all...)
	return r
}

// Stats is the buffer's current counters.
func (r *Reorderer) Stats() eventorder.Stats { return r.buf.Stats() }

// Handle is a message.HandlerFunc. It returns the messages that became ready,
// oldest first, for the router to publish. A message without a valid event time
// is dropped and logged: returning an error would make the broker redeliver it
// forever. Under the Emit late policy a late message is returned at once.
func (r *Reorderer) Handle(msg *message.Message) ([]*message.Message, error) {
	t, err := time.Parse(time.RFC3339Nano, msg.Metadata.Get(r.key))
	if err != nil {
		r.logger.Error("dropping message without a valid event time", err, wm.LogFields{"uuid": msg.UUID, "key": r.key})
		return nil, nil
	}
	return toMessages(r.buf.Add(t, msg)), nil
}

// Drain publishes everything held to topic once no message has arrived for
// idle, so the last messages of a quiet topic are not stranded. It runs until
// ctx ends and then publishes what is left, which is the flush at shutdown.
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
			r.logger.Error("publishing drained message", err, wm.LogFields{"uuid": m.UUID})
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
