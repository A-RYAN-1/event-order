// Package eventorder puts out-of-order events back in event-time order.
//
// Events from sensors, message brokers and mobile clients arrive late and
// shuffled. A Buffer holds them until a watermark says no earlier event is
// expected, then releases them sorted by event time. The watermark is the
// largest event time seen so far minus a bounded delay, the same model stream
// processors use, without a stream processor.
//
// A Buffer is not safe for concurrent use; serialise calls with a mutex or a
// single goroutine.
package eventorder

import (
	"container/heap"
	"time"
)

// Event is a value with the time it happened (not the time it arrived).
type Event[T any] struct {
	Time  time.Time
	Value T
	// Late is true when the event arrived after the watermark had passed its
	// time. It is only ever set on events released under LatePolicy Emit.
	Late bool
}

// LatePolicy says what happens to an event that arrives after the watermark
// has passed its event time.
type LatePolicy int

const (
	// Drop discards late events and counts them in Stats.Late.
	Drop LatePolicy = iota
	// Emit returns late events immediately, flagged Late, ahead of the
	// in-order events released by the same call.
	Emit
)

// Option configures a Buffer.
type Option func(*options)

type options struct {
	maxDelay time.Duration
	maxBuf   int
	policy   LatePolicy
}

// WithMaxDelay sets how far out of order events may arrive. Larger values
// tolerate more disorder and hold events longer. The default is zero: only
// events with the same or later time than everything seen are in order.
func WithMaxDelay(d time.Duration) Option {
	return func(o *options) { o.maxDelay = d }
}

// WithMaxBuffered caps how many events are held. When the cap is exceeded the
// oldest events are released early and the watermark moves up to them, so
// memory stays bounded at the price of more late events. Zero means no cap.
func WithMaxBuffered(n int) Option {
	return func(o *options) { o.maxBuf = n }
}

// WithLatePolicy chooses what happens to late events. The default is Drop.
func WithLatePolicy(p LatePolicy) Option {
	return func(o *options) { o.policy = p }
}

// Stats is a snapshot of a Buffer's counters.
type Stats struct {
	Buffered  int       // events currently held
	Released  uint64    // events released in order
	Late      uint64    // events that arrived late (dropped or emitted)
	Watermark time.Time // zero until the first event or AdvanceTo
}

type item[T any] struct {
	ev  Event[T]
	seq uint64 // arrival order, breaks ties so equal times keep their order
}

type minHeap[T any] []item[T]

func (h minHeap[T]) Len() int { return len(h) }
func (h minHeap[T]) Less(i, j int) bool {
	if h[i].ev.Time.Equal(h[j].ev.Time) {
		return h[i].seq < h[j].seq
	}
	return h[i].ev.Time.Before(h[j].ev.Time)
}
func (h minHeap[T]) Swap(i, j int) { h[i], h[j] = h[j], h[i] }
func (h *minHeap[T]) Push(x any)   { *h = append(*h, x.(item[T])) }
func (h *minHeap[T]) Pop() any {
	old := *h
	n := len(old)
	it := old[n-1]
	old[n-1] = item[T]{} // let the value be collected
	*h = old[:n-1]
	return it
}

// Buffer reorders events by event time.
type Buffer[T any] struct {
	opt      options
	h        minHeap[T]
	seq      uint64
	maxSeen  time.Time
	wm       time.Time
	hasWM    bool
	released uint64
	late     uint64
}

// New returns an empty Buffer.
func New[T any](opts ...Option) *Buffer[T] {
	b := &Buffer[T]{}
	for _, o := range opts {
		o(&b.opt)
	}
	if b.opt.maxDelay < 0 {
		b.opt.maxDelay = 0
	}
	return b
}

// Add inserts an event and returns the events that are now safe to process,
// in event-time order. The slice is empty when nothing can be released yet.
// Under the Emit late policy a late event is returned first with Late set.
func (b *Buffer[T]) Add(t time.Time, v T) []Event[T] {
	var out []Event[T]
	if b.hasWM && !t.After(b.wm) {
		b.late++
		if b.opt.policy == Emit {
			out = append(out, Event[T]{Time: t, Value: v, Late: true})
		}
		return out
	}
	b.seq++
	heap.Push(&b.h, item[T]{ev: Event[T]{Time: t, Value: v}, seq: b.seq})
	if t.After(b.maxSeen) {
		b.maxSeen = t
	}
	// One nanosecond below the bound makes "latest minus delay" inclusive of
	// events at exactly that time, as in Flink's bounded-out-of-orderness.
	b.raise(b.maxSeen.Add(-b.opt.maxDelay).Add(-time.Nanosecond))
	out = b.drain(out)
	for b.opt.maxBuf > 0 && b.h.Len() > b.opt.maxBuf {
		it := heap.Pop(&b.h).(item[T])
		b.raise(it.ev.Time)
		b.released++
		out = append(out, it.ev)
		out = b.drain(out)
	}
	return out
}

// AdvanceTo moves the watermark up to t without an event, for example from a
// heartbeat or a timer when a source goes quiet. It never moves backwards. It
// returns the events released by the move.
func (b *Buffer[T]) AdvanceTo(t time.Time) []Event[T] {
	b.raise(t)
	return b.drain(nil)
}

// Flush releases everything still held, in order, for shutdown or end of
// input. The watermark moves to the last released event.
func (b *Buffer[T]) Flush() []Event[T] {
	var out []Event[T]
	for b.h.Len() > 0 {
		it := heap.Pop(&b.h).(item[T])
		b.raise(it.ev.Time)
		b.released++
		out = append(out, it.ev)
	}
	return out
}

// Stats returns the current counters.
func (b *Buffer[T]) Stats() Stats {
	return Stats{Buffered: b.h.Len(), Released: b.released, Late: b.late, Watermark: b.wm}
}

func (b *Buffer[T]) raise(t time.Time) {
	if !b.hasWM || t.After(b.wm) {
		b.wm, b.hasWM = t, true
	}
}

func (b *Buffer[T]) drain(out []Event[T]) []Event[T] {
	for b.h.Len() > 0 && !b.h[0].ev.Time.After(b.wm) {
		it := heap.Pop(&b.h).(item[T])
		b.released++
		out = append(out, it.ev)
	}
	return out
}
