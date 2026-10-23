package eventorder

import "time"

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
	idle     time.Duration
	strategy func() DelayStrategy
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

// WithIdleTimeout makes Pipe release everything it holds when no event has
// arrived for d of wall-clock time, so a source that goes quiet does not strand
// its last events. Zero, the default, waits for input or Flush. Buffer and
// Keyed ignore it: they have no clock, drive them with AdvanceTo.
func WithIdleTimeout(d time.Duration) Option {
	return func(o *options) { o.idle = d }
}

// Stats is a snapshot of a Buffer's counters.
type Stats struct {
	Buffered  int       // events currently held
	Released  uint64    // events released in order
	Late      uint64    // events that arrived late (dropped or emitted)
	Peak      int       // most events held at once, to size WithMaxBuffered
	Watermark time.Time // zero until the first event or AdvanceTo
}

type item[T any] struct {
	ev  Event[T]
	seq uint64 // arrival order, breaks ties so equal times keep their order
}

// minHeap is a typed binary heap ordered by event time, then arrival. It is
// written out instead of using container/heap so pushes and pops do not box
// every item in an interface value.
type minHeap[T any] []item[T]

func (h minHeap[T]) less(i, j int) bool {
	if h[i].ev.Time.Equal(h[j].ev.Time) {
		return h[i].seq < h[j].seq
	}
	return h[i].ev.Time.Before(h[j].ev.Time)
}

func (h *minHeap[T]) push(it item[T]) {
	*h = append(*h, it)
	s := *h
	for i := len(s) - 1; i > 0; {
		p := (i - 1) / 2
		if !s.less(i, p) {
			break
		}
		s[i], s[p] = s[p], s[i]
		i = p
	}
}

func (h *minHeap[T]) pop() item[T] {
	s := *h
	top := s[0]
	n := len(s) - 1
	s[0] = s[n]
	s[n] = item[T]{} // let the value be collected
	s = s[:n]
	for i := 0; ; {
		l, small := 2*i+1, i
		if l < n && s.less(l, small) {
			small = l
		}
		if r := l + 1; r < n && s.less(r, small) {
			small = r
		}
		if small == i {
			break
		}
		s[i], s[small] = s[small], s[i]
		i = small
	}
	*h = s
	return top
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
	peak     int
	onLate   func(Event[T])
	strat    DelayStrategy
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
	if b.opt.strategy != nil {
		b.strat = b.opt.strategy()
	}
	return b
}

// Add inserts an event and returns the events that are now safe to process,
// in event-time order. The slice is empty when nothing can be released yet.
// Under the Emit late policy a late event is returned first with Late set.
func (b *Buffer[T]) Add(t time.Time, v T) []Event[T] {
	return b.AddTo(nil, t, v)
}

// AddTo is Add appending to dst, for a hot path that reuses one slice:
//
//	out = b.AddTo(out[:0], t, v)
//
// Once the slice has grown to the largest burst, Add costs no allocation.
func (b *Buffer[T]) AddTo(dst []Event[T], t time.Time, v T) []Event[T] {
	out := dst
	delay := b.opt.maxDelay
	if b.strat != nil {
		var lag time.Duration
		if t.Before(b.maxSeen) {
			lag = b.maxSeen.Sub(t)
		}
		b.strat.Observe(lag)
		delay = max(delay, b.strat.Delay())
	}
	if b.hasWM && !t.After(b.wm) {
		b.late++
		ev := Event[T]{Time: t, Value: v, Late: true}
		if b.onLate != nil {
			b.onLate(ev)
		}
		if b.opt.policy == Emit {
			out = append(out, ev)
		}
		return out
	}
	b.seq++
	b.h.push(item[T]{ev: Event[T]{Time: t, Value: v}, seq: b.seq})
	if len(b.h) > b.peak {
		b.peak = len(b.h)
	}
	if t.After(b.maxSeen) {
		b.maxSeen = t
	}
	// One nanosecond below the bound makes "latest minus delay" inclusive of
	// events at exactly that time, as in Flink's bounded-out-of-orderness.
	b.raise(b.maxSeen.Add(-delay).Add(-time.Nanosecond))
	out = b.drain(out)
	for b.opt.maxBuf > 0 && len(b.h) > b.opt.maxBuf {
		it := b.h.pop()
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
	for len(b.h) > 0 {
		it := b.h.pop()
		b.raise(it.ev.Time)
		b.released++
		out = append(out, it.ev)
	}
	return out
}

// OnLate sets a function called for every late event, whatever the policy,
// so dropped events can go to a dead-letter queue or a metric instead of
// vanishing. It runs inside Add; keep it fast. A nil fn clears it.
func (b *Buffer[T]) OnLate(fn func(Event[T])) { b.onLate = fn }

// Len is the number of events currently held.
func (b *Buffer[T]) Len() int { return len(b.h) }

// Watermark is the time up to which events have been released; the zero time
// until the first event or AdvanceTo.
func (b *Buffer[T]) Watermark() time.Time { return b.wm }

// Stats returns the current counters.
func (b *Buffer[T]) Stats() Stats {
	return Stats{Buffered: len(b.h), Released: b.released, Late: b.late, Peak: b.peak, Watermark: b.wm}
}

func (b *Buffer[T]) raise(t time.Time) {
	if !b.hasWM || t.After(b.wm) {
		b.wm, b.hasWM = t, true
	}
}

func (b *Buffer[T]) drain(out []Event[T]) []Event[T] {
	for len(b.h) > 0 && !b.h[0].ev.Time.After(b.wm) {
		it := b.h.pop()
		b.released++
		out = append(out, it.ev)
	}
	return out
}
