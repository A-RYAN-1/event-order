package eventorder

import (
	"slices"
	"time"
)

// refBuffer is the specification written the dumb way: a slice that is sorted
// on every step. It shares no code with Buffer. The tests run both on the same
// operations and demand identical output, call for call.
type refBuffer struct {
	delay   time.Duration
	cap     int
	emit    bool
	held    []refItem
	seq     int
	maxSeen time.Time
	wm      time.Time
	hasWM   bool
}

type refItem struct {
	ev  Event[int]
	seq int
}

func (r *refBuffer) sort() {
	slices.SortStableFunc(r.held, func(a, b refItem) int {
		if c := a.ev.Time.Compare(b.ev.Time); c != 0 {
			return c
		}
		return a.seq - b.seq
	})
}

func (r *refBuffer) raise(t time.Time) {
	if !r.hasWM || t.After(r.wm) {
		r.wm, r.hasWM = t, true
	}
}

// release takes every held event at or before the watermark.
func (r *refBuffer) release(out []Event[int]) []Event[int] {
	r.sort()
	n := 0
	for n < len(r.held) && !r.held[n].ev.Time.After(r.wm) {
		out = append(out, r.held[n].ev)
		n++
	}
	r.held = r.held[n:]
	return out
}

func (r *refBuffer) add(t time.Time, v int) []Event[int] {
	if r.hasWM && !t.After(r.wm) {
		if r.emit {
			return []Event[int]{{Time: t, Value: v, Late: true}}
		}
		return nil
	}
	r.seq++
	r.held = append(r.held, refItem{Event[int]{Time: t, Value: v}, r.seq})
	if t.After(r.maxSeen) {
		r.maxSeen = t
	}
	r.raise(r.maxSeen.Add(-r.delay).Add(-time.Nanosecond))
	out := r.release(nil)
	for r.cap > 0 && len(r.held) > r.cap {
		r.sort()
		first := r.held[0]
		r.held = r.held[1:]
		r.raise(first.ev.Time)
		out = append(out, first.ev)
		out = r.release(out)
	}
	return out
}

func (r *refBuffer) advanceTo(t time.Time) []Event[int] {
	r.raise(t)
	return r.release(nil)
}

func (r *refBuffer) flush() []Event[int] {
	r.sort()
	var out []Event[int]
	for _, it := range r.held {
		r.raise(it.ev.Time)
		out = append(out, it.ev)
	}
	r.held = nil
	return out
}

func (r *refBuffer) options() []Option {
	p := Drop
	if r.emit {
		p = Emit
	}
	return []Option{WithMaxDelay(r.delay), WithMaxBuffered(r.cap), WithLatePolicy(p)}
}
