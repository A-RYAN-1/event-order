package eventorder

import "iter"

// Reorder wraps a sequence of events in arrival order and yields them in
// event-time order, holding at most what opts allow. When the input ends, the
// rest is flushed in order. Stopping the loop early stops reading the input.
func Reorder[T any](seq iter.Seq[Event[T]], opts ...Option) iter.Seq[Event[T]] {
	return func(yield func(Event[T]) bool) {
		b := New[T](opts...)
		for ev := range seq {
			for _, out := range b.Add(ev.Time, ev.Value) {
				if !yield(out) {
					return
				}
			}
		}
		for _, out := range b.Flush() {
			if !yield(out) {
				return
			}
		}
	}
}
