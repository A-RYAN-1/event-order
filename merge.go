package eventorder

import "iter"

// Merge combines sources that are each already in event-time order into one
// sequence in event-time order, for example several partitions or devices after
// Reorder. Events with equal times come out in source order, then in the order
// within the source. The sources are read one event at a time, so memory is
// one event per source. Stopping the loop early stops reading every source.
//
// A source that is not itself in order is not fixed: its events are passed
// along in the order they come, which can leave the output out of order. Run
// such a source through Reorder first.
func Merge[T any](sources ...iter.Seq[Event[T]]) iter.Seq[Event[T]] {
	return func(yield func(Event[T]) bool) {
		type head struct {
			ev   Event[T]
			next func() (Event[T], bool)
			src  int
		}
		var heads []head
		for i, s := range sources {
			next, stop := iter.Pull(s)
			defer stop()
			if ev, ok := next(); ok {
				heads = append(heads, head{ev, next, i})
			}
		}
		less := func(a, b head) bool {
			if a.ev.Time.Equal(b.ev.Time) {
				return a.src < b.src
			}
			return a.ev.Time.Before(b.ev.Time)
		}
		down := func(i int) {
			for {
				l, small := 2*i+1, i
				if l < len(heads) && less(heads[l], heads[small]) {
					small = l
				}
				if r := l + 1; r < len(heads) && less(heads[r], heads[small]) {
					small = r
				}
				if small == i {
					return
				}
				heads[i], heads[small] = heads[small], heads[i]
				i = small
			}
		}
		for i := len(heads)/2 - 1; i >= 0; i-- {
			down(i)
		}
		for len(heads) > 0 {
			top := &heads[0]
			if !yield(top.ev) {
				return
			}
			if ev, ok := top.next(); ok {
				top.ev = ev
			} else {
				last := len(heads) - 1
				heads[0] = heads[last]
				heads[last] = head{}
				heads = heads[:last]
			}
			down(0)
		}
	}
}
