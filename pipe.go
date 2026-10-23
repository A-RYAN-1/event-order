package eventorder

import (
	"context"
	"time"
)

// Pipe reads events in arrival order from in and sends them in event-time
// order on the returned channel. When in is closed, what is still held is
// flushed in order and the output closes. When ctx is cancelled the output
// closes at once and held events are discarded. With WithIdleTimeout, a source
// that stays quiet for that long has everything it holds released.
//
// The caller must drain the returned channel or cancel ctx; the goroutine
// stops when either happens.
func Pipe[T any](ctx context.Context, in <-chan Event[T], opts ...Option) <-chan Event[T] {
	out := make(chan Event[T])
	b := New[T](opts...)
	idle := b.opt.idle
	go func() {
		defer close(out)
		send := func(evs []Event[T]) bool {
			for _, e := range evs {
				select {
				case out <- e:
				case <-ctx.Done():
					return false
				}
			}
			return true
		}
		var timer *time.Timer
		var tick <-chan time.Time
		if idle > 0 {
			timer = time.NewTimer(idle)
			defer timer.Stop()
			tick = timer.C
		}
		var buf []Event[T]
		for {
			select {
			case <-ctx.Done():
				return
			case ev, ok := <-in:
				if !ok {
					send(b.Flush())
					return
				}
				buf = b.AddTo(buf[:0], ev.Time, ev.Value)
				if !send(buf) {
					return
				}
				if timer != nil {
					if !timer.Stop() {
						select {
						case <-timer.C:
						default:
						}
					}
					timer.Reset(idle)
				}
			case <-tick:
				if !send(b.Flush()) {
					return
				}
				timer.Reset(idle)
			}
		}
	}()
	return out
}
