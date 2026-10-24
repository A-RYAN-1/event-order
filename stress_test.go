package eventorder

import (
	"sync"
	"testing"
	"time"
)

// The stress tests are meant for `go test -race`: several goroutines hammer
// every method of the Safe types at once. Without the race detector they still
// check that nothing is lost, duplicated or released out of order.

type tally struct {
	mu   sync.Mutex
	seen map[int]int
}

func (c *tally) add(t *testing.T, evs []Event[int]) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for i, e := range evs {
		c.seen[e.Value]++
		if i > 0 && !e.Late && !evs[i-1].Late && e.Time.Before(evs[i-1].Time) {
			t.Errorf("a single call released out of order: %v before %v", evs[i-1].Time, e.Time)
		}
	}
}

func (c *tally) expectEach(t *testing.T, total int) {
	t.Helper()
	for v := 0; v < total; v++ {
		if c.seen[v] != 1 {
			t.Fatalf("event %d came out %d times, want 1", v, c.seen[v])
		}
	}
	if len(c.seen) != total {
		t.Fatalf("%d distinct events came out, want %d", len(c.seen), total)
	}
}

func TestSafeStress(t *testing.T) {
	const writers, perWriter = 8, 2000
	base := time.Unix(1_700_000_000, 0)
	s := NewSafe[int](WithMaxDelay(50*time.Millisecond), WithMaxBuffered(64), WithLatePolicy(Emit))
	var lateCalls int
	var lmu sync.Mutex
	s.OnLate(func(Event[int]) { lmu.Lock(); lateCalls++; lmu.Unlock() })
	c := &tally{seen: map[int]int{}}

	stop := make(chan struct{})
	var side sync.WaitGroup
	side.Add(1)
	go func() { // timer and observer goroutine
		defer side.Done()
		i := 0
		for {
			select {
			case <-stop:
				return
			default:
			}
			i++
			c.add(t, s.AdvanceTo(base.Add(time.Duration(i)*time.Millisecond)))
			_ = s.Stats()
			if i%50 == 0 {
				c.add(t, s.Flush())
			}
		}
	}()

	var wg sync.WaitGroup
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			var dst []Event[int]
			for i := 0; i < perWriter; i++ {
				ts := base.Add(time.Duration((i*7+w*13)%400) * time.Millisecond)
				dst = s.AddTo(dst[:0], ts, w*perWriter+i)
				c.add(t, dst)
			}
		}(w)
	}
	wg.Wait()
	close(stop)
	side.Wait()
	c.add(t, s.Flush())
	c.expectEach(t, writers*perWriter)

	st := s.Stats()
	if st.Buffered != 0 {
		t.Fatalf("buffer not empty after Flush: %d held", st.Buffered)
	}
	lmu.Lock()
	defer lmu.Unlock()
	if int(st.Late) != lateCalls {
		t.Fatalf("Stats.Late = %d but OnLate ran %d times", st.Late, lateCalls)
	}
}

func TestSafeKeyedStress(t *testing.T) {
	const writers, perWriter, keys = 8, 1500, 6
	base := time.Unix(1_700_000_000, 0)
	s := NewSafeKeyed[int, int](WithMaxDelay(50*time.Millisecond), WithMaxBuffered(32), WithLatePolicy(Emit))
	c := &tally{seen: map[int]int{}}

	stop := make(chan struct{})
	var side sync.WaitGroup
	side.Add(1)
	go func() {
		defer side.Done()
		i := 0
		for {
			select {
			case <-stop:
				return
			default:
			}
			i++
			s.AdvanceAll(base.Add(time.Duration(i)*time.Millisecond), func(_ int, evs []Event[int]) { c.add(t, evs) })
			_ = s.Len()
			_, _ = s.Stats(i % keys)
			if i%40 == 0 {
				c.add(t, s.Flush(i%keys))
			}
		}
	}()

	var wg sync.WaitGroup
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < perWriter; i++ {
				ts := base.Add(time.Duration((i*11+w*5)%300) * time.Millisecond)
				c.add(t, s.Add((i+w)%keys, ts, w*perWriter+i))
			}
		}(w)
	}
	wg.Wait()
	close(stop)
	side.Wait()
	s.FlushAll(func(_ int, evs []Event[int]) { c.add(t, evs) })
	c.expectEach(t, writers*perWriter)
	if s.Len() != 0 {
		t.Fatalf("%d keys left after FlushAll", s.Len())
	}
}
