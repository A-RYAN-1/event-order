package eventorder

import (
	"math/rand"
	"runtime"
	"testing"
	"time"
)

func TestCapBoundsHeldEvents(t *testing.T) {
	const cap = 500
	r := rand.New(rand.NewSource(1))
	b := New[int](WithMaxDelay(1000*time.Hour), WithMaxBuffered(cap))
	for i := 0; i < 200_000; i++ {
		b.Add(t0.Add(time.Duration(r.Intn(1_000_000))*time.Millisecond), i)
		if b.Len() > cap {
			t.Fatalf("after %d adds the buffer holds %d events, cap %d", i+1, b.Len(), cap)
		}
	}
	// An event is pushed before the oldest is popped, so Peak can be one over.
	if p := b.Stats().Peak; p > cap+1 {
		t.Fatalf("Peak %d exceeds cap+1 = %d", p, cap+1)
	}
}

// With events one second apart, shuffled only inside blocks of delay+1
// seconds, nothing is late and no more than delay+1 events are held between
// calls (Peak counts the arriving event before the watermark moves, so one more).
func TestHeldEventsFollowTheDisorderWindow(t *testing.T) {
	for _, delay := range []int{0, 1, 5, 30, 120} {
		r := rand.New(rand.NewSource(int64(delay)))
		b := New[int](WithMaxDelay(time.Duration(delay) * time.Second))
		block := delay + 1
		var out int
		for start := 0; start < 20_000; start += block {
			for _, o := range r.Perm(block) {
				out += len(b.Add(at(start+o), start+o))
				if b.Len() > block {
					t.Fatalf("delay %ds: holding %d between calls, bound is %d", delay, b.Len(), block)
				}
			}
		}
		out += len(b.Flush())
		st := b.Stats()
		if st.Late != 0 {
			t.Fatalf("delay %ds: %d late events, want 0", delay, st.Late)
		}
		if st.Peak > block+1 {
			t.Fatalf("delay %ds: Peak %d, bound is %d", delay, st.Peak, block+1)
		}
		if out < 20_000 {
			t.Fatalf("delay %ds: %d events out, want at least 20000", delay, out)
		}
	}
}

// The heap itself stays small: two million events through a cap of 1000 leave
// about the memory of a thousand events, not two million. The memory owned by
// the buffer is the heap with it alive minus the heap once it is dropped. The
// bound is loose on purpose so it does not depend on the Go version; the
// measured value is logged.
func TestHeapDoesNotGrowWithStreamLength(t *testing.T) {
	const cap, n = 1000, 2_000_000
	// The buffer lives only inside feed, so it is garbage once feed returns.
	feed := func() uint64 {
		r := rand.New(rand.NewSource(3))
		b := New[int](WithMaxDelay(1000*time.Hour), WithMaxBuffered(cap))
		var dst []Event[int]
		for i := 0; i < n; i++ {
			dst = b.AddTo(dst[:0], t0.Add(time.Duration(r.Intn(1_000_000))*time.Millisecond), i)
		}
		var m runtime.MemStats
		runtime.GC()
		runtime.ReadMemStats(&m)
		runtime.KeepAlive(b)
		return m.HeapAlloc
	}
	alive := feed()
	var dropped runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&dropped)
	owned := int64(alive) - int64(dropped.HeapAlloc)
	t.Logf("%d events through cap %d: the buffer owns %d bytes (%d bytes per held event)", n, cap, owned, owned/cap)
	if owned > 1<<20 {
		t.Fatalf("buffer owns %d bytes for %d held events, want under 1 MiB", owned, cap)
	}
}
