package eventorder

import (
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"
)

type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *fakeClock) now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *fakeClock) advance(d time.Duration) {
	c.mu.Lock()
	c.t = c.t.Add(d)
	c.mu.Unlock()
}

func TestAlignedIsTheMinimum(t *testing.T) {
	a := NewAligned[string]()
	if _, ok := a.Watermark(); ok {
		t.Fatal("watermark exists with no sources")
	}
	a.Register("a")
	a.Register("b")
	if _, adv, ok := a.Observe("a", at(10)); adv || ok {
		t.Fatalf("advanced=%v ok=%v while b has not reported", adv, ok)
	}
	w, adv, ok := a.Observe("b", at(4))
	if !adv || !ok || !w.Equal(at(4)) {
		t.Fatalf("got %v %v %v, want 4s advanced", w, adv, ok)
	}
	w, adv, _ = a.Observe("a", at(20))
	if adv || !w.Equal(at(4)) {
		t.Fatalf("fast source moved the watermark: %v %v", w, adv)
	}
	w, adv, _ = a.Observe("b", at(15))
	if !adv || !w.Equal(at(15)) {
		t.Fatalf("got %v %v, want 15s", w, adv)
	}
}

func TestAlignedIgnoresRegressingSource(t *testing.T) {
	a := NewAligned[int]()
	a.Observe(1, at(10))
	a.Observe(2, at(10))
	a.Observe(1, at(3))
	if w, _ := a.Watermark(); !w.Equal(at(10)) {
		t.Fatalf("watermark %v, want 10s", w)
	}
	if got := a.Lag()[0].Watermark; !got.Equal(at(10)) {
		t.Fatalf("source watermark went back to %v", got)
	}
}

func TestAlignedIdleSourceIsLeftOutAndRejoins(t *testing.T) {
	a := NewAligned[string]()
	a.Register("a")
	a.Register("b")
	a.Observe("a", at(10))
	a.Observe("b", at(2))
	w, adv, _ := a.Idle("b")
	if !adv || !w.Equal(at(10)) {
		t.Fatalf("idling the slow source gave %v %v, want 10s advanced", w, adv)
	}
	// b comes back behind the combined watermark: no regress, and b holds nothing
	// back until it passes it.
	w, adv, _ = a.Observe("b", at(5))
	if adv || !w.Equal(at(10)) {
		t.Fatalf("rejoined source moved the watermark: %v %v", w, adv)
	}
	w, adv, _ = a.Observe("a", at(30))
	if adv || !w.Equal(at(10)) {
		t.Fatalf("b at 5s should still hold the minimum at the floor, got %v %v", w, adv)
	}
	w, adv, _ = a.Observe("b", at(25))
	if !adv || !w.Equal(at(25)) {
		t.Fatalf("got %v %v, want 25s", w, adv)
	}
}

func TestAlignedAllIdleHoldsWatermark(t *testing.T) {
	a := NewAligned[string]()
	a.Observe("a", at(10))
	a.Idle("a")
	if w, ok := a.Watermark(); !ok || !w.Equal(at(10)) {
		t.Fatalf("got %v %v, want held at 10s", w, ok)
	}
}

func TestAlignedRemove(t *testing.T) {
	a := NewAligned[string]()
	a.Register("a")
	a.Register("b")
	a.Observe("a", at(10))
	a.Observe("b", at(1))
	w, adv, _ := a.Remove("b")
	if !adv || !w.Equal(at(10)) || a.Len() != 1 {
		t.Fatalf("got %v %v len %d", w, adv, a.Len())
	}
}

func TestAlignedIdleTimeout(t *testing.T) {
	clk := &fakeClock{t: t0}
	a := NewAligned[string](WithSourceIdleTimeout(time.Minute), WithAlignedClock(clk.now))
	a.Register("a")
	a.Register("b")
	a.Observe("a", at(100))
	a.Observe("b", at(1))
	clk.advance(30 * time.Second)
	a.Observe("a", at(200))
	if w, _ := a.Watermark(); !w.Equal(at(1)) {
		t.Fatalf("b timed out too early: %v", w)
	}
	clk.advance(45 * time.Second) // b silent for 75s, a for 45s
	if w, _ := a.Watermark(); !w.Equal(at(200)) {
		t.Fatalf("b should be out after the timeout, got %v", w)
	}
	if st := a.Lag(); !st[len(st)-1].Idle && !st[0].Idle {
		t.Fatalf("no source reported idle: %+v", st)
	}
}

func TestAlignedUnreportedSourceTimesOut(t *testing.T) {
	clk := &fakeClock{t: t0}
	a := NewAligned[string](WithSourceIdleTimeout(time.Minute), WithAlignedClock(clk.now))
	a.Register("dead")
	a.Observe("a", at(50))
	if _, ok := a.Watermark(); ok {
		t.Fatal("watermark exists while a registered source has not reported")
	}
	clk.advance(2 * time.Minute)
	a.Observe("a", at(60))
	if w, ok := a.Watermark(); !ok || !w.Equal(at(60)) {
		t.Fatalf("got %v %v, want 60s", w, ok)
	}
}

func TestAlignedLagNamesTheHolder(t *testing.T) {
	a := NewAligned[string]()
	a.Observe("fast", at(100))
	a.Observe("mid", at(60))
	a.Observe("slow", at(10))
	st := a.Lag()
	if len(st) != 3 || st[0].Source != "slow" || !st[0].Holding || st[0].Behind != 90*time.Second {
		t.Fatalf("lag report %+v", st)
	}
	if st[1].Holding || st[2].Holding || st[2].Source != "fast" || st[2].Behind != 0 {
		t.Fatalf("lag report %+v", st)
	}
	a.Register("new")
	st = a.Lag()
	if st[0].Source != "new" || !st[0].Holding || st[0].Reported {
		t.Fatalf("unreported source should lead the report: %+v", st)
	}
	if st[1].Holding {
		t.Fatalf("slow is not the holder while new blocks: %+v", st)
	}
}

func TestAlignedEqualSourcesHoldNothing(t *testing.T) {
	a := NewAligned[int]()
	a.Observe(1, at(5))
	a.Observe(2, at(5))
	for _, s := range a.Lag() {
		if s.Holding || s.Behind != 0 {
			t.Fatalf("%+v", s)
		}
	}
}

// Two sources, a far ahead of b but both in order. Ordering the merged stream
// by the fastest source's clock turns every event of b into a late one; with the
// combined watermark none is late.
func TestAlignedAvoidsLateEventsThatTheFastestClockCauses(t *testing.T) {
	fast := New[string](WithMaxDelay(0))
	down := New[string](WithMaxDelay(1000 * time.Hour))
	ag := NewAligned[string]()
	ag.Register("a")
	ag.Register("b")
	var released []Event[string]
	for i := 0; i < 100; i++ {
		for _, e := range []struct {
			src string
			sec int
		}{{"a", 100 + i}, {"b", i}} {
			fast.Add(at(e.sec), e.src)
			released = append(released, down.Add(at(e.sec), e.src)...)
			if w, adv, _ := ag.Observe(e.src, at(e.sec)); adv {
				released = append(released, down.AdvanceTo(w)...)
			}
		}
	}
	released = append(released, down.Flush()...)
	if fast.Stats().Late != 100 {
		t.Fatalf("scenario broken: fastest-source clock made %d late events, want 100", fast.Stats().Late)
	}
	if down.Stats().Late != 0 || len(released) != 200 {
		t.Fatalf("aligned: %d late, %d released of 200", down.Stats().Late, len(released))
	}
	for i := 1; i < len(released); i++ {
		if released[i].Time.Before(released[i-1].Time) {
			t.Fatalf("output out of order at %d", i)
		}
	}
}

// refAligned is the specification written the dumb way: keep every report in a
// list and recompute the answer from scratch on each query.
type refAligned struct {
	timeout time.Duration
	srcs    []*refSrc
	comb    time.Time
	has     bool
}

type refSrc struct {
	name     int
	wm       time.Time
	has      bool
	idle     bool
	lastSeen time.Time
}

func (r *refAligned) find(n int) *refSrc {
	for _, s := range r.srcs {
		if s.name == n {
			return s
		}
	}
	return nil
}

func (r *refAligned) ensure(n int, now time.Time) *refSrc {
	if s := r.find(n); s != nil {
		return s
	}
	s := &refSrc{name: n, lastSeen: now}
	r.srcs = append(r.srcs, s)
	return s
}

func (r *refAligned) eval(now time.Time) (time.Time, bool, bool) {
	var min time.Time
	have := false
	for _, s := range r.srcs {
		if s.idle || (r.timeout > 0 && now.Sub(s.lastSeen) > r.timeout) {
			continue
		}
		if !s.has {
			return r.comb, false, r.has
		}
		if !have || s.wm.Before(min) {
			min, have = s.wm, true
		}
	}
	if have && (!r.has || min.After(r.comb)) {
		r.comb, r.has = min, true
		return r.comb, true, true
	}
	return r.comb, false, r.has
}

func (r *refAligned) remove(n int) {
	for i, s := range r.srcs {
		if s.name == n {
			r.srcs = append(r.srcs[:i], r.srcs[i+1:]...)
			return
		}
	}
}

func TestAlignedMatchesReference(t *testing.T) {
	for seed := int64(0); seed < 2000; seed++ {
		r := rand.New(rand.NewSource(seed))
		clk := &fakeClock{t: t0}
		timeout := time.Duration(r.Intn(3)) * 20 * time.Second
		a := NewAligned[int](WithSourceIdleTimeout(timeout), WithAlignedClock(clk.now))
		ref := &refAligned{timeout: timeout}
		var prev time.Time
		var prevOK bool
		for step := 0; step < 150; step++ {
			n := r.Intn(5)
			var gw, ww time.Time
			var gadv, wadv, gok, wok bool
			var op string
			now := clk.now()
			switch k := r.Intn(100); {
			case k < 55:
				wm := at(r.Intn(400))
				op = fmt.Sprintf("Observe(%d,%v)", n, wm.Sub(t0))
				gw, gadv, gok = a.Observe(n, wm)
				s := ref.ensure(n, now)
				if !s.has || wm.After(s.wm) {
					s.wm, s.has = wm, true
				}
				s.idle, s.lastSeen = false, now
				ww, wadv, wok = ref.eval(now)
			case k < 65:
				op = fmt.Sprintf("Register(%d)", n)
				a.Register(n)
				ref.ensure(n, now)
				gw, gadv, gok = a.recomputeForTest()
				ww, wadv, wok = ref.eval(now)
			case k < 75:
				op = fmt.Sprintf("Idle(%d)", n)
				gw, gadv, gok = a.Idle(n)
				ref.ensure(n, now).idle = true
				ww, wadv, wok = ref.eval(now)
			case k < 82:
				op = fmt.Sprintf("Remove(%d)", n)
				gw, gadv, gok = a.Remove(n)
				ref.remove(n)
				ww, wadv, wok = ref.eval(now)
			default:
				d := time.Duration(r.Intn(30)) * time.Second
				op = fmt.Sprintf("advance(%v)", d)
				clk.advance(d)
				now = clk.now()
				gw, gok = a.Watermark()
				ww, wadv, wok = ref.eval(now)
				_ = wadv
				gadv = wadv // advance flag is not returned by Watermark
			}
			if gok != wok || !gw.Equal(ww) || gadv != wadv {
				t.Fatalf("seed %d step %d %s: got (%v,%v,%v) want (%v,%v,%v)", seed, step, op,
					gw.Sub(t0), gadv, gok, ww.Sub(t0), wadv, wok)
			}
			if prevOK && gok && gw.Before(prev) {
				t.Fatalf("seed %d step %d %s: watermark moved back from %v to %v", seed, step, op, prev.Sub(t0), gw.Sub(t0))
			}
			prev, prevOK = gw, gok
			if a.Len() != len(ref.srcs) {
				t.Fatalf("seed %d step %d %s: Len %d, want %d", seed, step, op, a.Len(), len(ref.srcs))
			}
		}
	}
}

// recomputeForTest lets the reference test ask for the current answer after
// Register, which returns nothing.
func (a *Aligned[S]) recomputeForTest() (time.Time, bool, bool) {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.recompute()
}

// The combined watermark is never above any active source's watermark once it
// has been established from them, never moves back, and Buffer output stays
// sorted when fed only by it.
func TestAlignedNeverPassesASlowSource(t *testing.T) {
	for seed := int64(0); seed < 500; seed++ {
		r := rand.New(rand.NewSource(seed))
		a := NewAligned[int]()
		for i := 0; i < 4; i++ {
			a.Register(i)
		}
		high := make([]time.Time, 4)
		var prev time.Time
		for step := 0; step < 300; step++ {
			n := r.Intn(4)
			wm := at(r.Intn(1000))
			if wm.After(high[n]) {
				high[n] = wm
			}
			w, _, ok := a.Observe(n, wm)
			if !ok {
				continue
			}
			for i, h := range high {
				if h.IsZero() {
					t.Fatalf("seed %d: watermark exists while source %d has not reported", seed, i)
				}
				if w.After(h) {
					t.Fatalf("seed %d: combined %v passes source %d at %v", seed, w.Sub(t0), i, h.Sub(t0))
				}
			}
			if w.Before(prev) {
				t.Fatalf("seed %d: watermark moved back", seed)
			}
			prev = w
		}
	}
}

func FuzzAligned(f *testing.F) {
	f.Add([]byte{1, 5, 2, 9, 0, 3, 7, 1})
	f.Add([]byte{})
	f.Fuzz(func(t *testing.T, data []byte) {
		clk := &fakeClock{t: t0}
		a := NewAligned[byte](WithSourceIdleTimeout(10*time.Second), WithAlignedClock(clk.now))
		var prev time.Time
		var have bool
		for i := 0; i+1 < len(data); i += 2 {
			src, v := data[i]%6, data[i+1]
			var w time.Time
			var ok bool
			switch v % 5 {
			case 0:
				w, _, ok = a.Idle(src)
			case 1:
				w, _, ok = a.Remove(src)
			case 2:
				clk.advance(time.Duration(v) * time.Second)
				w, ok = a.Watermark()
			default:
				w, _, ok = a.Observe(src, at(int(v)))
			}
			a.Lag()
			if have && ok && w.Before(prev) {
				t.Fatalf("watermark moved back from %v to %v", prev, w)
			}
			if ok {
				prev, have = w, true
			}
		}
	})
}

func TestAlignedConcurrentUse(t *testing.T) {
	a := NewAligned[int]()
	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			r := rand.New(rand.NewSource(int64(g)))
			for i := 0; i < 5000; i++ {
				switch r.Intn(5) {
				case 0:
					a.Idle(r.Intn(10))
				case 1:
					a.Remove(r.Intn(10))
				case 2:
					a.Lag()
				default:
					a.Observe(r.Intn(10), at(i))
				}
			}
		}(g)
	}
	wg.Wait()
}

// Memory follows the live sources: removing a source releases it, so churning
// through a million short-lived sources holds at most the ones still alive.
func TestAlignedMemoryFollowsLiveSources(t *testing.T) {
	a := NewAligned[int]()
	for i := 0; i < 1_000_000; i++ {
		a.Observe(i, at(i%100))
		if i >= 50 {
			a.Remove(i - 50)
		}
		if a.Len() > 51 {
			t.Fatalf("after %d sources Len is %d, want at most 51", i+1, a.Len())
		}
	}
}
