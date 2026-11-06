package eventorder

import (
	"fmt"
	"math"
	"math/rand"
	"runtime"
	"slices"
	"testing"
)

// refSequencer is the specification written the dumb way: a map of held items
// and a sorted key list built on every step. It shares no code with Sequencer.
type refSequencer struct {
	window  uint64
	next    uint64
	held    map[uint64]int
	gaps    [][2]uint64
	stale   int
	dups    int
	release []Numbered[int]
}

func newRefSequencer(first uint64, window int) *refSequencer {
	return &refSequencer{window: uint64(max(window, 1)), next: first, held: map[uint64]int{}}
}

func (r *refSequencer) keys() []uint64 {
	ks := make([]uint64, 0, len(r.held))
	for k := range r.held {
		ks = append(ks, k)
	}
	slices.Sort(ks)
	return ks
}

// moveTo gives up on every number below to that is not held, releasing the
// held ones in order.
func (r *refSequencer) moveTo(to uint64) {
	for _, k := range r.keys() {
		if k >= to {
			break
		}
		if k > r.next {
			r.gaps = append(r.gaps, [2]uint64{r.next, k})
		}
		r.release = append(r.release, Numbered[int]{Seq: k, Value: r.held[k]})
		delete(r.held, k)
		r.next = k + 1
	}
	if to > r.next {
		r.gaps = append(r.gaps, [2]uint64{r.next, to})
		r.next = to
	}
}

func (r *refSequencer) drain() {
	for {
		v, ok := r.held[r.next]
		if !ok {
			return
		}
		r.release = append(r.release, Numbered[int]{Seq: r.next, Value: v})
		delete(r.held, r.next)
		r.next++
	}
}

func (r *refSequencer) add(seq uint64, v int) {
	if seq < r.next {
		r.stale++
		return
	}
	if seq >= r.next+r.window {
		r.moveTo(seq - r.window + 1)
	}
	if _, ok := r.held[seq]; ok {
		r.dups++
		return
	}
	r.held[seq] = v
	r.drain()
}

func (r *refSequencer) skip() {
	if ks := r.keys(); len(ks) > 0 {
		r.moveTo(ks[0])
		r.drain()
	}
}

func (r *refSequencer) flush() {
	for len(r.held) > 0 {
		r.skip()
	}
}

// take returns and clears what the reference released since the last call.
func (r *refSequencer) take() []Numbered[int] {
	out := r.release
	r.release = nil
	return out
}

type gapLog struct{ gaps [][2]uint64 }

func (g *gapLog) record(from, to uint64) { g.gaps = append(g.gaps, [2]uint64{from, to}) }

// TestSequencerMatchesReference runs 3000 random operation sequences (Add with
// repeats, stale numbers and jumps; Skip; Flush) through Sequencer and the
// reference and compares every call's output, every gap and the counters.
func TestSequencerMatchesReference(t *testing.T) {
	for seed := int64(0); seed < 3000; seed++ {
		r := rand.New(rand.NewSource(seed))
		window := r.Intn(12) // includes 0, taken as 1
		first := uint64(r.Intn(3))
		if r.Intn(10) == 0 {
			first = math.MaxUint64 - 400 // numbers near the top of uint64
		}
		ref := newRefSequencer(first, window)
		s := NewSequencer[int](first, window)
		var log gapLog
		s.OnGap(log.record)
		var stale int
		s.OnStale(func(Numbered[int]) { stale++ })
		top := first
		for step := 0; step < 150; step++ {
			var got []Numbered[int]
			var op string
			switch n := r.Intn(100); {
			case n < 88:
				// Mostly near the frontier, sometimes behind it, sometimes far ahead.
				seq := top + uint64(r.Intn(6))
				switch r.Intn(12) {
				case 0:
					seq = first + uint64(r.Intn(int(top-first)+1))
				case 1:
					seq = top + uint64(r.Intn(40))
				}
				if seq > top {
					top = seq
				}
				op = fmt.Sprintf("add %d", seq-first)
				got = s.Add(seq, step)
				ref.add(seq, step)
			case n < 96:
				op = "skip"
				got = s.Skip()
				ref.skip()
			default:
				op = "flush"
				got = s.Flush()
				ref.flush()
			}
			want := ref.take()
			if !slices.Equal(got, want) {
				t.Fatalf("seed %d step %d (%s, window %d): got %v want %v", seed, step, op, window, got, want)
			}
			if !slices.Equal(log.gaps, ref.gaps) {
				t.Fatalf("seed %d step %d (%s): gaps %v want %v", seed, step, op, log.gaps, ref.gaps)
			}
			st := s.Stats()
			if st.Held != len(ref.held) || s.Len() != len(ref.held) || st.Next != ref.next || s.Next() != ref.next {
				t.Fatalf("seed %d step %d: held %d next %d, reference %d %d", seed, step, st.Held, st.Next, len(ref.held), ref.next)
			}
			if int(st.Stale) != ref.stale || stale != ref.stale || int(st.Dups) != ref.dups || int(st.Gaps) != len(ref.gaps) {
				t.Fatalf("seed %d step %d: stats %+v, reference stale %d dups %d gaps %d", seed, step, st, ref.stale, ref.dups, len(ref.gaps))
			}
		}
	}
}

// TestSequencerAccountsForEveryNumber checks the promise without the
// reference: released numbers strictly increase, and every number from first
// to Next is released once or inside exactly one gap.
func TestSequencerAccountsForEveryNumber(t *testing.T) {
	for seed := int64(0); seed < 500; seed++ {
		r := rand.New(rand.NewSource(seed))
		window := 1 + r.Intn(20)
		s := NewSequencer[int](0, window)
		covered := map[uint64]int{}
		s.OnGap(func(from, to uint64) {
			for n := from; n < to; n++ {
				covered[n]++
			}
		})
		last, any := uint64(0), false
		check := func(out []Numbered[int]) {
			for _, it := range out {
				if any && it.Seq <= last {
					t.Fatalf("seed %d: released %d after %d", seed, it.Seq, last)
				}
				last, any = it.Seq, true
				covered[it.Seq]++
			}
		}
		perm := r.Perm(400)
		for i, p := range perm {
			if r.Intn(8) == 0 {
				continue // this number never arrives
			}
			check(s.Add(uint64(p), i))
			if s.Len() >= window {
				t.Fatalf("seed %d: holding %d with window %d", seed, s.Len(), window)
			}
		}
		check(s.Flush())
		for n := uint64(0); n < s.Next(); n++ {
			if covered[n] != 1 {
				t.Fatalf("seed %d: number %d accounted for %d times", seed, n, covered[n])
			}
		}
	}
}

// TestSequencerInOrderWithinWindow: a stream shuffled only inside blocks that
// fit the window comes out complete, in order, with no gap.
func TestSequencerInOrderWithinWindow(t *testing.T) {
	const window, n = 16, 10_000
	r := rand.New(rand.NewSource(7))
	s := NewSequencer[int](100, window)
	var got []uint64
	for start := 0; start < n; start += window {
		for _, o := range r.Perm(window) {
			for _, it := range s.Add(uint64(100+start+o), start+o) {
				got = append(got, it.Seq)
				if it.Value != int(it.Seq)-100 {
					t.Fatalf("value %d released for number %d", it.Value, it.Seq)
				}
			}
		}
	}
	if st := s.Stats(); st.Gaps != 0 || st.Held != 0 || len(got) != n {
		t.Fatalf("stats %+v, released %d of %d", st, len(got), n)
	}
	if !slices.IsSorted(got) || got[0] != 100 {
		t.Fatalf("released out of order or from the wrong start")
	}
}

func TestSequencerCases(t *testing.T) {
	t.Run("gap waits then fills", func(t *testing.T) {
		s := NewSequencer[string](0, 8)
		if out := s.Add(1, "b"); len(out) != 0 {
			t.Fatalf("released %v before 0 arrived", out)
		}
		out := s.Add(0, "a")
		if want := []Numbered[string]{{0, "a"}, {1, "b"}}; !slices.Equal(out, want) {
			t.Fatalf("got %v want %v", out, want)
		}
	})
	t.Run("overflow gives up on the oldest gap only", func(t *testing.T) {
		s := NewSequencer[int](0, 4)
		var gaps [][2]uint64
		s.OnGap(func(f, to uint64) { gaps = append(gaps, [2]uint64{f, to}) })
		s.Add(2, 2)
		s.Add(3, 3)
		out := s.Add(5, 5) // 5 does not fit next=0 and window 4: give up on 0 and 1
		want := []Numbered[int]{{2, 2}, {3, 3}}
		if !slices.Equal(out, want) || !slices.Equal(gaps, [][2]uint64{{0, 2}}) || s.Next() != 4 {
			t.Fatalf("out %v gaps %v next %d", out, gaps, s.Next())
		}
	})
	t.Run("a huge jump is one gap and cheap", func(t *testing.T) {
		s := NewSequencer[int](0, 4)
		s.Add(1, 1)
		out := s.Add(1<<62, 9)
		if !slices.Equal(out, []Numbered[int]{{1, 1}}) || s.Len() != 1 {
			t.Fatalf("out %v len %d", out, s.Len())
		}
		if st := s.Stats(); st.Gaps != 2 || st.Missing != 1+(1<<62-3-2) {
			t.Fatalf("stats %+v", st)
		}
	})
	t.Run("stale and duplicate", func(t *testing.T) {
		s := NewSequencer[int](10, 4)
		var stale []Numbered[int]
		s.OnStale(func(n Numbered[int]) { stale = append(stale, n) })
		s.Add(10, 0)
		s.Add(12, 1)
		s.Add(12, 2) // held already
		s.Add(10, 3) // released already
		s.Add(9, 4)  // before first
		st := s.Stats()
		if st.Dups != 1 || st.Stale != 2 || len(stale) != 2 || stale[0].Value != 3 {
			t.Fatalf("stats %+v stale %v", st, stale)
		}
	})
	t.Run("skip and flush", func(t *testing.T) {
		s := NewSequencer[int](0, 100)
		if out := s.Skip(); out != nil {
			t.Fatalf("skip on empty released %v", out)
		}
		s.Add(3, 3)
		s.Add(4, 4)
		s.Add(9, 9)
		if out := s.Skip(); !slices.Equal(out, []Numbered[int]{{3, 3}, {4, 4}}) {
			t.Fatalf("skip released %v", out)
		}
		if out := s.Flush(); !slices.Equal(out, []Numbered[int]{{9, 9}}) || s.Next() != 10 {
			t.Fatalf("flush released %v, next %d", out, s.Next())
		}
		if st := s.Stats(); st.Gaps != 2 || st.Missing != 3+4 || st.Peak != 3 {
			t.Fatalf("stats %+v", st)
		}
	})
	t.Run("window below one never waits", func(t *testing.T) {
		s := NewSequencer[int](0, 0)
		if out := s.Add(5, 5); !slices.Equal(out, []Numbered[int]{{5, 5}}) {
			t.Fatalf("out %v", out)
		}
	})
	t.Run("top of uint64", func(t *testing.T) {
		s := NewSequencer[int](math.MaxUint64-1, 4)
		s.Add(math.MaxUint64, 1)
		out := s.Add(math.MaxUint64-1, 0)
		if len(out) != 2 || out[1].Seq != math.MaxUint64 {
			t.Fatalf("out %v", out)
		}
	})
}

// The ring never holds more than window slots, however long the stream and
// however many numbers go missing. The bound is loose on purpose; the measured
// value is logged.
func TestSequencerMemoryBound(t *testing.T) {
	const window, n = 1024, 2_000_000
	feed := func() uint64 {
		r := rand.New(rand.NewSource(5))
		s := NewSequencer[int](0, window)
		var dst []Numbered[int]
		var seq uint64
		for i := 0; i < n; i++ {
			seq += uint64(1 + r.Intn(3)) // a third of the numbers never arrive
			jitter := uint64(r.Intn(window / 2))
			dst = s.AddTo(dst[:0], seq+jitter, i)
			if s.Len() >= window || len(s.slots) > window {
				t.Fatalf("after %d adds: held %d, ring %d, window %d", i+1, s.Len(), len(s.slots), window)
			}
		}
		var m runtime.MemStats
		runtime.GC()
		runtime.ReadMemStats(&m)
		runtime.KeepAlive(s)
		return m.HeapAlloc
	}
	alive := feed()
	var dropped runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&dropped)
	owned := int64(alive) - int64(dropped.HeapAlloc)
	t.Logf("%d items through window %d: the sequencer owns %d bytes", n, window, owned)
	if owned > 1<<20 {
		t.Fatalf("sequencer owns %d bytes for window %d, want under 1 MiB", owned, window)
	}
}

// Properties for any input: numbers come out strictly increasing, every
// number up to Next is released or in one gap, nothing is held past the window.
func FuzzSequencer(f *testing.F) {
	f.Add([]byte{3, 1, 2, 0, 9, 5}, uint8(4))
	f.Add([]byte{255, 0, 0, 7}, uint8(0))
	f.Fuzz(func(t *testing.T, steps []byte, window uint8) {
		s := NewSequencer[int](0, int(window))
		covered := map[uint64]int{}
		// Numbers grow by at most 66 per step, so every gap is short to walk.
		s.OnGap(func(from, to uint64) {
			for n := from; n < to; n++ {
				covered[n]++
			}
		})
		var last uint64
		var any bool
		var base uint64
		take := func(out []Numbered[int]) {
			for _, it := range out {
				if any && it.Seq <= last {
					t.Fatalf("released %d after %d", it.Seq, last)
				}
				last, any = it.Seq, true
				covered[it.Seq]++
			}
		}
		for i, b := range steps {
			if b == 255 {
				take(s.Skip())
			} else {
				base += uint64(b % 4)
				take(s.Add(base+uint64(b/4), i))
			}
			if s.Len() >= max(int(window), 1) {
				t.Fatalf("holding %d with window %d", s.Len(), window)
			}
		}
		take(s.Flush())
		st := s.Stats()
		if st.Released+st.Missing != s.Next() {
			t.Fatalf("released %d + missing %d != next %d", st.Released, st.Missing, s.Next())
		}
		for n := uint64(0); n < s.Next(); n++ {
			if covered[n] != 1 {
				t.Fatalf("number %d accounted for %d times", n, covered[n])
			}
		}
	})
}
