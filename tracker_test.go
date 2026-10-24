package eventorder

import (
	"math/rand"
	"sync"
	"testing"
	"time"
)

// refTracker is the specification written the dumb way: a set of pending
// offsets, and the answer is found by scanning it.
type refTracker struct {
	pending map[int64]bool
	last    int64
	any     bool
}

func newRefTracker() *refTracker { return &refTracker{pending: map[int64]bool{}} }

func (r *refTracker) track(off int64) bool {
	if r.any && off <= r.last {
		return false
	}
	r.pending[off] = true
	r.last, r.any = off, true
	return true
}

func (r *refTracker) done(off int64) bool {
	if !r.pending[off] {
		return false
	}
	delete(r.pending, off)
	return true
}

func (r *refTracker) commit() (int64, bool) {
	if !r.any {
		return 0, false
	}
	low, found := int64(0), false
	for off := range r.pending {
		if !found || off < low {
			low, found = off, true
		}
	}
	if found {
		return low, true
	}
	return r.last + 1, true
}

func TestTrackerBasics(t *testing.T) {
	tr := NewTracker()
	if _, ok := tr.Commit(); ok {
		t.Fatal("Commit before any Track must report ok=false")
	}
	for _, off := range []int64{10, 11, 12, 15} {
		if !tr.Track(off) {
			t.Fatalf("Track(%d) refused", off)
		}
	}
	if tr.Track(15) || tr.Track(3) {
		t.Fatal("Track must refuse an offset that does not increase")
	}
	if off, _ := tr.Commit(); off != 10 {
		t.Fatalf("Commit = %d, want 10 (nothing done)", off)
	}
	tr.Done(11)
	tr.Done(15)
	if off, _ := tr.Commit(); off != 10 {
		t.Fatalf("Commit = %d, want 10 (10 still pending)", off)
	}
	if tr.Done(11) || tr.Done(99) {
		t.Fatal("Done must refuse a repeated or unknown offset")
	}
	tr.Done(10)
	if off, _ := tr.Commit(); off != 12 {
		t.Fatalf("Commit = %d, want 12", off)
	}
	tr.Done(12)
	if off, _ := tr.Commit(); off != 16 {
		t.Fatalf("Commit = %d, want 16 (one past the last tracked)", off)
	}
	if tr.Pending() != 0 {
		t.Fatalf("Pending = %d, want 0", tr.Pending())
	}
}

func TestTrackerMatchesReference(t *testing.T) {
	for seed := int64(0); seed < 2000; seed++ {
		r := rand.New(rand.NewSource(seed))
		tr, ref := NewTracker(), newRefTracker()
		next := int64(r.Intn(1000))
		for step := 0; step < 150; step++ {
			switch r.Intn(3) {
			case 0: // track, sometimes with a gap, sometimes a bad offset
				off := next
				if r.Intn(8) == 0 {
					off = next - int64(r.Intn(5))
				}
				if got, want := tr.Track(off), ref.track(off); got != want {
					t.Fatalf("seed %d step %d: Track(%d) = %v, want %v", seed, step, off, got, want)
				}
				if off >= next {
					next = off + 1 + int64(r.Intn(3))
				}
			case 1: // done, mostly a real offset, sometimes junk
				off := next - 1 - int64(r.Intn(12))
				if got, want := tr.Done(off), ref.done(off); got != want {
					t.Fatalf("seed %d step %d: Done(%d) = %v, want %v", seed, step, off, got, want)
				}
			case 2:
				go1, ok1 := tr.Commit()
				go2, ok2 := ref.commit()
				if go1 != go2 || ok1 != ok2 {
					t.Fatalf("seed %d step %d: Commit = %d,%v want %d,%v", seed, step, go1, ok1, go2, ok2)
				}
			}
		}
		if got, want := tr.Pending(), len(ref.pending); got != want {
			t.Fatalf("seed %d: Pending = %d, want %d", seed, got, want)
		}
	}
}

// TestTrackerNeverCommitsPastHeldEvents is the point of the type. Events carry
// consecutive offsets but arrive shuffled; after every step, the offset
// Commit returns must be at or below the offset of every event that is not yet
// processed, so a crash and a replay from it loses nothing.
func TestTrackerNeverCommitsPastHeldEvents(t *testing.T) {
	for seed := int64(0); seed < 300; seed++ {
		r := rand.New(rand.NewSource(seed))
		const n = 400
		times := make([]int, n) // times[i] is the event time of offset i
		for i := range times {
			times[i] = i
		}
		for i := 0; i < n; i++ { // shuffle inside a window of 8
			j := i + r.Intn(8)
			if j >= n {
				j = n - 1
			}
			times[i], times[j] = times[j], times[i]
		}
		buf := New[int](WithMaxDelay(time.Duration(r.Intn(10))*time.Second), WithMaxBuffered(r.Intn(6)))
		tr := NewTracker()
		processed := make([]bool, n)
		check := func(step int) {
			c, _ := tr.Commit()
			for off := int64(0); off < int64(step); off++ {
				if !processed[off] && off < c {
					t.Fatalf("seed %d step %d: committed %d but offset %d is not processed", seed, step, c, off)
				}
			}
		}
		finish := func(evs []Event[int]) {
			for _, e := range evs {
				if !tr.Done(int64(e.Value)) {
					t.Fatalf("seed %d: Done(%d) refused", seed, e.Value)
				}
				processed[e.Value] = true
			}
		}
		buf.OnLate(func(e Event[int]) {
			tr.Done(int64(e.Value)) // a dropped late event is finished too
			processed[e.Value] = true
		})
		for off := 0; off < n; off++ {
			tr.Track(int64(off))
			finish(buf.Add(t0.Add(time.Duration(times[off])*time.Second), off))
			check(off + 1)
		}
		finish(buf.Flush())
		if c, _ := tr.Commit(); c != n {
			t.Fatalf("seed %d: after Flush Commit = %d, want %d", seed, c, n)
		}
	}
}

func TestKeyedTrackerRevoke(t *testing.T) {
	kt := NewKeyedTracker[string]()
	kb := NewKeyed[string, int](WithMaxDelay(10 * time.Second))
	feed := func(part string, off int64, sec int) []Event[int] {
		kt.Track(part, off)
		return kb.Add(part, t0.Add(time.Duration(sec)*time.Second), int(off))
	}
	feed("p0", 0, 5)
	feed("p0", 1, 3)
	feed("p1", 0, 1)
	if off, ok := kt.Commit("p0"); !ok || off != 0 {
		t.Fatalf("p0 Commit = %d,%v, want 0 (events held)", off, ok)
	}
	// Revoke p0: flush what it holds, process it, then revoke.
	for _, e := range kb.Flush("p0") {
		kt.Done("p0", int64(e.Value))
	}
	if off, ok := kt.Revoke("p0"); !ok || off != 2 {
		t.Fatalf("p0 Revoke = %d,%v, want 2", off, ok)
	}
	if _, ok := kt.Commit("p0"); ok {
		t.Fatal("p0 must be forgotten after Revoke")
	}
	if kt.Len() != 1 {
		t.Fatalf("Len = %d, want 1", kt.Len())
	}
	// Revoking without finishing leaves the commit at the first pending offset.
	if off, _ := kt.Revoke("p1"); off != 0 {
		t.Fatalf("p1 Revoke = %d, want 0 (offset 0 never finished)", off)
	}
	if _, ok := kt.Revoke("nope"); ok {
		t.Fatal("Revoke of an unknown key must report ok=false")
	}
	if kt.Done("nope", 1) {
		t.Fatal("Done on an unknown key must report false")
	}
}

// TestTrackerMemoryBound: with a bounded lag between Track and Done, the
// tracker holds only about that lag, however many offsets pass through.
func TestTrackerMemoryBound(t *testing.T) {
	tr := NewTracker()
	const total, lag = 200_000, 100
	maxEntries := 0
	for off := int64(0); off < total; off++ {
		tr.Track(off)
		if off >= lag {
			tr.Done(off - lag + 1) // leave the oldest one pending for a while
			tr.Done(off - lag)
		}
		if n := len(tr.entries); n > maxEntries {
			maxEntries = n
		}
	}
	if maxEntries > 4*lag+128 {
		t.Fatalf("tracker held %d entries for a lag of %d", maxEntries, lag)
	}
}

func TestTrackerStress(t *testing.T) {
	tr := NewTracker()
	const n = 20_000
	for off := int64(0); off < n; off++ {
		tr.Track(off)
	}
	var wg sync.WaitGroup
	for w := 0; w < 8; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for off := int64(w); off < n; off += 8 {
				if !tr.Done(off) {
					t.Errorf("Done(%d) refused", off)
				}
				tr.Commit()
				tr.Pending()
			}
		}(w)
	}
	wg.Wait()
	if off, _ := tr.Commit(); off != n {
		t.Fatalf("Commit = %d, want %d", off, n)
	}
}

func TestKeyedTrackerStress(t *testing.T) {
	kt := NewKeyedTracker[int]()
	var wg sync.WaitGroup
	for w := 0; w < 8; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for off := int64(0); off < 2000; off++ {
				kt.Track(w%4, int64(w)*100_000+off)
				kt.Done(w%4, int64(w)*100_000+off)
				kt.Commit(w % 4)
			}
		}(w)
	}
	wg.Wait()
}

func FuzzTracker(f *testing.F) {
	f.Add([]byte{0, 1, 0, 1, 2, 1, 0, 2, 2})
	f.Add([]byte{0, 0, 0, 1, 5, 1, 4, 2, 0, 3, 2})
	f.Fuzz(func(t *testing.T, data []byte) {
		tr, ref := NewTracker(), newRefTracker()
		next := int64(0)
		for i := 0; i+1 < len(data); i += 2 {
			arg := int64(data[i+1] % 16)
			switch data[i] % 3 {
			case 0:
				off := next + arg - 2
				if got, want := tr.Track(off), ref.track(off); got != want {
					t.Fatalf("Track(%d) = %v, want %v", off, got, want)
				}
				if off >= next {
					next = off + 1
				}
			case 1:
				off := next - arg
				if got, want := tr.Done(off), ref.done(off); got != want {
					t.Fatalf("Done(%d) = %v, want %v", off, got, want)
				}
			case 2:
				a, aok := tr.Commit()
				b, bok := ref.commit()
				if a != b || aok != bok {
					t.Fatalf("Commit = %d,%v want %d,%v", a, aok, b, bok)
				}
			}
		}
	})
}
