package eventorder

import (
	"math/rand"
	"slices"
	"sync"
	"testing"
	"time"
)

// refDedup is the specification written the dumb way: a slice of remembered
// (time, id, order) entries scanned linearly. It shares no code with Dedup.
type refDedup struct {
	window time.Duration
	maxIDs int
	held   []refDup
	seq    int
	newest time.Time
	dups   int
}

type refDup struct {
	t   time.Time
	id  int
	seq int
}

func (r *refDedup) seen(t time.Time, id int) bool {
	if t.After(r.newest) {
		r.newest = t
	}
	r.held = slices.DeleteFunc(r.held, func(h refDup) bool { return r.newest.Sub(h.t) > r.window })
	for _, h := range r.held {
		if h.id == id {
			r.dups++
			return true
		}
	}
	if r.newest.Sub(t) > r.window {
		return false
	}
	r.seq++
	r.held = append(r.held, refDup{t, id, r.seq})
	for r.maxIDs > 0 && len(r.held) > r.maxIDs {
		oldest := 0
		for i, h := range r.held {
			o := r.held[oldest]
			if h.t.Before(o.t) || (h.t.Equal(o.t) && h.seq < o.seq) {
				oldest = i
			}
		}
		r.held = slices.Delete(r.held, oldest, oldest+1)
	}
	return false
}

func TestDedupBasics(t *testing.T) {
	d := NewDedup[string](10*time.Second, 0)
	if d.Seen(t0, "a") {
		t.Fatal("first sight is not a duplicate")
	}
	if !d.Seen(t0.Add(2*time.Second), "a") {
		t.Fatal("the same id again, inside the window, is a duplicate")
	}
	d.Seen(t0.Add(30*time.Second), "b") // moves the window past "a"
	if d.Seen(t0.Add(31*time.Second), "a") {
		t.Fatal("an id older than the window is forgotten")
	}
	if d.Seen(t0, "c") {
		t.Fatal("an event already older than the window is too old to tell, not a duplicate")
	}
	if d.Duplicates() != 1 {
		t.Fatalf("Duplicates = %d, want 1", d.Duplicates())
	}
	if d.Len() != 2 { // "b" and the "a" seen again; "c" was too old to remember
		t.Fatalf("Len = %d, want 2", d.Len())
	}
}

func TestDedupMatchesReference(t *testing.T) {
	for seed := int64(0); seed < 2000; seed++ {
		r := rand.New(rand.NewSource(seed))
		window := time.Duration(r.Intn(20)) * time.Second
		maxIDs := 0
		if r.Intn(2) == 0 {
			maxIDs = 1 + r.Intn(8)
		}
		d := NewDedup[int](window, maxIDs)
		ref := &refDedup{window: window, maxIDs: maxIDs}
		for step := 0; step < 150; step++ {
			ts := t0.Add(time.Duration(r.Intn(60)) * time.Second)
			id := r.Intn(25)
			if got, want := d.Seen(ts, id), ref.seen(ts, id); got != want {
				t.Fatalf("seed %d step %d: Seen(%v, %d) = %v, want %v (window %v, max %d)", seed, step, ts, id, got, want, window, maxIDs)
			}
			if d.Len() != len(ref.held) {
				t.Fatalf("seed %d step %d: Len = %d, want %d", seed, step, d.Len(), len(ref.held))
			}
			if maxIDs > 0 && d.Len() > maxIDs {
				t.Fatalf("seed %d step %d: Len %d above cap %d", seed, step, d.Len(), maxIDs)
			}
		}
		if int(d.Duplicates()) != ref.dups {
			t.Fatalf("seed %d: Duplicates = %d, want %d", seed, d.Duplicates(), ref.dups)
		}
	}
}

// TestDedupWithBuffer: every event is sent twice, the copy up to `lag` events
// later, and the pair is shuffled. With a window at least that large, each id
// must come out of the Buffer exactly once.
func TestDedupWithBuffer(t *testing.T) {
	for seed := int64(0); seed < 200; seed++ {
		r := rand.New(rand.NewSource(seed))
		const n, lag = 300, 6
		type msg struct{ id, sec, key int }
		var in []msg
		for i := 0; i < n; i++ { // the original, and a copy delivered up to lag events later
			in = append(in, msg{i, i, 2 * i}, msg{i, i, 2*i + 1 + r.Intn(2*lag)})
		}
		slices.SortStableFunc(in, func(a, b msg) int { return a.key - b.key })
		for i := 0; i < len(in); i++ { // arrival disorder
			j := min(len(in)-1, i+r.Intn(3))
			in[i], in[j] = in[j], in[i]
		}
		d := NewDedup[int](30*time.Second, 0)
		buf := New[int](WithMaxDelay(30 * time.Second))
		got := map[int]int{}
		for _, m := range in {
			ts := t0.Add(time.Duration(m.sec) * time.Second)
			if d.Seen(ts, m.id) {
				continue
			}
			for _, e := range buf.Add(ts, m.id) {
				got[e.Value]++
			}
		}
		for _, e := range buf.Flush() {
			got[e.Value]++
		}
		for id := 0; id < n; id++ {
			if got[id] != 1 {
				t.Fatalf("seed %d: id %d released %d times, want 1", seed, id, got[id])
			}
		}
	}
}

func TestDedupMemoryBound(t *testing.T) {
	d := NewDedup[int](50*time.Second, 0)
	peak := 0
	for i := 0; i < 200_000; i++ {
		d.Seen(t0.Add(time.Duration(i)*time.Second), i)
		peak = max(peak, d.Len())
	}
	if peak > 52 {
		t.Fatalf("held %d ids for a 50 s window of 1 s events", peak)
	}
	c := NewDedup[int](time.Hour, 1000)
	for i := 0; i < 200_000; i++ {
		c.Seen(t0.Add(time.Duration(i)*time.Millisecond), i)
		if c.Len() > 1000 {
			t.Fatalf("Len %d above cap 1000", c.Len())
		}
	}
}

func TestDedupStress(t *testing.T) {
	d := NewDedup[int](time.Minute, 5000)
	var wg sync.WaitGroup
	var mu sync.Mutex
	firsts := 0
	for w := 0; w < 8; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 2000; i++ {
				if !d.Seen(t0.Add(time.Duration(i)*time.Millisecond), i) {
					mu.Lock()
					firsts++
					mu.Unlock()
				}
				d.Len()
			}
		}()
	}
	wg.Wait()
	if firsts != 2000 { // each of the 2000 ids is first exactly once across workers
		t.Fatalf("%d first sights across workers, want 2000", firsts)
	}
	if d.Duplicates() != 8*2000-2000 {
		t.Fatalf("Duplicates = %d, want %d", d.Duplicates(), 8*2000-2000)
	}
}

func FuzzDedup(f *testing.F) {
	f.Add([]byte{1, 2, 3, 1, 2, 3, 9, 1}, uint8(4), uint8(0))
	f.Add([]byte{0, 0, 0, 0, 200, 0, 0, 1}, uint8(0), uint8(2))
	f.Fuzz(func(t *testing.T, data []byte, window uint8, maxIDs uint8) {
		win := time.Duration(window%30) * time.Second
		d := NewDedup[int](win, int(maxIDs%6))
		ref := &refDedup{window: win, maxIDs: int(maxIDs % 6)}
		for i := 0; i+1 < len(data); i += 2 {
			ts := t0.Add(time.Duration(data[i]) * time.Second)
			id := int(data[i+1] % 12)
			if got, want := d.Seen(ts, id), ref.seen(ts, id); got != want {
				t.Fatalf("step %d: Seen = %v, want %v", i, got, want)
			}
		}
	})
}
