package eventorder

import (
	"math/rand"
	"slices"
	"testing"
	"time"
)

var t0 = time.Date(2026, 10, 22, 12, 0, 0, 0, time.UTC)

func at(sec int) time.Time { return t0.Add(time.Duration(sec) * time.Second) }

func values(evs []Event[string]) []string {
	var out []string
	for _, e := range evs {
		out = append(out, e.Value)
	}
	return out
}

func TestReleasesInEventTimeOrder(t *testing.T) {
	b := New[string](WithMaxDelay(5 * time.Second))
	var got []Event[string]
	for _, e := range []struct {
		s int
		v string
	}{{3, "c"}, {1, "a"}, {2, "b"}, {10, "j"}, {6, "f"}, {20, "t"}} {
		got = append(got, b.Add(at(e.s), e.v)...)
	}
	got = append(got, b.Flush()...)
	want := []string{"a", "b", "c", "f", "j", "t"}
	if g := values(got); !slices.Equal(g, want) {
		t.Fatalf("got %v want %v", g, want)
	}
}

func TestHoldsEventsUntilWatermarkPasses(t *testing.T) {
	b := New[string](WithMaxDelay(5 * time.Second))
	if out := b.Add(at(10), "x"); len(out) != 0 {
		t.Fatalf("released too early: %v", values(out))
	}
	if out := b.Add(at(14), "y"); len(out) != 0 {
		t.Fatalf("released too early: %v", values(out))
	}
	// watermark = 16 - 5 - 1ns is just under 11: x (10) goes, y (14) stays
	out := b.Add(at(16), "z")
	if g := values(out); !slices.Equal(g, []string{"x"}) {
		t.Fatalf("got %v", g)
	}
}

func TestZeroDelayKeepsEqualTimesTogether(t *testing.T) {
	b := New[string]()
	if out := b.Add(at(1), "first"); len(out) != 0 {
		t.Fatalf("got %v", values(out))
	}
	if out := b.Add(at(1), "second"); len(out) != 0 {
		t.Fatalf("an equal time must not be late: %v", values(out))
	}
	out := b.Add(at(2), "next")
	if g := values(out); !slices.Equal(g, []string{"first", "second"}) {
		t.Fatalf("equal times must keep arrival order, got %v", g)
	}
	if b.Stats().Late != 0 {
		t.Fatal("no event was late")
	}
}

func TestLateDropAndCount(t *testing.T) {
	b := New[string](WithMaxDelay(2 * time.Second))
	b.Add(at(10), "a")
	b.Add(at(20), "b") // watermark just under 18, releases a
	out := b.Add(at(5), "old")
	if len(out) != 0 {
		t.Fatalf("late event should be dropped, got %v", values(out))
	}
	if s := b.Stats(); s.Late != 1 || s.Buffered != 1 {
		t.Fatalf("stats %+v", s)
	}
}

func TestLateEmitIsFlagged(t *testing.T) {
	b := New[string](WithMaxDelay(2*time.Second), WithLatePolicy(Emit))
	b.Add(at(10), "a")
	b.Add(at(20), "b")
	out := b.Add(at(5), "old")
	if len(out) != 1 || !out[0].Late || out[0].Value != "old" {
		t.Fatalf("got %+v", out)
	}
}

func TestMaxBufferedBoundsMemory(t *testing.T) {
	b := New[int](WithMaxDelay(time.Hour), WithMaxBuffered(3))
	var all []Event[int]
	for i := 1; i <= 10; i++ {
		all = append(all, b.Add(at(i), i)...)
		if b.Stats().Buffered > 3 {
			t.Fatalf("buffered %d > cap", b.Stats().Buffered)
		}
	}
	all = append(all, b.Flush()...)
	for i := range all {
		if all[i].Value != i+1 {
			t.Fatalf("order broke at %d: %v", i, all)
		}
	}
}

func TestAdvanceToReleasesWithoutEvents(t *testing.T) {
	b := New[string](WithMaxDelay(time.Hour))
	b.Add(at(1), "a")
	b.Add(at(2), "b")
	out := b.AdvanceTo(at(1))
	if g := values(out); !slices.Equal(g, []string{"a"}) {
		t.Fatalf("got %v", g)
	}
	if out := b.AdvanceTo(at(0)); len(out) != 0 || !b.Stats().Watermark.Equal(at(1)) {
		t.Fatal("watermark must not move backwards")
	}
}

func TestNegativeDelayIsZero(t *testing.T) {
	b := New[string](WithMaxDelay(-time.Minute))
	b.Add(at(1), "a")
	if out := b.Add(at(2), "b"); len(out) != 1 {
		t.Fatalf("got %v", values(out))
	}
}

// Any shuffle whose displacement stays inside the delay must come out sorted
// with nothing late.
func TestBoundedDisorderComesOutSorted(t *testing.T) {
	rng := rand.New(rand.NewSource(1))
	const n, delay = 5000, 30
	times := make([]int, n)
	for i := range times {
		times[i] = i
	}
	// shuffle inside windows of size delay: no event moves further than delay
	for i := 0; i+delay <= n; i += delay {
		rng.Shuffle(delay, func(a, c int) { times[i+a], times[i+c] = times[i+c], times[i+a] })
	}
	b := New[int](WithMaxDelay(delay * time.Second))
	var got []int
	for _, s := range times {
		for _, e := range b.Add(at(s), s) {
			got = append(got, e.Value)
		}
	}
	for _, e := range b.Flush() {
		got = append(got, e.Value)
	}
	if len(got) != n || !slices.IsSorted(got) {
		t.Fatalf("len %d sorted %v", len(got), slices.IsSorted(got))
	}
	if b.Stats().Late != 0 {
		t.Fatalf("late %d", b.Stats().Late)
	}
}

func BenchmarkAdd(b *testing.B) {
	buf := New[int](WithMaxDelay(time.Second), WithMaxBuffered(1024))
	rng := rand.New(rand.NewSource(1))
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		buf.Add(t0.Add(time.Duration(i)*time.Millisecond+time.Duration(rng.Intn(500))*time.Millisecond), i)
	}
}

func TestAddToReusesSliceAndMatchesAdd(t *testing.T) {
	base := time.Unix(1_700_000_000, 0)
	a := New[int](WithMaxDelay(2 * time.Second))
	c := New[int](WithMaxDelay(2 * time.Second))
	var buf []Event[int]
	for i, off := range []int{5, 1, 4, 2, 9, 3, 12, 0, 20} {
		ts := base.Add(time.Duration(off) * time.Second)
		want := a.Add(ts, i)
		buf = c.AddTo(buf[:0], ts, i)
		if len(want) != len(buf) {
			t.Fatalf("step %d: Add released %d, AddTo %d", i, len(want), len(buf))
		}
		for k := range want {
			if want[k] != buf[k] {
				t.Fatalf("step %d: event %d differs: %v vs %v", i, k, want[k], buf[k])
			}
		}
	}
}

func TestOnLateSeesDroppedEvents(t *testing.T) {
	base := time.Unix(1_700_000_000, 0)
	b := New[string]()
	var got []string
	b.OnLate(func(e Event[string]) {
		if !e.Late {
			t.Errorf("OnLate event not flagged Late")
		}
		got = append(got, e.Value)
	})
	b.Add(base.Add(10*time.Second), "on time")
	b.Add(base, "too late")
	if len(got) != 1 || got[0] != "too late" {
		t.Fatalf("OnLate got %v", got)
	}
	if b.Stats().Late != 1 || b.Len() != 1 {
		t.Fatalf("stats %+v len %d", b.Stats(), b.Len())
	}
}

func TestPeakAndWatermarkAccessors(t *testing.T) {
	base := time.Unix(1_700_000_000, 0)
	b := New[int](WithMaxDelay(time.Minute))
	if !b.Watermark().IsZero() {
		t.Fatal("watermark should start zero")
	}
	for i := 0; i < 5; i++ {
		b.Add(base.Add(time.Duration(i)*time.Second), i)
	}
	if s := b.Stats(); s.Peak != 5 || s.Buffered != 5 {
		t.Fatalf("stats %+v", s)
	}
	b.Flush()
	if s := b.Stats(); s.Peak != 5 || s.Buffered != 0 || !b.Watermark().Equal(base.Add(4*time.Second)) {
		t.Fatalf("after flush %+v wm %v", s, b.Watermark())
	}
}
