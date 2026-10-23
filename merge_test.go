package eventorder

import (
	"iter"
	"math/rand"
	"slices"
	"testing"
)

func seqOf(evs ...Event[string]) func(func(Event[string]) bool) {
	return func(yield func(Event[string]) bool) {
		for _, e := range evs {
			if !yield(e) {
				return
			}
		}
	}
}

func ev(sec int, v string) Event[string] { return Event[string]{Time: at(sec), Value: v} }

func collect(seq func(func(Event[string]) bool)) []string {
	var out []string
	for e := range seq {
		out = append(out, e.Value)
	}
	return out
}

func TestMergeInterleavesInOrder(t *testing.T) {
	got := collect(Merge(
		seqOf(ev(1, "a1"), ev(4, "a4"), ev(9, "a9")),
		seqOf(ev(2, "b2"), ev(3, "b3")),
		seqOf(ev(5, "c5")),
	))
	want := []string{"a1", "b2", "b3", "a4", "c5", "a9"}
	if !slices.Equal(got, want) {
		t.Fatalf("got %v want %v", got, want)
	}
}

func TestMergeTiesFollowSourceOrder(t *testing.T) {
	got := collect(Merge(seqOf(ev(1, "a"), ev(1, "a2")), seqOf(ev(1, "b"))))
	// equal times: the lower source drains first, each source keeps its order
	if want := []string{"a", "a2", "b"}; !slices.Equal(got, want) {
		t.Fatalf("got %v want %v", got, want)
	}
}

func TestMergeEdgeCases(t *testing.T) {
	if got := collect(Merge[string]()); len(got) != 0 {
		t.Fatalf("no sources: %v", got)
	}
	if got := collect(Merge(seqOf(), seqOf())); len(got) != 0 {
		t.Fatalf("empty sources: %v", got)
	}
	if got := collect(Merge(seqOf(), seqOf(ev(1, "x")))); !slices.Equal(got, []string{"x"}) {
		t.Fatalf("one empty source: %v", got)
	}
}

func TestMergeStopsEarlyAndStopsSources(t *testing.T) {
	stopped := 0
	src := func(evs ...Event[string]) func(func(Event[string]) bool) {
		return func(yield func(Event[string]) bool) {
			defer func() { stopped++ }()
			for _, e := range evs {
				if !yield(e) {
					return
				}
			}
		}
	}
	n := 0
	for range Merge(src(ev(1, "a"), ev(3, "c")), src(ev(2, "b"), ev(4, "d"))) {
		n++
		if n == 2 {
			break
		}
	}
	if n != 2 || stopped != 2 {
		t.Fatalf("read %d events, stopped %d sources", n, stopped)
	}
}

func TestMergeMatchesSortedUnion(t *testing.T) {
	r := rand.New(rand.NewSource(7))
	for trial := 0; trial < 200; trial++ {
		var srcs []iter.Seq[Event[string]]
		var all []Event[string]
		for s := 0; s < 1+r.Intn(6); s++ {
			var evs []Event[string]
			sec := r.Intn(5)
			for i := 0; i < r.Intn(30); i++ {
				sec += r.Intn(4) // sorted within the source, ties allowed
				evs = append(evs, ev(sec, string(rune('A'+s))+string(rune('a'+i%26))))
			}
			srcs = append(srcs, seqOf(evs...))
			all = append(all, evs...)
		}
		var times []int
		for e := range Merge(srcs...) {
			times = append(times, int(e.Time.Sub(t0).Seconds()))
		}
		if len(times) != len(all) || !slices.IsSorted(times) {
			t.Fatalf("trial %d: %d of %d events, sorted=%v", trial, len(times), len(all), slices.IsSorted(times))
		}
	}
}
