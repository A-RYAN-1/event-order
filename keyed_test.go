package eventorder

import (
	"iter"
	"slices"
	"testing"
	"time"
)

func TestKeyedIsIndependent(t *testing.T) {
	k := NewKeyed[string, string](WithMaxDelay(time.Second))
	k.Add("a", at(1), "a1")
	k.Add("b", at(100), "b100") // far ahead on b must not release anything on a
	if s, _ := k.Stats("a"); s.Buffered != 1 || s.Released != 0 {
		t.Fatalf("a stats %+v", s)
	}
	out := k.Add("a", at(3), "a3")
	if g := values(out); !slices.Equal(g, []string{"a1"}) {
		t.Fatalf("got %v", g)
	}
}

func TestKeyedFlushForgetsKey(t *testing.T) {
	k := NewKeyed[int, string](WithMaxDelay(time.Minute))
	k.Add(1, at(2), "two")
	k.Add(1, at(1), "one")
	if g := values(k.Flush(1)); !slices.Equal(g, []string{"one", "two"}) {
		t.Fatalf("got %v", g)
	}
	if k.Len() != 0 {
		t.Fatalf("len %d", k.Len())
	}
	if k.Flush(1) != nil || k.AdvanceTo(9, at(1)) != nil {
		t.Fatal("unknown key must release nothing")
	}
}

func TestKeyedFlushAll(t *testing.T) {
	k := NewKeyed[int, string](WithMaxDelay(time.Minute))
	k.Add(1, at(2), "b")
	k.Add(1, at(1), "a")
	k.Add(2, at(1), "x")
	seen := map[int][]string{}
	k.FlushAll(func(key int, evs []Event[string]) { seen[key] = values(evs) })
	if !slices.Equal(seen[1], []string{"a", "b"}) || !slices.Equal(seen[2], []string{"x"}) || k.Len() != 0 {
		t.Fatalf("got %v len %d", seen, k.Len())
	}
}

func TestReorderIterator(t *testing.T) {
	in := []Event[string]{{Time: at(3), Value: "c"}, {Time: at(1), Value: "a"}, {Time: at(2), Value: "b"}}
	seq := iter.Seq[Event[string]](func(yield func(Event[string]) bool) {
		for _, e := range in {
			if !yield(e) {
				return
			}
		}
	})
	var got []string
	for e := range Reorder(seq, WithMaxDelay(5*time.Second)) {
		got = append(got, e.Value)
	}
	if !slices.Equal(got, []string{"a", "b", "c"}) {
		t.Fatalf("got %v", got)
	}
	// breaking out early must not panic
	for range Reorder(seq) {
		break
	}
}
