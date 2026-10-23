package eventorder

import (
	"context"
	"sync"
	"testing"
	"time"
)

func TestPipeOrdersAndFlushesOnClose(t *testing.T) {
	base := time.Unix(1_700_000_000, 0)
	in := make(chan Event[int])
	out := Pipe(context.Background(), in, WithMaxDelay(3*time.Second))
	go func() {
		for _, off := range []int{4, 2, 3, 1, 8, 6, 7, 5} {
			in <- Event[int]{Time: base.Add(time.Duration(off) * time.Second), Value: off}
		}
		close(in)
	}()
	var got []int
	for e := range out {
		got = append(got, e.Value)
	}
	if len(got) != 8 {
		t.Fatalf("lost events: %v", got)
	}
	for i := 1; i < len(got); i++ {
		if got[i] < got[i-1] {
			t.Fatalf("out of order: %v", got)
		}
	}
}

func TestPipeIdleTimeoutReleasesHeldEvents(t *testing.T) {
	base := time.Unix(1_700_000_000, 0)
	in := make(chan Event[string])
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	out := Pipe(ctx, in, WithMaxDelay(time.Hour), WithIdleTimeout(30*time.Millisecond))
	in <- Event[string]{Time: base.Add(2 * time.Second), Value: "b"}
	in <- Event[string]{Time: base.Add(time.Second), Value: "a"}
	var got []string
	deadline := time.After(2 * time.Second)
	for len(got) < 2 {
		select {
		case e := <-out:
			got = append(got, e.Value)
		case <-deadline:
			t.Fatalf("idle timeout never released events, got %v", got)
		}
	}
	if got[0] != "a" || got[1] != "b" {
		t.Fatalf("got %v", got)
	}
}

func TestPipeStopsOnContextCancel(t *testing.T) {
	in := make(chan Event[int])
	ctx, cancel := context.WithCancel(context.Background())
	out := Pipe(ctx, in)
	cancel()
	select {
	case _, ok := <-out:
		if ok {
			t.Fatal("expected closed output")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Pipe did not stop after cancel")
	}
}

func TestSafeUnderConcurrency(t *testing.T) {
	base := time.Unix(1_700_000_000, 0)
	s := NewSafe[int](WithMaxDelay(time.Second), WithLatePolicy(Emit))
	var wg sync.WaitGroup
	var mu sync.Mutex
	total := 0
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < 500; i++ {
				n := len(s.Add(base.Add(time.Duration(i)*time.Millisecond), g))
				mu.Lock()
				total += n
				mu.Unlock()
			}
		}(g)
	}
	wg.Wait()
	total += len(s.Flush())
	if total != 4000 {
		t.Fatalf("released %d of 4000", total)
	}
	s.OnLate(nil)
	s.AddTo(nil, base, 1)
	s.AdvanceTo(base)
	if s.Stats().Released == 0 {
		t.Fatal("stats empty")
	}
}

func TestSafeKeyedBasics(t *testing.T) {
	base := time.Unix(1_700_000_000, 0)
	s := NewSafeKeyed[string, int]()
	var lateKeys []string
	s.OnLate(func(k string, _ Event[int]) { lateKeys = append(lateKeys, k) })
	s.Add("a", base.Add(10*time.Second), 1)
	s.Add("a", base, 2) // late
	if len(lateKeys) != 1 || lateKeys[0] != "a" {
		t.Fatalf("late keys %v", lateKeys)
	}
	if st, ok := s.Stats("a"); !ok || st.Late != 1 {
		t.Fatalf("stats %+v %v", st, ok)
	}
	if s.Len() != 1 {
		t.Fatal("len")
	}
	s.AdvanceTo("a", base.Add(20*time.Second))
	s.AdvanceAll(base.Add(30*time.Second), nil)
	s.Add("b", base, 3)
	flushed := 0
	s.FlushAll(func(string, []Event[int]) { flushed++ })
	if flushed != 1 || len(s.Flush("zzz")) != 0 || s.Len() != 0 {
		t.Fatalf("flushed %d len %d", flushed, s.Len())
	}
}

func TestKeyedAdvanceAllForgetsEmptyKeys(t *testing.T) {
	base := time.Unix(1_700_000_000, 0)
	k := NewKeyed[int, string](WithMaxDelay(time.Minute))
	k.Add(1, base, "x")
	k.Add(2, base.Add(time.Hour), "y")
	got := map[int][]string{}
	k.AdvanceAll(base.Add(30*time.Minute), func(key int, evs []Event[string]) {
		for _, e := range evs {
			got[key] = append(got[key], e.Value)
		}
	})
	if len(got[1]) != 1 || len(got[2]) != 0 {
		t.Fatalf("released %v", got)
	}
	if k.Len() != 1 {
		t.Fatalf("empty key not forgotten, Len=%d", k.Len())
	}
	k.OnLate(nil)
}
