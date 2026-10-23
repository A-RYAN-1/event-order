package eventorder

import (
	"slices"
	"testing"
	"time"
)

func TestPercentileDelayNearestRank(t *testing.T) {
	s := PercentileDelay(0.9, 10)()
	if s.Delay() != 0 {
		t.Fatalf("empty window delay %v", s.Delay())
	}
	for i := 1; i <= 10; i++ {
		s.Observe(time.Duration(i) * time.Second)
	}
	if got := s.Delay(); got != 9*time.Second {
		t.Fatalf("p90 of 1..10s = %v, want 9s", got)
	}
}

func TestPercentileDelayWindowSlides(t *testing.T) {
	s := PercentileDelay(1, 3)()
	for _, l := range []time.Duration{50, 1, 2} {
		s.Observe(l * time.Second)
	}
	if got := s.Delay(); got != 50*time.Second {
		t.Fatalf("max over window = %v", got)
	}
	s.Observe(3 * time.Second) // pushes the 50s lag out
	if got := s.Delay(); got != 3*time.Second {
		t.Fatalf("after the outlier left the window = %v, want 3s", got)
	}
}

func TestPercentileDelayClampsArguments(t *testing.T) {
	s := PercentileDelay(5, 0)() // q clamped to 1, window to 1
	s.Observe(time.Second)
	s.Observe(2 * time.Second)
	if got := s.Delay(); got != 2*time.Second {
		t.Fatalf("got %v", got)
	}
	z := PercentileDelay(-1, 4)()
	z.Observe(7 * time.Second)
	z.Observe(3 * time.Second)
	if got := z.Delay(); got != 3*time.Second {
		t.Fatalf("q below 0 should give the minimum, got %v", got)
	}
}

func TestStrategyAdaptsToObservedDisorder(t *testing.T) {
	b := New[int](WithDelayStrategy(PercentileDelay(1, 50)))
	var got []Event[int]
	// ordered feed first: nothing is held back beyond the single newest time
	for i := 0; i < 20; i++ {
		got = append(got, b.Add(at(i), i)...)
	}
	// then events arrive 4 seconds behind the newest; the strategy learns 4s
	for i := 20; i < 60; i++ {
		got = append(got, b.Add(at(i+4), i+4)...)
		got = append(got, b.Add(at(i), i)...)
	}
	got = append(got, b.Flush()...)
	for i := 1; i < len(got); i++ {
		if got[i].Time.Before(got[i-1].Time) {
			t.Fatalf("out of order at %d: %v after %v", i, got[i].Time, got[i-1].Time)
		}
	}
}

func TestStrategyFloorIsMaxDelay(t *testing.T) {
	b := New[string](WithMaxDelay(5*time.Second), WithDelayStrategy(PercentileDelay(0.5, 10)))
	b.Add(at(10), "x")
	if out := b.Add(at(14), "y"); len(out) != 0 {
		t.Fatalf("floor of 5s ignored: %v", values(out))
	}
	if out := b.Add(at(16), "z"); !slices.Equal(values(out), []string{"x"}) {
		t.Fatalf("got %v", values(out))
	}
}

func TestStrategyCountsLateEventsToo(t *testing.T) {
	b := New[int](WithDelayStrategy(PercentileDelay(1, 4)))
	b.Add(at(100), 1)
	b.Add(at(0), 2) // 100s late and dropped; the strategy still sees the lag
	if d := b.strat.Delay(); d != 100*time.Second {
		t.Fatalf("delay %v, want 100s", d)
	}
	if b.Stats().Late != 1 {
		t.Fatalf("late %d", b.Stats().Late)
	}
}

func TestKeyedStrategiesAreIndependent(t *testing.T) {
	k := NewKeyed[string, int](WithDelayStrategy(PercentileDelay(1, 4)))
	k.Add("a", at(100), 1)
	k.Add("a", at(0), 2) // teaches key a a 100s delay
	k.Add("b", at(100), 1)
	if k.bufs["b"].strat.Delay() != 0 {
		t.Fatal("key b inherited key a's lag")
	}
}

func TestNilStrategyFactoryKeepsFixedDelay(t *testing.T) {
	for _, f := range []func() DelayStrategy{nil, func() DelayStrategy { return nil }} {
		b := New[int](WithMaxDelay(time.Second), WithDelayStrategy(f))
		b.Add(at(5), 1)
		if out := b.Add(at(5), 2); len(out) != 0 {
			t.Fatalf("got %v", out)
		}
	}
}

func TestPercentileObserveAllocatesNothing(t *testing.T) {
	s := PercentileDelay(0.99, 256)()
	for i := 0; i < 256; i++ {
		s.Observe(time.Duration(i))
	}
	n := testing.AllocsPerRun(1000, func() {
		s.Observe(time.Duration(37))
		_ = s.Delay()
	})
	if n != 0 {
		t.Fatalf("%v allocations per Observe", n)
	}
}
