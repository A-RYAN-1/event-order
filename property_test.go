package eventorder

import (
	"fmt"
	"math/rand"
	"slices"
	"testing"
	"time"
)

func sameEvents(a, b []Event[int]) bool {
	return slices.EqualFunc(a, b, func(x, y Event[int]) bool {
		return x.Time.Equal(y.Time) && x.Value == y.Value && x.Late == y.Late
	})
}

func describe(evs []Event[int]) string {
	s := make([]string, len(evs))
	for i, e := range evs {
		s[i] = fmt.Sprintf("%d@%dms", e.Value, e.Time.Sub(t0).Milliseconds())
		if e.Late {
			s[i] += "L"
		}
	}
	return fmt.Sprint(s)
}

// randomRef picks options at random, including the awkward ones: no delay,
// a tiny cap, Drop and Emit.
func randomRef(r *rand.Rand) *refBuffer {
	ref := &refBuffer{
		delay: time.Duration(r.Intn(21)) * time.Second,
		emit:  r.Intn(2) == 0,
	}
	if r.Intn(2) == 0 {
		ref.cap = 1 + r.Intn(8)
	}
	return ref
}

// randomTime is spread over a few seconds with millisecond steps so that ties,
// near ties and exact-boundary times all occur.
func randomTime(r *rand.Rand, step int) time.Time {
	return t0.Add(time.Duration(step*100+r.Intn(30_000)-15_000) * time.Millisecond)
}

// TestBufferMatchesReference runs 3000 random operation sequences (Add,
// AdvanceTo, Flush) through Buffer and the sort-every-step reference and
// compares every call's output exactly.
func TestBufferMatchesReference(t *testing.T) {
	for seed := int64(0); seed < 3000; seed++ {
		r := rand.New(rand.NewSource(seed))
		ref := randomRef(r)
		b := New[int](ref.options()...)
		for step := 0; step < 120; step++ {
			var got, want []Event[int]
			var op string
			switch n := r.Intn(100); {
			case n < 85:
				ts := randomTime(r, step)
				op = fmt.Sprintf("add %dms", ts.Sub(t0).Milliseconds())
				got, want = b.Add(ts, step), ref.add(ts, step)
			case n < 95:
				ts := randomTime(r, step)
				op = fmt.Sprintf("advance %dms", ts.Sub(t0).Milliseconds())
				got, want = b.AdvanceTo(ts), ref.advanceTo(ts)
			default:
				op = "flush"
				got, want = b.Flush(), ref.flush()
			}
			if !sameEvents(got, want) {
				t.Fatalf("seed %d step %d (%s, delay %v cap %d emit %v):\n got  %s\n want %s",
					seed, step, op, ref.delay, ref.cap, ref.emit, describe(got), describe(want))
			}
			if b.Len() != len(ref.held) {
				t.Fatalf("seed %d step %d: Len %d, reference holds %d", seed, step, b.Len(), len(ref.held))
			}
		}
	}
}

// TestKeyedMatchesPerKeyReference checks that Keyed is exactly one reference
// buffer per key, however the keys interleave.
func TestKeyedMatchesPerKeyReference(t *testing.T) {
	for seed := int64(0); seed < 1000; seed++ {
		r := rand.New(rand.NewSource(seed))
		proto := randomRef(r)
		k := NewKeyed[int, int](proto.options()...)
		refs := map[int]*refBuffer{}
		for step := 0; step < 150; step++ {
			key := r.Intn(5)
			ref := refs[key]
			if ref == nil {
				ref = &refBuffer{delay: proto.delay, cap: proto.cap, emit: proto.emit}
				refs[key] = ref
			}
			ts := randomTime(r, step)
			got, want := k.Add(key, ts, step), ref.add(ts, step)
			if !sameEvents(got, want) {
				t.Fatalf("seed %d step %d key %d:\n got  %s\n want %s", seed, step, key, describe(got), describe(want))
			}
		}
		k.FlushAll(func(key int, got []Event[int]) {
			if want := refs[key].flush(); !sameEvents(got, want) {
				t.Fatalf("seed %d FlushAll key %d:\n got  %s\n want %s", seed, key, describe(got), describe(want))
			}
		})
	}
}

// TestLateExactlyWhenBeyondDelay checks the documented guarantee without the
// reference: with no cap, no AdvanceTo and no Flush, an event is late exactly
// when it is more than the delay behind the latest event seen before it.
func TestLateExactlyWhenBeyondDelay(t *testing.T) {
	for seed := int64(0); seed < 1000; seed++ {
		r := rand.New(rand.NewSource(seed))
		delay := time.Duration(r.Intn(10_000)) * time.Millisecond
		b := New[int](WithMaxDelay(delay), WithLatePolicy(Emit))
		var maxSeen time.Time
		for i := 0; i < 200; i++ {
			ts := t0.Add(time.Duration(r.Intn(60_000)) * time.Millisecond)
			wantLate := !maxSeen.IsZero() && ts.Before(maxSeen.Add(-delay))
			var gotLate bool
			for _, e := range b.Add(ts, i) {
				if e.Late && e.Value == i {
					gotLate = true
				}
			}
			if gotLate != wantLate {
				t.Fatalf("seed %d event %d at %v: late=%v, want %v (latest before it %v, delay %v)",
					seed, i, ts.Sub(t0), gotLate, wantLate, maxSeen.Sub(t0), delay)
			}
			if ts.After(maxSeen) {
				maxSeen = ts
			}
		}
	}
}

// TestNothingLostOrInvented: every event added comes out exactly once, from
// Add, AdvanceTo or the final Flush, whatever the options.
func TestNothingLostOrInvented(t *testing.T) {
	for seed := int64(0); seed < 500; seed++ {
		r := rand.New(rand.NewSource(seed))
		ref := randomRef(r)
		ref.emit = true // Drop discards by design; Emit returns every event
		b := New[int](ref.options()...)
		seen := map[int]int{}
		const n = 300
		for i := 0; i < n; i++ {
			for _, e := range b.Add(randomTime(r, i), i) {
				seen[e.Value]++
			}
			if r.Intn(10) == 0 {
				for _, e := range b.AdvanceTo(randomTime(r, i)) {
					seen[e.Value]++
				}
			}
		}
		for _, e := range b.Flush() {
			seen[e.Value]++
		}
		for i := 0; i < n; i++ {
			if seen[i] != 1 {
				t.Fatalf("seed %d: event %d came out %d times", seed, i, seen[i])
			}
		}
	}
}
