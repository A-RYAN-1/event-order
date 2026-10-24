package eventorder

import (
	"iter"
	"slices"
	"testing"
	"time"
)

// FuzzKeyed: Keyed must equal one reference buffer per key, for any bytes.
func FuzzKeyed(f *testing.F) {
	f.Add([]byte{0, 5, 1, 1, 0, 4, 2, 9, 1, 3}, uint8(2), uint8(0), true)
	f.Add([]byte{0, 0, 1, 0, 2, 0, 0, 255, 1, 255}, uint8(0), uint8(2), false)
	f.Fuzz(func(t *testing.T, data []byte, delay uint8, cap uint8, emit bool) {
		proto := refBuffer{delay: time.Duration(delay) * time.Second, cap: int(cap), emit: emit}
		k := NewKeyed[int, int](proto.options()...)
		refs := map[int]*refBuffer{}
		for i := 0; i+1 < len(data); i += 2 {
			key := int(data[i] % 4)
			ref := refs[key]
			if ref == nil {
				ref = &refBuffer{delay: proto.delay, cap: proto.cap, emit: proto.emit}
				refs[key] = ref
			}
			ts := t0.Add(time.Duration(data[i+1]) * time.Second)
			got, want := k.Add(key, ts, i), ref.add(ts, i)
			if !sameEvents(got, want) {
				t.Fatalf("key %d step %d:\n got  %s\n want %s", key, i, describe(got), describe(want))
			}
		}
		k.FlushAll(func(key int, got []Event[int]) {
			if want := refs[key].flush(); !sameEvents(got, want) {
				t.Fatalf("FlushAll key %d:\n got  %s\n want %s", key, describe(got), describe(want))
			}
		})
	})
}

// FuzzMerge: for sources that are each in order, Merge equals a stable sort by
// time then source, and it stops reading when the consumer stops.
func FuzzMerge(f *testing.F) {
	f.Add([]byte{0, 1, 1, 0, 2, 3, 0, 0, 3, 5}, uint8(3))
	f.Add([]byte{0, 0, 0, 0, 1, 0, 1, 0}, uint8(0))
	f.Fuzz(func(t *testing.T, data []byte, stopAfter uint8) {
		const sources = 4
		var evs [sources][]Event[int]
		var clock [sources]int
		total := 0
		for i := 0; i+1 < len(data); i += 2 {
			s := int(data[i] % sources)
			clock[s] += int(data[i+1] % 8) // non-decreasing inside a source
			evs[s] = append(evs[s], Event[int]{Time: t0.Add(time.Duration(clock[s]) * time.Second), Value: s*1000 + i})
			total++
		}
		seqs := make([]iter.Seq[Event[int]], sources)
		var want []Event[int]
		for s := range seqs {
			list := evs[s]
			want = append(want, list...)
			seqs[s] = func(yield func(Event[int]) bool) {
				for _, e := range list {
					if !yield(e) {
						return
					}
				}
			}
		}
		slices.SortStableFunc(want, func(a, b Event[int]) int { return a.Time.Compare(b.Time) })
		var got []Event[int]
		for e := range Merge(seqs...) {
			got = append(got, e)
		}
		if !sameEvents(got, want) {
			t.Fatalf("merge differs:\n got  %s\n want %s", describe(got), describe(want))
		}
		partial := 0
		for range Merge(seqs...) {
			partial++
			if partial > int(stopAfter) {
				break
			}
		}
		if wantN := min(total, int(stopAfter)+1); partial != wantN {
			t.Fatalf("early stop read %d events, want %d", partial, wantN)
		}
	})
}
