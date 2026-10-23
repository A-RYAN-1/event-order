package eventorder

import (
	"testing"
	"time"
)

// Properties for any input: output is sorted, nothing is lost or invented,
// and without a cap nothing arrives out of order after release.
func FuzzBuffer(f *testing.F) {
	f.Add([]byte{5, 1, 4, 2, 9, 3}, uint8(2), uint8(0))
	f.Add([]byte{0, 0, 0, 255, 1}, uint8(0), uint8(3))
	f.Fuzz(func(t *testing.T, offs []byte, delay uint8, cap uint8) {
		base := time.Unix(1_700_000_000, 0)
		b := New[int](
			WithMaxDelay(time.Duration(delay)*time.Second),
			WithMaxBuffered(int(cap)),
			WithLatePolicy(Emit),
		)
		var inOrder []Event[int]
		var late int
		collect := func(evs []Event[int]) {
			for _, e := range evs {
				if e.Late {
					late++
				} else {
					inOrder = append(inOrder, e)
				}
			}
		}
		for i, o := range offs {
			collect(b.Add(base.Add(time.Duration(o)*time.Second), i))
		}
		collect(b.Flush())
		if len(inOrder)+late != len(offs) {
			t.Fatalf("lost events: %d in order + %d late != %d in", len(inOrder), late, len(offs))
		}
		for i := 1; i < len(inOrder); i++ {
			if inOrder[i].Time.Before(inOrder[i-1].Time) {
				t.Fatalf("out of order at %d: %v after %v", i, inOrder[i].Time, inOrder[i-1].Time)
			}
		}
		if cap == 0 && int(delay) >= 255 && late != 0 {
			t.Fatalf("delay covers all offsets but %d events were late", late)
		}
	})
}
