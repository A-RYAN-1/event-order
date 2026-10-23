package eventorder_test

import (
	"fmt"
	"time"

	"github.com/A-RYAN-1/event-order"
)

func ExampleBuffer() {
	t0 := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	b := eventorder.New[string](eventorder.WithMaxDelay(5 * time.Second))

	// Readings arrive shuffled; event times are 3s, 1s, 2s, then a much later one.
	for _, r := range []struct {
		sec int
		val string
	}{{3, "c"}, {1, "a"}, {2, "b"}, {20, "z"}} {
		for _, ev := range b.Add(t0.Add(time.Duration(r.sec)*time.Second), r.val) {
			fmt.Println(ev.Value)
		}
	}
	// Output:
	// a
	// b
	// c
}
