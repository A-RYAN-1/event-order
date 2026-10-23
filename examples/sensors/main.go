// Command sensors feeds shuffled temperature readings into a Buffer and
// prints them in the order they happened.
package main

import (
	"fmt"
	"time"

	"github.com/A-RYAN-1/event-order"
)

func main() {
	start := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	b := eventorder.New[string](
		eventorder.WithMaxDelay(3*time.Second),
		eventorder.WithLatePolicy(eventorder.Emit),
	)

	// Arrival order is not event order, and the last reading is far too late.
	arrivals := []struct {
		sec int
		val string
	}{{2, "21.4C"}, {1, "21.3C"}, {4, "21.7C"}, {3, "21.5C"}, {9, "22.1C"}, {2, "21.4C (retry)"}}

	for _, r := range arrivals {
		for _, ev := range b.Add(start.Add(time.Duration(r.sec)*time.Second), r.val) {
			tag := ""
			if ev.Late {
				tag = "  <- late"
			}
			fmt.Printf("t+%ds  %s%s\n", int(ev.Time.Sub(start).Seconds()), ev.Value, tag)
		}
	}
	for _, ev := range b.Flush() {
		fmt.Printf("t+%ds  %s  (flushed)\n", int(ev.Time.Sub(start).Seconds()), ev.Value)
	}
	fmt.Printf("%+v\n", b.Stats())
}
