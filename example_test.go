package eventorder_test

import (
	"context"
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

func ExamplePipe() {
	t0 := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	in := make(chan eventorder.Event[string])
	out := eventorder.Pipe(context.Background(), in, eventorder.WithMaxDelay(5*time.Second))
	go func() {
		defer close(in)
		for _, r := range []struct {
			sec int
			val string
		}{{3, "c"}, {1, "a"}, {2, "b"}} {
			in <- eventorder.Event[string]{Time: t0.Add(time.Duration(r.sec) * time.Second), Value: r.val}
		}
	}()
	for ev := range out {
		fmt.Println(ev.Value)
	}
	// Output:
	// a
	// b
	// c
}

func ExampleKeyed() {
	t0 := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	k := eventorder.NewKeyed[string, int](eventorder.WithMaxDelay(time.Second))
	k.Add("sensor-1", t0.Add(2*time.Second), 20)
	k.Add("sensor-1", t0.Add(1*time.Second), 10)
	k.Add("sensor-2", t0, 99) // its own clock: not held back by sensor-1
	k.FlushAll(func(key string, evs []eventorder.Event[int]) {
		if key == "sensor-1" {
			for _, e := range evs {
				fmt.Println(key, e.Value)
			}
		}
	})
	// Output:
	// sensor-1 10
	// sensor-1 20
}

func ExampleBuffer_OnLate() {
	t0 := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	b := eventorder.New[string]()
	b.OnLate(func(e eventorder.Event[string]) { fmt.Println("dead letter:", e.Value) })
	b.Add(t0.Add(10*time.Second), "now")
	b.Add(t0, "stale")
	// Output:
	// dead letter: stale
}
