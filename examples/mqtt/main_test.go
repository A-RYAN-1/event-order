package main

import (
	"context"
	"encoding/json"
	"testing"
	"time"
)

var t0 = time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)

func payload(sec int) []byte {
	b, _ := json.Marshal(Reading{Device: "d1", Time: t0.Add(time.Duration(sec) * time.Second), Value: float64(sec)})
	return b
}

func TestOrderedPutsReadingsInOrderAndSkipsGarbage(t *testing.T) {
	in := make(chan []byte)
	out := ordered(context.Background(), in, 5*time.Second, 0)
	go func() {
		for _, sec := range []int{3, 1, 2, 5, 4} {
			in <- payload(sec)
		}
		in <- []byte("not json")
		close(in)
	}()
	var got []float64
	for ev := range out {
		got = append(got, ev.Value.Value)
	}
	want := []float64{1, 2, 3, 4, 5}
	if len(got) != len(want) {
		t.Fatalf("got %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("got %v, want %v", got, want)
		}
	}
}

func TestOrderedReleasesTheTailWhenTheBrokerGoesQuiet(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	in := make(chan []byte)
	out := ordered(ctx, in, time.Hour, 50*time.Millisecond) // a delay far longer than the test
	in <- payload(2)
	in <- payload(1)
	select {
	case ev := <-out:
		if ev.Value.Value != 1 {
			t.Fatalf("first released %v, want 1", ev.Value.Value)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("idle timeout did not release the held readings")
	}
	if ev := <-out; ev.Value.Value != 2 {
		t.Fatalf("second released %v, want 2", ev.Value.Value)
	}
}
