package eventorder

import (
	"testing"
	"time"
)

// BenchmarkTrackerCycle is one Track, one Done and one Commit per iteration,
// with a lag of 64 offsets between Track and Done.
func BenchmarkTrackerCycle(b *testing.B) {
	tr := NewTracker()
	const lag = 64
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		tr.Track(int64(i))
		if i >= lag {
			tr.Done(int64(i - lag))
		}
		tr.Commit()
	}
}

// BenchmarkDedupSeen is one Seen per iteration over a stream with every id sent
// twice, a window of 1,024 one-millisecond events.
func BenchmarkDedupSeen(b *testing.B) {
	d := NewDedup[int](1024*time.Millisecond, 0)
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		id := i / 2
		d.Seen(t0.Add(time.Duration(id)*time.Millisecond), id)
	}
}
