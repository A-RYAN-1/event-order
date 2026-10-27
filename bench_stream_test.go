package eventorder

import (
	"testing"
	"time"
)

// BenchmarkAlignedObserve is one source report against eight sources.
func BenchmarkAlignedObserve(b *testing.B) {
	b.ReportAllocs()
	a := NewAligned[int]()
	for s := 0; s < 8; s++ {
		a.Register(s)
	}
	for i := 0; i < b.N; i++ {
		a.Observe(i&7, t0.Add(time.Duration(i)*time.Millisecond))
	}
}

// BenchmarkStreamIngest is the consumer path per message without ids: track the
// offset, buffer, release, mark done. Eight keys, events shuffled inside a
// window of 256. Dedup.Seen is benchmarked on its own.
func BenchmarkStreamIngest(b *testing.B) {
	b.ReportAllocs()
	ts := disordered(1<<16, 256)
	s := NewStream[int, int](WithBuffer(WithMaxDelay(256 * time.Millisecond)))
	var next [8]int64
	for i := 0; i < b.N; i++ {
		key := i & 7
		t := ts[i&(1<<16-1)].Add(time.Duration(i>>16) * time.Hour)
		next[key]++
		for _, r := range s.Ingest(key, next[key], t, "", i) {
			s.Done(key, r.Offset)
		}
	}
}
