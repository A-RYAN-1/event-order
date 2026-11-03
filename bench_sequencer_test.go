package eventorder

import (
	"math/rand"
	"testing"
)

// BenchmarkSequencerAdd feeds numbers shuffled inside blocks of 64, so each
// item waits for at most 63 others, through a window of 1024.
func BenchmarkSequencerAdd(b *testing.B) {
	const block = 64
	r := rand.New(rand.NewSource(1))
	perm := r.Perm(block)
	s := NewSequencer[int](0, 1024)
	var dst []Numbered[int]
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		seq := uint64(i/block*block + perm[i%block])
		dst = s.AddTo(dst[:0], seq, i)
	}
}
