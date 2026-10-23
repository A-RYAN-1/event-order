package eventorder

import (
	"math/rand"
	"sort"
	"testing"
	"time"
)

// disordered returns n events one millisecond apart whose arrival order is
// shuffled inside a window of jitter events, the shape of a real feed.
func disordered(n, jitter int) []time.Time {
	r := rand.New(rand.NewSource(1))
	base := time.Unix(1_700_000_000, 0)
	ts := make([]time.Time, n)
	for i := range ts {
		ts[i] = base.Add(time.Duration(i) * time.Millisecond)
	}
	for i := 0; i < n; i++ {
		j := i + r.Intn(jitter)
		if j >= n {
			j = n - 1
		}
		ts[i], ts[j] = ts[j], ts[i]
	}
	return ts
}

// naive is what most services write first: append, sort the whole slice,
// release the prefix at or below the watermark.
type naive struct {
	delay time.Duration
	buf   []Event[int]
	max   time.Time
}

func (n *naive) add(t time.Time, v int) []Event[int] {
	n.buf = append(n.buf, Event[int]{Time: t, Value: v})
	if t.After(n.max) {
		n.max = t
	}
	sort.Slice(n.buf, func(i, j int) bool { return n.buf[i].Time.Before(n.buf[j].Time) })
	wm := n.max.Add(-n.delay)
	k := 0
	for k < len(n.buf) && !n.buf[k].Time.After(wm) {
		k++
	}
	out := append([]Event[int](nil), n.buf[:k]...)
	n.buf = n.buf[k:]
	return out
}

func BenchmarkBufferAdd(b *testing.B) {
	for _, jitter := range []int{16, 256} {
		ts := disordered(1<<16, jitter)
		b.Run("jitter="+itoa(jitter), func(b *testing.B) {
			b.ReportAllocs()
			buf := New[int](WithMaxDelay(time.Duration(jitter) * time.Millisecond))
			for i := 0; i < b.N; i++ {
				buf.Add(ts[i&(1<<16-1)].Add(time.Duration(i>>16)*time.Hour), i)
			}
		})
	}
}

func BenchmarkNaiveSortAdd(b *testing.B) {
	for _, jitter := range []int{16, 256} {
		ts := disordered(1<<16, jitter)
		b.Run("jitter="+itoa(jitter), func(b *testing.B) {
			b.ReportAllocs()
			n := &naive{delay: time.Duration(jitter) * time.Millisecond}
			for i := 0; i < b.N; i++ {
				n.add(ts[i&(1<<16-1)].Add(time.Duration(i>>16)*time.Hour), i)
			}
		})
	}
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var d []byte
	for ; n > 0; n /= 10 {
		d = append([]byte{byte('0' + n%10)}, d...)
	}
	return string(d)
}
