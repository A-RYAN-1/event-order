package eventorder

import (
	"slices"
	"sort"
	"time"
)

// DelayStrategy chooses the delay of a Buffer from the disorder it has seen,
// instead of a fixed WithMaxDelay. Observe is called for every event added,
// late ones included, with how far behind the newest event time it arrived (zero
// for an event that is the newest so far). Delay is read after each Observe and
// is the watermark delay to use from now on. A strategy belongs to one Buffer
// and is never called from two goroutines at once.
type DelayStrategy interface {
	Observe(lag time.Duration)
	Delay() time.Duration
}

// WithDelayStrategy lets a DelayStrategy set the delay. newStrategy is called
// once per Buffer, so Keyed gives every key its own state. WithMaxDelay, when
// also given, is the floor: the delay used is the larger of the two. A nil
// newStrategy, or one that returns nil, leaves the fixed delay in place.
func WithDelayStrategy(newStrategy func() DelayStrategy) Option {
	return func(o *options) { o.strategy = newStrategy }
}

// PercentileDelay returns a strategy factory whose delay is the q-th quantile
// (0 to 1, for example 0.99) of the lags of the last window events. It adapts
// to a source whose disorder changes over time while ignoring rare outliers: at
// q = 0.99 about one event in a hundred may arrive later than the delay and be
// treated as late. Before any event is seen the delay is zero. Observe does no
// allocation once the window is filled; the window is clamped to at least 1 and
// q to [0, 1].
func PercentileDelay(q float64, window int) func() DelayStrategy {
	if window < 1 {
		window = 1
	}
	q = min(max(q, 0), 1)
	return func() DelayStrategy {
		return &percentile{q: q, ring: make([]time.Duration, 0, window), sorted: make([]time.Duration, 0, window)}
	}
}

type percentile struct {
	q      float64
	ring   []time.Duration // lags in arrival order, a circular buffer once full
	sorted []time.Duration // the same lags, ascending
	next   int             // slot in ring to overwrite once full
}

func (p *percentile) Observe(lag time.Duration) {
	if len(p.ring) == cap(p.ring) {
		old := p.ring[p.next]
		i := sort.Search(len(p.sorted), func(i int) bool { return p.sorted[i] >= old })
		p.sorted = slices.Delete(p.sorted, i, i+1)
		p.ring[p.next] = lag
		p.next = (p.next + 1) % len(p.ring)
	} else {
		p.ring = append(p.ring, lag)
	}
	i := sort.Search(len(p.sorted), func(i int) bool { return p.sorted[i] > lag })
	p.sorted = slices.Insert(p.sorted, i, lag)
}

func (p *percentile) Delay() time.Duration {
	n := len(p.sorted)
	if n == 0 {
		return 0
	}
	// Nearest-rank quantile: the smallest value with at least q of the sample at
	// or below it.
	rank := int(p.q*float64(n)+0.999999999) - 1
	return p.sorted[min(max(rank, 0), n-1)]
}
