package eventorder

import (
	"sync"
	"time"
)

// Dedup drops repeated events from an at-least-once source. Brokers and
// retrying producers deliver the same event twice, often out of order, so
// a duplicate can arrive well after the original. Dedup remembers event ids
// for a window of event time and answers whether an id was already seen.
//
// The window is measured against the newest event time seen, the same clock a
// Buffer uses: an id is forgotten once it is more than window behind. Set the
// window at least as large as the Buffer's delay plus how late a retry can
// arrive. A duplicate that arrives after its id was forgotten is not caught,
// and the set lives in memory only, so after a restart earlier ids are gone:
// keep the processing itself idempotent where a duplicate costs money.
//
// Dedup is safe for concurrent use.
type Dedup[K comparable] struct {
	mu     sync.Mutex
	window time.Duration
	maxIDs int
	seen   map[K]time.Time
	h      []dedupItem[K] // min-heap by event time
	newest time.Time
	seq    uint64
	dups   uint64
}

type dedupItem[K comparable] struct {
	t   time.Time
	id  K
	seq uint64 // insertion order, breaks ties so eviction is deterministic
}

func (a dedupItem[K]) before(b dedupItem[K]) bool {
	if a.t.Equal(b.t) {
		return a.seq < b.seq
	}
	return a.t.Before(b.t)
}

// NewDedup returns a Dedup that remembers ids for window of event time. When
// maxIDs is above zero and more ids are held, the oldest are forgotten first,
// which bounds memory at the price of missing older duplicates.
func NewDedup[K comparable](window time.Duration, maxIDs int) *Dedup[K] {
	if window < 0 {
		window = 0
	}
	return &Dedup[K]{window: window, maxIDs: maxIDs, seen: make(map[K]time.Time)}
}

// Seen reports whether id, an event at time t, is a duplicate of an event
// remembered earlier. A first sight returns false and is remembered. An event
// already older than the window is not remembered and returns false: it is
// too old to tell.
func (d *Dedup[K]) Seen(t time.Time, id K) bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	if t.After(d.newest) {
		d.newest = t
	}
	d.expire()
	if _, dup := d.seen[id]; dup {
		d.dups++
		return true
	}
	if d.newest.Sub(t) > d.window {
		return false
	}
	d.seen[id] = t
	d.seq++
	d.push(dedupItem[K]{t: t, id: id, seq: d.seq})
	for d.maxIDs > 0 && len(d.h) > d.maxIDs {
		d.evict()
	}
	return false
}

// Len is the number of ids currently remembered.
func (d *Dedup[K]) Len() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return len(d.h)
}

// Duplicates is the number of duplicates found so far.
func (d *Dedup[K]) Duplicates() uint64 {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.dups
}

func (d *Dedup[K]) expire() {
	for len(d.h) > 0 && d.newest.Sub(d.h[0].t) > d.window {
		d.evict()
	}
}

func (d *Dedup[K]) evict() {
	it := d.pop()
	delete(d.seen, it.id)
}

func (d *Dedup[K]) push(it dedupItem[K]) {
	d.h = append(d.h, it)
	for i := len(d.h) - 1; i > 0; {
		p := (i - 1) / 2
		if !d.h[i].before(d.h[p]) {
			break
		}
		d.h[i], d.h[p] = d.h[p], d.h[i]
		i = p
	}
}

func (d *Dedup[K]) pop() dedupItem[K] {
	top := d.h[0]
	n := len(d.h) - 1
	d.h[0] = d.h[n]
	d.h[n] = dedupItem[K]{}
	d.h = d.h[:n]
	for i := 0; ; {
		l, small := 2*i+1, i
		if l < n && d.h[l].before(d.h[small]) {
			small = l
		}
		if r := l + 1; r < n && d.h[r].before(d.h[small]) {
			small = r
		}
		if small == i {
			break
		}
		d.h[i], d.h[small] = d.h[small], d.h[i]
		i = small
	}
	return top
}
