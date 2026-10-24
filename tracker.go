package eventorder

import (
	"slices"
	"sync"
)

// Tracker answers one question for a consumer that holds events in a Buffer
// before processing them: which broker offset is safe to commit?
//
// A broker commit means "everything before this offset is done". Events held
// for reordering are not done, so committing the highest offset seen would
// lose them if the process crashes. Call Track(offset) when an event enters the
// buffer, Done(offset) when it has been processed (or dropped as late), and
// commit what Commit returns. Offsets may finish in any order; the commit
// position only moves past a gap when the gap is done.
//
// A Tracker is for one partition, whose offsets are tracked in increasing
// order. It is safe for concurrent use, so workers may call Done.
type Tracker struct {
	mu      sync.Mutex
	entries []trackEntry // offsets in increasing order; entries[:head] are done and gone
	head    int
	last    int64
	any     bool
}

type trackEntry struct {
	off  int64
	done bool
}

// NewTracker returns an empty Tracker.
func NewTracker() *Tracker { return &Tracker{} }

// Track records that the event at offset has entered the buffer. Offsets must
// increase; it reports false, and records nothing, for an offset not greater
// than the last one tracked.
func (t *Tracker) Track(offset int64) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.any && offset <= t.last {
		return false
	}
	t.entries = append(t.entries, trackEntry{off: offset})
	t.last, t.any = offset, true
	return true
}

// Done records that the event at offset has been processed or discarded. It
// reports false for an offset that is not tracked or is already done.
func (t *Tracker) Done(offset int64) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	live := t.entries[t.head:]
	i, found := slices.BinarySearchFunc(live, offset, func(e trackEntry, o int64) int {
		switch {
		case e.off < o:
			return -1
		case e.off > o:
			return 1
		}
		return 0
	})
	if !found || live[i].done {
		return false
	}
	live[i].done = true
	for t.head < len(t.entries) && t.entries[t.head].done {
		t.head++
	}
	if t.head > 64 && t.head > len(t.entries)/2 {
		t.entries = append(t.entries[:0], t.entries[t.head:]...)
		t.head = 0
	}
	return true
}

// Commit returns the offset to commit: the lowest tracked offset that is not
// done, or one past the last tracked offset when everything is done. ok is
// false until the first Track.
func (t *Tracker) Commit() (offset int64, ok bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if !t.any {
		return 0, false
	}
	if t.head < len(t.entries) {
		return t.entries[t.head].off, true
	}
	return t.last + 1, true
}

// Pending is the number of tracked offsets that are not done.
func (t *Tracker) Pending() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	n := 0
	for _, e := range t.entries[t.head:] {
		if !e.done {
			n++
		}
	}
	return n
}

// KeyedTracker keeps one Tracker per key, usually per partition. Pair it with
// Keyed: Keyed.Flush(partition) on a revoke releases that partition's held
// events, process them and call Done, then Revoke returns the offset to commit
// before the partition goes to another consumer. Safe for concurrent use.
type KeyedTracker[K comparable] struct {
	mu sync.Mutex
	m  map[K]*Tracker
}

// NewKeyedTracker returns an empty KeyedTracker.
func NewKeyedTracker[K comparable]() *KeyedTracker[K] {
	return &KeyedTracker[K]{m: make(map[K]*Tracker)}
}

func (k *KeyedTracker[K]) get(key K, create bool) *Tracker {
	k.mu.Lock()
	defer k.mu.Unlock()
	t, ok := k.m[key]
	if !ok && create {
		t = NewTracker()
		k.m[key] = t
	}
	return t
}

// Track is Tracker.Track for one key.
func (k *KeyedTracker[K]) Track(key K, offset int64) bool {
	return k.get(key, true).Track(offset)
}

// Done is Tracker.Done for one key; false for an unknown key.
func (k *KeyedTracker[K]) Done(key K, offset int64) bool {
	if t := k.get(key, false); t != nil {
		return t.Done(offset)
	}
	return false
}

// Commit is Tracker.Commit for one key; ok is false for an unknown key.
func (k *KeyedTracker[K]) Commit(key K) (offset int64, ok bool) {
	if t := k.get(key, false); t != nil {
		return t.Commit()
	}
	return 0, false
}

// Revoke forgets a key and returns the offset to commit for it, as Commit
// would have. Events still pending are not done, so the offset stops at the
// first of them and the next owner of the partition will read them again.
func (k *KeyedTracker[K]) Revoke(key K) (offset int64, ok bool) {
	k.mu.Lock()
	t := k.m[key]
	delete(k.m, key)
	k.mu.Unlock()
	if t == nil {
		return 0, false
	}
	return t.Commit()
}

// Len is the number of keys currently tracked.
func (k *KeyedTracker[K]) Len() int {
	k.mu.Lock()
	defer k.mu.Unlock()
	return len(k.m)
}
