package eventorder

import "time"

// Keyed keeps one Buffer per key, so events of different devices, users or
// partitions are reordered independently and one slow key does not hold back
// the others. Not safe for concurrent use.
type Keyed[K comparable, T any] struct {
	opts []Option
	bufs map[K]*Buffer[T]
	late func(K, Event[T])
}

// NewKeyed returns an empty Keyed; every key's Buffer is made with opts.
func NewKeyed[K comparable, T any](opts ...Option) *Keyed[K, T] {
	return &Keyed[K, T]{opts: opts, bufs: make(map[K]*Buffer[T])}
}

// Add inserts an event for key and returns that key's newly released events.
func (k *Keyed[K, T]) Add(key K, t time.Time, v T) []Event[T] {
	b, ok := k.bufs[key]
	if !ok {
		b = New[T](k.opts...)
		if k.late != nil {
			b.OnLate(func(e Event[T]) { k.late(key, e) })
		}
		k.bufs[key] = b
	}
	return b.Add(t, v)
}

// AdvanceTo moves one key's watermark, see Buffer.AdvanceTo. An unknown key
// releases nothing.
func (k *Keyed[K, T]) AdvanceTo(key K, t time.Time) []Event[T] {
	if b, ok := k.bufs[key]; ok {
		return b.AdvanceTo(t)
	}
	return nil
}

// OnLate sets a function called with the key for every late event, see
// Buffer.OnLate. It applies to existing and future keys. A nil fn clears it.
func (k *Keyed[K, T]) OnLate(fn func(key K, e Event[T])) {
	k.late = fn
	for key, b := range k.bufs {
		key := key
		if fn == nil {
			b.OnLate(nil)
			continue
		}
		b.OnLate(func(e Event[T]) { fn(key, e) })
	}
}

// AdvanceAll moves every key watermark up to t, see Buffer.AdvanceTo, calling
// fn once per key that released events. Keys left empty are forgotten, so a
// periodic call with the current time keeps the number of tracked keys
// bounded by the keys that were active recently. Key order is not defined.
func (k *Keyed[K, T]) AdvanceAll(t time.Time, fn func(key K, events []Event[T])) {
	for key, b := range k.bufs {
		if evs := b.AdvanceTo(t); len(evs) > 0 && fn != nil {
			fn(key, evs)
		}
		if b.Len() == 0 {
			delete(k.bufs, key)
		}
	}
}

// Flush releases and forgets one key. Call it when a key is finished, or the
// map of buffers grows with every key ever seen.
func (k *Keyed[K, T]) Flush(key K) []Event[T] {
	b, ok := k.bufs[key]
	if !ok {
		return nil
	}
	delete(k.bufs, key)
	return b.Flush()
}

// FlushAll releases every key, calling fn once per key with its events in
// order, and forgets them all. Key order is not defined.
func (k *Keyed[K, T]) FlushAll(fn func(key K, events []Event[T])) {
	for key, b := range k.bufs {
		if evs := b.Flush(); len(evs) > 0 {
			fn(key, evs)
		}
		delete(k.bufs, key)
	}
}

// Len is the number of keys currently tracked.
func (k *Keyed[K, T]) Len() int { return len(k.bufs) }

// Stats returns the counters of one key; ok is false for an unknown key.
func (k *Keyed[K, T]) Stats(key K) (s Stats, ok bool) {
	b, found := k.bufs[key]
	if !found {
		return Stats{}, false
	}
	return b.Stats(), true
}
