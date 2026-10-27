package eventorder

import (
	"sync"
	"time"
)

// Record is an event as Stream hands it back: the event time, the broker
// offset it came from and the value.
type Record[T any] struct {
	Time   time.Time
	Offset int64
	Value  T
}

// Stream wires the pieces a broker consumer needs into one place, in the order
// that is correct: duplicates are dropped first, the rest are held per key in a
// Keyed buffer, and a KeyedTracker follows which offsets are finished so that
// Commit never passes an event that is still held.
//
// The flow for each message is Ingest, process the Records it returns, Done for
// each of them, and commit what Commit returns. A message that is a duplicate,
// late (older than the key's watermark) or has an offset the key already saw is
// finished by Stream itself; it never comes back as a Record.
//
// Key is usually the partition. On a rebalance call Revoke for each lost
// partition, and on shutdown call Flush. Stream is safe for concurrent use, so
// worker goroutines may call Done and Commit.
type Stream[K comparable, T any] struct {
	mu      sync.Mutex
	buf     *Keyed[K, entry[T]]
	track   *KeyedTracker[K]
	dedup   *Dedup[string]
	onLate  func(K, Record[T])
	dups    uint64
	late    uint64
	skipped uint64
}

type entry[T any] struct {
	offset int64
	value  T
}

// StreamOption configures a Stream.
type StreamOption func(*streamConfig)

type streamConfig struct {
	buffer []Option
	window time.Duration
	maxIDs int
	dedup  bool
}

// WithBuffer sets the options of every key's Buffer: WithMaxDelay,
// WithMaxBuffered, WithDelayStrategy. WithLatePolicy is ignored, because Stream
// always drops late events and reports them to WithOnLate.
func WithBuffer(opts ...Option) StreamOption {
	return func(c *streamConfig) { c.buffer = append(c.buffer, opts...) }
}

// WithDedup drops repeated ids, see NewDedup. Without it, ids are not looked at.
func WithDedup(window time.Duration, maxIDs int) StreamOption {
	return func(c *streamConfig) { c.window, c.maxIDs, c.dedup = window, maxIDs, true }
}

// StreamStats are the counters of a Stream.
type StreamStats struct {
	// Duplicates are messages dropped because their id was seen in the window.
	Duplicates uint64
	// Late are messages dropped because they arrived at or before the
	// watermark of their key.
	Late uint64
	// Skipped are messages whose offset was not above the last one seen for
	// their key, for example a replay after a rebalance that was not Revoked.
	Skipped uint64
	// Keys is the number of keys currently held.
	Keys int
}

// NewStream returns a Stream. The late hook, if any, is set with OnLate.
func NewStream[K comparable, T any](opts ...StreamOption) *Stream[K, T] {
	var c streamConfig
	for _, o := range opts {
		o(&c)
	}
	// Late events must reach the buffer's hook, which is only called for the
	// policies that look at them; Drop is the default and calls it.
	s := &Stream[K, T]{
		buf:   NewKeyed[K, entry[T]](append(c.buffer, WithLatePolicy(Drop))...),
		track: NewKeyedTracker[K](),
	}
	if c.dedup {
		s.dedup = NewDedup[string](c.window, c.maxIDs)
	}
	s.buf.OnLate(func(key K, e Event[entry[T]]) {
		s.late++
		s.track.Done(key, e.Value.offset)
		if s.onLate != nil {
			s.onLate(key, Record[T]{Time: e.Time, Offset: e.Value.offset, Value: e.Value.value})
		}
	})
	return s
}

// OnLate sets a function called for every late message, for a dead-letter queue
// or a metric. The offset is already finished when it runs. A nil fn clears it.
// The function must not call back into the Stream.
func (s *Stream[K, T]) OnLate(fn func(key K, r Record[T])) {
	s.mu.Lock()
	s.onLate = fn
	s.mu.Unlock()
}

// Ingest takes one message: key and offset say where it came from, t is its
// event time, id its unique id for deduplication (empty to skip) and v the
// value. It returns the records that became ready, in event-time order for the
// key. Call Done for each once processed.
func (s *Stream[K, T]) Ingest(key K, offset int64, t time.Time, id string, v T) []Record[T] {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.track.Track(key, offset) {
		s.skipped++
		return nil
	}
	if s.dedup != nil && id != "" && s.dedup.Seen(t, id) {
		s.dups++
		s.track.Done(key, offset)
		return nil
	}
	return s.records(s.buf.Add(key, t, entry[T]{offset: offset, value: v}))
}

// Skip records a message that is finished without being processed, for example
// a payload that cannot be decoded, so the commit position can move past it.
// It reports false for an offset not above the last one seen for the key.
func (s *Stream[K, T]) Skip(key K, offset int64) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.track.Track(key, offset) {
		s.skipped++
		return false
	}
	s.track.Done(key, offset)
	return true
}

// AdvanceAll releases what is due at time t in every key, see
// Keyed.AdvanceAll, calling fn once per key with its records. Call it
// periodically so quiet keys drain and are forgotten.
func (s *Stream[K, T]) AdvanceAll(t time.Time, fn func(key K, records []Record[T])) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.buf.AdvanceAll(t, func(key K, evs []Event[entry[T]]) { fn(key, s.records(evs)) })
}

// Done marks the message at offset as processed. It reports false for an offset
// that is not pending.
func (s *Stream[K, T]) Done(key K, offset int64) bool {
	return s.track.Done(key, offset)
}

// Commit is the offset to commit for key: everything before it is processed.
// ok is false for a key with no messages yet.
func (s *Stream[K, T]) Commit(key K) (offset int64, ok bool) {
	return s.track.Commit(key)
}

// Watermark is key's watermark, the time up to which its events are released,
// for feeding an Aligned. ok is false for a key that holds nothing.
func (s *Stream[K, T]) Watermark(key K) (time.Time, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	st, ok := s.buf.Stats(key)
	return st.Watermark, ok
}

// Revoke is for a partition about to be given up. It releases what key still
// holds, passes each record to fn (in order) and marks it done, forgets the
// key and returns the offset to commit. The key starts clean if it comes back.
func (s *Stream[K, T]) Revoke(key K, fn func(Record[T])) (offset int64, ok bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, r := range s.records(s.buf.Flush(key)) {
		fn(r)
		s.track.Done(key, r.Offset)
	}
	return s.track.Revoke(key)
}

// Flush is for a clean shutdown. It releases everything still held, passes each
// record to fn and marks it done; Commit then returns the final offsets.
func (s *Stream[K, T]) Flush(fn func(key K, r Record[T])) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.buf.FlushAll(func(key K, evs []Event[entry[T]]) {
		for _, r := range s.records(evs) {
			fn(key, r)
			s.track.Done(key, r.Offset)
		}
	})
}

// Stats returns the counters.
func (s *Stream[K, T]) Stats() StreamStats {
	s.mu.Lock()
	defer s.mu.Unlock()
	return StreamStats{Duplicates: s.dups, Late: s.late, Skipped: s.skipped, Keys: s.buf.Len()}
}

func (s *Stream[K, T]) records(evs []Event[entry[T]]) []Record[T] {
	if len(evs) == 0 {
		return nil
	}
	out := make([]Record[T], len(evs))
	for i, e := range evs {
		out[i] = Record[T]{Time: e.Time, Offset: e.Value.offset, Value: e.Value.value}
	}
	return out
}
