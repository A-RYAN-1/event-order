package eventorder

import (
	"sync"
	"time"
)

// Safe is a Buffer that can be used from several goroutines. Calls are
// serialised by a mutex; the OnLate function runs while it is held, so it must
// not call back into the same Safe.
type Safe[T any] struct {
	mu sync.Mutex
	b  *Buffer[T]
}

// NewSafe returns an empty Safe, configured like New.
func NewSafe[T any](opts ...Option) *Safe[T] { return &Safe[T]{b: New[T](opts...)} }

// Add is Buffer.Add under the lock.
func (s *Safe[T]) Add(t time.Time, v T) []Event[T] {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Add(t, v)
}

// AddTo is Buffer.AddTo under the lock.
func (s *Safe[T]) AddTo(dst []Event[T], t time.Time, v T) []Event[T] {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.AddTo(dst, t, v)
}

// AdvanceTo is Buffer.AdvanceTo under the lock, for a timer goroutine.
func (s *Safe[T]) AdvanceTo(t time.Time) []Event[T] {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.AdvanceTo(t)
}

// Flush is Buffer.Flush under the lock.
func (s *Safe[T]) Flush() []Event[T] {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Flush()
}

// OnLate is Buffer.OnLate under the lock.
func (s *Safe[T]) OnLate(fn func(Event[T])) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.b.OnLate(fn)
}

// Stats is Buffer.Stats under the lock.
func (s *Safe[T]) Stats() Stats {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Stats()
}

// SafeKeyed is a Keyed that can be used from several goroutines.
type SafeKeyed[K comparable, T any] struct {
	mu sync.Mutex
	k  *Keyed[K, T]
}

// NewSafeKeyed returns an empty SafeKeyed, configured like NewKeyed.
func NewSafeKeyed[K comparable, T any](opts ...Option) *SafeKeyed[K, T] {
	return &SafeKeyed[K, T]{k: NewKeyed[K, T](opts...)}
}

// Add is Keyed.Add under the lock.
func (s *SafeKeyed[K, T]) Add(key K, t time.Time, v T) []Event[T] {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.k.Add(key, t, v)
}

// AdvanceTo is Keyed.AdvanceTo under the lock.
func (s *SafeKeyed[K, T]) AdvanceTo(key K, t time.Time) []Event[T] {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.k.AdvanceTo(key, t)
}

// AdvanceAll is Keyed.AdvanceAll under the lock; fn runs while it is held.
func (s *SafeKeyed[K, T]) AdvanceAll(t time.Time, fn func(key K, events []Event[T])) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.k.AdvanceAll(t, fn)
}

// Flush is Keyed.Flush under the lock.
func (s *SafeKeyed[K, T]) Flush(key K) []Event[T] {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.k.Flush(key)
}

// FlushAll is Keyed.FlushAll under the lock; fn runs while it is held.
func (s *SafeKeyed[K, T]) FlushAll(fn func(key K, events []Event[T])) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.k.FlushAll(fn)
}

// OnLate is Keyed.OnLate under the lock.
func (s *SafeKeyed[K, T]) OnLate(fn func(key K, e Event[T])) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.k.OnLate(fn)
}

// Len is Keyed.Len under the lock.
func (s *SafeKeyed[K, T]) Len() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.k.Len()
}

// Stats is Keyed.Stats under the lock.
func (s *SafeKeyed[K, T]) Stats(key K) (Stats, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.k.Stats(key)
}
