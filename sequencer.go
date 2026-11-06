package eventorder

// Numbered is a value with the sequence number its producer gave it.
type Numbered[T any] struct {
	Seq   uint64
	Value T
}

// SeqStats is a snapshot of a Sequencer's counters.
type SeqStats struct {
	Held     int    // items waiting for a lower number
	Released uint64 // items released in order
	Stale    uint64 // items whose number was already released or given up on
	Dups     uint64 // items whose number was already held
	Gaps     uint64 // runs of numbers given up on
	Missing  uint64 // numbers given up on, summed over all gaps
	Peak     int    // most items held at once
	Next     uint64 // the number the Sequencer waits for
}

// Sequencer releases items in the order of a sequence number that the producer
// attaches (a per-key counter, a log index, a frame number). Event time trusts
// the producer clock and needs a delay to place an event; a sequence number is
// exact, so an item is released the moment every lower number has been, and
// the only wait is for a missing number.
//
// A missing number is waited for while the items held behind it span fewer
// than window numbers. An item that arrives window or more numbers ahead of
// the next expected one gives up on the oldest missing numbers until it fits:
// the items held below the new start are released in order, and each run of
// numbers given up on is reported once to OnGap and counted in Stats. Skip
// gives up on the current gap at once, for a timer when the producer has gone
// quiet. An item that arrives after its number was released or given up on is
// stale: it is dropped, counted and passed to OnStale.
//
// What it promises: released numbers strictly increase, every number is either
// released once or reported in exactly one gap, and no more than window-1
// items are held, so memory is bounded by window. What it does not do: it
// trusts the numbers (a producer that restarts its counter at zero looks like a
// stream of stale items; start a new Sequencer for it), it orders one stream
// (keep one per key), and it lives in memory only.
//
// A Sequencer is not safe for concurrent use.
type Sequencer[T any] struct {
	window   uint64
	slots    []seqSlot[T] // ring; slots[head] is number next
	head     int
	next     uint64
	held     int
	peak     int
	released uint64
	stale    uint64
	dups     uint64
	gaps     uint64
	missing  uint64
	onGap    func(from, to uint64)
	onStale  func(Numbered[T])
}

type seqSlot[T any] struct {
	v  T
	ok bool
}

// NewSequencer returns a Sequencer that expects first as its first number and
// waits for a missing number while the items behind it span fewer than window
// numbers. A window below 1 is taken as 1, which never waits: any gap is given
// up on as soon as a higher number arrives. The ring of slots grows as needed
// up to window, so a large window costs memory only when it is used.
func NewSequencer[T any](first uint64, window int) *Sequencer[T] {
	if window < 1 {
		window = 1
	}
	return &Sequencer[T]{window: uint64(window), next: first}
}

// OnGap sets a function called once for each run of numbers given up on, from
// inclusive to to exclusive. It runs inside Add, Skip and Flush; keep it fast.
// A nil fn clears it.
func (s *Sequencer[T]) OnGap(fn func(from, to uint64)) { s.onGap = fn }

// OnStale sets a function called for every stale item, so it can go to a
// dead-letter queue or a metric. It runs inside Add. A nil fn clears it.
func (s *Sequencer[T]) OnStale(fn func(Numbered[T])) { s.onStale = fn }

// Add inserts an item and returns the items now released, in sequence order.
// The slice is empty when the item waits behind a missing number, repeats a
// held number, or is stale.
func (s *Sequencer[T]) Add(seq uint64, v T) []Numbered[T] {
	return s.AddTo(nil, seq, v)
}

// AddTo is Add appending to dst, for a hot path that reuses one slice.
func (s *Sequencer[T]) AddTo(dst []Numbered[T], seq uint64, v T) []Numbered[T] {
	out := dst
	if seq < s.next {
		s.stale++
		if s.onStale != nil {
			s.onStale(Numbered[T]{Seq: seq, Value: v})
		}
		return out
	}
	if seq-s.next >= s.window {
		out = s.advance(out, seq-s.window+1)
	}
	off := seq - s.next
	s.grow(off + 1)
	i := s.index(off)
	if s.slots[i].ok {
		s.dups++
		return out
	}
	s.slots[i] = seqSlot[T]{v: v, ok: true}
	s.held++
	if s.held > s.peak {
		s.peak = s.held
	}
	return s.drain(out)
}

// Skip gives up on the numbers missing before the lowest held item and
// releases from there. With nothing held it does nothing: there is no gap yet,
// only numbers that have not arrived.
func (s *Sequencer[T]) Skip() []Numbered[T] {
	if s.held == 0 {
		return nil
	}
	var off uint64
	for !s.slots[s.index(off)].ok {
		off++
	}
	out := s.advance(nil, s.next+off)
	return s.drain(out)
}

// Flush releases every held item in order, reporting the gaps between them,
// for shutdown or end of input. Afterwards the Sequencer waits for one past
// the last released number.
func (s *Sequencer[T]) Flush() []Numbered[T] {
	var out []Numbered[T]
	for s.held > 0 {
		out = append(out, s.Skip()...)
	}
	return out
}

// Len is the number of items held.
func (s *Sequencer[T]) Len() int { return s.held }

// Next is the number the Sequencer waits for.
func (s *Sequencer[T]) Next() uint64 { return s.next }

// Stats returns the current counters.
func (s *Sequencer[T]) Stats() SeqStats {
	return SeqStats{
		Held: s.held, Released: s.released, Stale: s.stale, Dups: s.dups,
		Gaps: s.gaps, Missing: s.missing, Peak: s.peak, Next: s.next,
	}
}

func (s *Sequencer[T]) index(off uint64) int {
	return int((uint64(s.head) + off) % uint64(len(s.slots)))
}

// grow makes room for offsets below n (n <= window), doubling the ring and
// laying it out again from head.
func (s *Sequencer[T]) grow(n uint64) {
	if uint64(len(s.slots)) >= n {
		return
	}
	size := max(uint64(len(s.slots))*2, n, 8)
	size = min(size, s.window)
	slots := make([]seqSlot[T], size)
	for off := range uint64(len(s.slots)) {
		slots[off] = s.slots[s.index(off)]
	}
	s.slots, s.head = slots, 0
}

// drain releases the held items from next onward while they are contiguous.
func (s *Sequencer[T]) drain(out []Numbered[T]) []Numbered[T] {
	for s.held > 0 {
		sl := &s.slots[s.head]
		if !sl.ok {
			break
		}
		out = append(out, Numbered[T]{Seq: s.next, Value: sl.v})
		*sl = seqSlot[T]{}
		s.pop()
	}
	return out
}

// pop moves next and head one step after the slot at head has been emptied.
func (s *Sequencer[T]) pop() {
	s.held--
	s.released++
	s.next++
	s.head++
	if s.head == len(s.slots) {
		s.head = 0
	}
}

// advance moves next up to to, releasing the held items below it in order and
// reporting each run of missing numbers as a gap. Only the slots that exist are
// visited, so a jump of any size costs at most one pass over the ring.
func (s *Sequencer[T]) advance(out []Numbered[T], to uint64) []Numbered[T] {
	gapFrom, inGap := uint64(0), false
	for s.next < to && s.held > 0 {
		sl := &s.slots[s.head]
		if sl.ok {
			if inGap {
				s.gap(gapFrom, s.next)
				inGap = false
			}
			out = append(out, Numbered[T]{Seq: s.next, Value: sl.v})
			*sl = seqSlot[T]{}
			s.pop()
			continue
		}
		if !inGap {
			gapFrom, inGap = s.next, true
		}
		s.next++
		s.head++
		if s.head == len(s.slots) {
			s.head = 0
		}
	}
	if s.next < to {
		// Nothing is held any more, so the rest of the jump is one gap and
		// the ring is empty: no slot needs visiting.
		if !inGap {
			gapFrom, inGap = s.next, true
		}
		s.next, s.head = to, 0
	}
	if inGap {
		s.gap(gapFrom, s.next)
	}
	return out
}

func (s *Sequencer[T]) gap(from, to uint64) {
	s.gaps++
	s.missing += to - from
	if s.onGap != nil {
		s.onGap(from, to)
	}
}
