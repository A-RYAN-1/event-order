package eventorder

import (
	"fmt"
	"math/rand"
	"slices"
	"testing"
	"time"
)

type logMsg struct {
	offset int64
	t      time.Time
	id     string
}

// makeLog builds one partition's log: events one second apart, shuffled inside
// blocks of block seconds (so a delay of block seconds loses nothing), with some
// events delivered a second time a few messages later.
func makeLog(r *rand.Rand, n, block int) []logMsg {
	var evs []logMsg
	for start := 0; start < n; start += block {
		end := min(start+block, n)
		for _, o := range r.Perm(end - start) {
			evs = append(evs, logMsg{t: at(start + o), id: fmt.Sprintf("e%d", start+o)})
		}
	}
	var out []logMsg
	for i, m := range evs {
		out = append(out, m)
		if r.Intn(6) == 0 && i > 0 {
			out = append(out, evs[i-r.Intn(min(i, 3))-1]) // a repeat of a recent event
		}
	}
	for i := range out {
		out[i].offset = int64(i)
	}
	return out
}

func TestStreamOrdersDedupsAndCommits(t *testing.T) {
	s := NewStream[int, string](WithBuffer(WithMaxDelay(5*time.Second)), WithDedup(time.Minute, 0))
	var got []string
	process := func(recs []Record[string]) {
		for _, r := range recs {
			got = append(got, r.Value)
			s.Done(0, r.Offset)
		}
	}
	process(s.Ingest(0, 0, at(3), "c", "c"))
	process(s.Ingest(0, 1, at(1), "a", "a"))
	if off, _ := s.Commit(0); off != 0 {
		t.Fatalf("commit %d while c and a are held, want 0", off)
	}
	process(s.Ingest(0, 2, at(1), "a", "a")) // repeat
	process(s.Ingest(0, 3, at(2), "b", "b"))
	process(s.Ingest(0, 4, at(9), "d", "d")) // moves the watermark to 3.999s
	if want := []string{"a", "b", "c"}; !slices.Equal(got, want) {
		t.Fatalf("got %v, want %v", got, want)
	}
	if off, _ := s.Commit(0); off != 4 {
		t.Fatalf("commit %d, want 4: d is still held", off)
	}
	s.Flush(func(_ int, r Record[string]) { got = append(got, r.Value) })
	if off, _ := s.Commit(0); off != 5 {
		t.Fatalf("commit %d after flush, want 5", off)
	}
	if st := s.Stats(); st.Duplicates != 1 || st.Late != 0 || st.Keys != 0 {
		t.Fatalf("stats %+v", st)
	}
}

func TestStreamLateIsFinishedAndReported(t *testing.T) {
	s := NewStream[int, string](WithBuffer(WithMaxDelay(time.Second)))
	var late []Record[string]
	s.OnLate(func(_ int, r Record[string]) { late = append(late, r) })
	s.Ingest(0, 0, at(10), "", "x")
	recs := s.Ingest(0, 1, at(2), "", "old")
	if len(recs) != 0 || len(late) != 1 || late[0].Value != "old" || late[0].Offset != 1 {
		t.Fatalf("recs %v late %v", recs, late)
	}
	// offset 1 is finished, offset 0 is still held: commit stops at 0
	if off, _ := s.Commit(0); off != 0 {
		t.Fatalf("commit %d, want 0", off)
	}
	if st := s.Stats(); st.Late != 1 {
		t.Fatalf("stats %+v", st)
	}
}

func TestStreamSkipsReplayedOffsets(t *testing.T) {
	s := NewStream[int, int](WithBuffer(WithMaxDelay(time.Hour)))
	s.Ingest(0, 5, at(1), "", 1)
	if recs := s.Ingest(0, 5, at(2), "", 2); len(recs) != 0 {
		t.Fatalf("replayed offset returned %v", recs)
	}
	var flushed []int
	s.Flush(func(_ int, r Record[int]) { flushed = append(flushed, r.Value) })
	if !slices.Equal(flushed, []int{1}) {
		t.Fatalf("replayed message was buffered: flushed %v", flushed)
	}
	if st := s.Stats(); st.Skipped != 1 {
		t.Fatalf("stats %+v", st)
	}
}

func TestStreamSkipMovesTheCommitPast(t *testing.T) {
	s := NewStream[int, int](WithBuffer(WithMaxDelay(time.Hour)))
	s.Ingest(0, 0, at(1), "", 1)
	if !s.Skip(0, 1) {
		t.Fatal("Skip refused a new offset")
	}
	if s.Skip(0, 1) {
		t.Fatal("Skip accepted a repeated offset")
	}
	if off, _ := s.Commit(0); off != 0 {
		t.Fatalf("commit %d, want 0: offset 0 is still held", off)
	}
	s.Flush(func(int, Record[int]) {})
	if off, _ := s.Commit(0); off != 2 {
		t.Fatalf("commit %d, want 2", off)
	}
}

func TestStreamRevokeGivesTheOffsetAndStartsClean(t *testing.T) {
	s := NewStream[string, int](WithBuffer(WithMaxDelay(time.Minute)))
	s.Ingest("p0", 10, at(1), "", 1)
	s.Ingest("p0", 11, at(2), "", 2)
	s.Ingest("p1", 3, at(1), "", 3)
	var got []int
	off, ok := s.Revoke("p0", func(r Record[int]) { got = append(got, r.Value) })
	if !ok || off != 12 || !slices.Equal(got, []int{1, 2}) {
		t.Fatalf("off %d ok %v got %v", off, ok, got)
	}
	if _, ok := s.Commit("p0"); ok {
		t.Fatal("revoked key still known")
	}
	// the partition comes back and is re-read from an earlier offset
	if recs := s.Ingest("p0", 10, at(1), "", 1); len(recs) != 0 {
		t.Fatalf("unexpected release %v", recs)
	}
	if off, _ := s.Commit("p0"); off != 10 {
		t.Fatalf("commit %d, want 10", off)
	}
	if off, _ := s.Commit("p1"); off != 3 {
		t.Fatalf("other partition moved: %d", off)
	}
}

// Crash at a random message, restart from the committed offset: every event of
// the log is processed at least once across both runs, each run's output is in
// event-time order, and the commit position never moves back.
func TestStreamCrashAndRestartLosesNothing(t *testing.T) {
	for seed := int64(0); seed < 1500; seed++ {
		r := rand.New(rand.NewSource(seed))
		block := 1 + r.Intn(8)
		log := makeLog(r, 60+r.Intn(60), block)
		opts := []StreamOption{WithBuffer(WithMaxDelay(time.Duration(block) * time.Second)), WithDedup(10*time.Minute, 0)}
		seen := map[string]bool{}

		run := func(from int64, crashAt int) (committed int64) {
			s := NewStream[int, string](opts...)
			var last time.Time
			var prev int64 = -1
			process := func(recs []Record[string]) {
				for _, rec := range recs {
					if rec.Time.Before(last) {
						t.Fatalf("seed %d: output out of order", seed)
					}
					last = rec.Time
					seen[rec.Value] = true
					s.Done(0, rec.Offset)
				}
			}
			for i := int(from); i < len(log); i++ {
				if i == crashAt {
					break
				}
				m := log[i]
				process(s.Ingest(0, m.offset, m.t, m.id, m.id))
				if c, ok := s.Commit(0); ok {
					if c < prev {
						t.Fatalf("seed %d: commit moved back %d to %d", seed, prev, c)
					}
					prev = c
				}
			}
			if crashAt >= 0 && crashAt < len(log) {
				c, _ := s.Commit(0)
				return c
			}
			s.Flush(func(_ int, rec Record[string]) {
				if rec.Time.Before(last) {
					t.Fatalf("seed %d: flushed out of order", seed)
				}
				last = rec.Time
				seen[rec.Value] = true
			})
			c, _ := s.Commit(0)
			return c
		}

		crash := r.Intn(len(log))
		c := run(0, crash)
		final := run(c, -1)
		if final != int64(len(log)) {
			t.Fatalf("seed %d: final commit %d, want %d", seed, final, len(log))
		}
		for _, m := range log {
			if !seen[m.id] {
				t.Fatalf("seed %d: event %s lost (crash at %d, restart from %d, block %d)", seed, m.id, crash, c, block)
			}
		}
	}
}

// Partitions through a Stream, their watermarks aligned, and one downstream
// buffer: the merged output is in global event-time order with no late events.
func TestStreamAlignedPartitionsGiveGlobalOrder(t *testing.T) {
	for seed := int64(0); seed < 300; seed++ {
		r := rand.New(rand.NewSource(seed))
		const parts = 3
		block := 1 + r.Intn(5)
		var logs [parts][]logMsg
		for p := range logs {
			logs[p] = makeLog(r, 40+r.Intn(40), block)
		}
		s := NewStream[int, string](WithBuffer(WithMaxDelay(time.Duration(block)*time.Second)), WithDedup(10*time.Minute, 0))
		ag := NewAligned[int]()
		down := New[string](WithMaxDelay(1000 * time.Hour))
		for p := 0; p < parts; p++ {
			ag.Register(p)
		}
		var out []Event[string]
		next := [parts]int{}
		for done := 0; done < parts; {
			p := r.Intn(parts)
			if next[p] >= len(logs[p]) {
				continue
			}
			m := logs[p][next[p]]
			next[p]++
			if next[p] == len(logs[p]) {
				done++
			}
			for _, rec := range s.Ingest(p, m.offset, m.t, fmt.Sprintf("%d/%s", p, m.id), m.id) {
				out = append(out, down.Add(rec.Time, fmt.Sprintf("%d/%s", p, rec.Value))...)
				s.Done(p, rec.Offset)
			}
			if wm, ok := s.Watermark(p); ok {
				if w, adv, _ := ag.Observe(p, wm); adv {
					out = append(out, down.AdvanceTo(w)...)
				}
			}
		}
		out = append(out, down.Flush()...)
		if down.Stats().Late != 0 {
			t.Fatalf("seed %d: %d late events downstream", seed, down.Stats().Late)
		}
		for i := 1; i < len(out); i++ {
			if out[i].Time.Before(out[i-1].Time) {
				t.Fatalf("seed %d: merged output out of order at %d", seed, i)
			}
		}
	}
}

// FuzzStream drives Ingest, AdvanceAll, Revoke and Flush with arbitrary bytes
// and checks the safety rule: Commit never passes an offset that was held and
// has not been marked done.
func FuzzStream(f *testing.F) {
	f.Add([]byte{0, 5, 1, 1, 2, 9, 0, 3, 3, 0, 4, 0}, uint8(3))
	f.Add([]byte{}, uint8(0))
	f.Fuzz(func(t *testing.T, data []byte, delay uint8) {
		s := NewStream[int, int](WithBuffer(WithMaxDelay(time.Duration(delay%10)*time.Second)), WithDedup(time.Minute, 8))
		pending := map[int]map[int64]bool{} // offsets handed to the buffer, not yet released
		next := map[int]int64{}
		check := func(why string) {
			for key, set := range pending {
				c, ok := s.Commit(key)
				if !ok {
					continue
				}
				for off := range set {
					if off < c {
						t.Fatalf("%s: key %d commit %d passes held offset %d", why, key, c, off)
					}
				}
			}
		}
		release := func(key int, recs []Record[int]) {
			for _, r := range recs {
				delete(pending[key], r.Offset)
				s.Done(key, r.Offset)
			}
		}
		for i := 0; i+2 < len(data); i += 3 {
			key := int(data[i+1] % 3)
			switch data[i] % 5 {
			case 0, 1:
				if pending[key] == nil {
					pending[key] = map[int64]bool{}
				}
				off := next[key]
				next[key]++
				before := s.Stats()
				recs := s.Ingest(key, off, at(int(data[i+2])), fmt.Sprint(data[i+2]%16), i)
				after := s.Stats()
				if after.Duplicates == before.Duplicates && after.Late == before.Late && after.Skipped == before.Skipped {
					pending[key][off] = true
				}
				release(key, recs)
			case 2:
				s.AdvanceAll(at(int(data[i+2])), func(k int, recs []Record[int]) { release(k, recs) })
			case 3:
				s.Revoke(key, func(r Record[int]) { delete(pending[key], r.Offset) })
				delete(pending, key)
				delete(next, key)
			case 4:
				s.Flush(func(k int, r Record[int]) { delete(pending[k], r.Offset) })
			}
			check(fmt.Sprintf("step %d", i/3))
		}
	})
}

func TestStreamConcurrentDone(t *testing.T) {
	s := NewStream[int, int](WithBuffer(WithMaxDelay(0)))
	done := make(chan Record[int], 64)
	finished := make(chan struct{})
	go func() {
		defer close(finished)
		for r := range done {
			s.Done(0, r.Offset)
			s.Commit(0)
		}
	}()
	for i := 0; i < 20000; i++ {
		for _, r := range s.Ingest(0, int64(i), at(i), "", i) {
			done <- r
		}
	}
	close(done)
	<-finished
	if off, _ := s.Commit(0); off < 19999 {
		t.Fatalf("commit %d", off)
	}
}
