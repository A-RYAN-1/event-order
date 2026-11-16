package main

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"sort"
	"testing"
	"time"
)

var t0 = time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)

var errCrash = errors.New("crash")

// fakeMsg is a delivery of one message; acked is shared by every delivery of
// the same sequence, as the server's ack state would be.
type fakeMsg struct {
	seq    uint64
	header map[string]string
	data   string
	acked  map[uint64]bool
	acks   *int
}

func (m fakeMsg) Seq() uint64               { return m.seq }
func (m fakeMsg) Header(name string) string { return m.header[name] }
func (m fakeMsg) Data() []byte              { return []byte(m.data) }
func (m fakeMsg) Ack(context.Context) error {
	m.acked[m.seq] = true
	*m.acks++
	return nil
}

// fakeSource serves a script of messages in batches, then either reports a
// cancelled context (clean stop) or fails with errCrash.
type fakeSource struct {
	script []fakeMsg
	next   int
	batch  int
	crash  bool
}

func (f *fakeSource) Fetch(context.Context) ([]message, error) {
	if f.next >= len(f.script) {
		if f.crash {
			return nil, errCrash
		}
		return nil, context.Canceled
	}
	end := min(f.next+max(f.batch, 1), len(f.script))
	var out []message
	for _, m := range f.script[f.next:end] {
		out = append(out, m)
	}
	f.next = end
	return out, nil
}

// world is the server side: the acknowledged sequences and the ack count.
type world struct {
	acked map[uint64]bool
	acks  int
}

func newWorld() *world { return &world{acked: map[uint64]bool{}} }

func (w *world) msg(seq uint64, id string, sec int) fakeMsg {
	return fakeMsg{
		seq: seq, acked: w.acked, acks: &w.acks, data: fmt.Sprintf(`{"v":%d}`, sec),
		header: map[string]string{"Reading-Id": id, "Event-Time": t0.Add(time.Duration(sec) * time.Second).Format(time.RFC3339)},
	}
}

func TestConsumeOrdersDedupsAndAcknowledgesEverything(t *testing.T) {
	w := newWorld()
	bad := w.msg(5, "x", 9)
	bad.header["Event-Time"] = "yesterday" // unreadable: skipped, still acknowledged
	fs := &fakeSource{batch: 2, script: []fakeMsg{
		w.msg(1, "c", 3), w.msg(2, "a", 1), w.msg(3, "b", 2), w.msg(4, "a", 1), // "a" published twice
		bad,
	}}
	var got []string
	err := consume(context.Background(), fs, config{maxDelay: 5 * time.Second, window: time.Minute},
		func(rd Reading) { got = append(got, rd.ID) })
	if err != nil {
		t.Fatal(err)
	}
	if fmt.Sprint(got) != "[a b c]" {
		t.Fatalf("handled %v, want [a b c]", got)
	}
	if len(w.acked) != len(fs.script) {
		t.Fatalf("acknowledged %d of %d messages: %v", len(w.acked), len(fs.script), w.acked)
	}
}

// A stream with other subjects has gaps in its sequence numbers; the commit
// position still reaches the end.
func TestSequenceGapsDoNotBlockTheAck(t *testing.T) {
	w := newWorld()
	fs := &fakeSource{script: []fakeMsg{w.msg(10, "a", 1), w.msg(40, "b", 2), w.msg(41, "c", 3)}}
	if err := consume(context.Background(), fs, config{maxDelay: time.Second, window: time.Minute}, func(Reading) {}); err != nil {
		t.Fatal(err)
	}
	if len(w.acked) != 3 {
		t.Fatalf("acknowledged %v, want 10, 40 and 41", w.acked)
	}
}

// A message held longer than AckWait is delivered again with the same
// sequence. It must be handled once and acknowledged once.
func TestRedeliveryWhileHeldIsHandledOnce(t *testing.T) {
	w := newWorld()
	fs := &fakeSource{batch: 1, script: []fakeMsg{w.msg(1, "b", 2), w.msg(2, "c", 3), w.msg(1, "b", 2), w.msg(3, "a", 1)}}
	var got []string
	if err := consume(context.Background(), fs, config{maxDelay: time.Hour, window: time.Hour}, func(rd Reading) { got = append(got, rd.ID) }); err != nil {
		t.Fatal(err)
	}
	if fmt.Sprint(got) != "[a b c]" || len(w.acked) != 3 || w.acks != 3 {
		t.Fatalf("handled %v, acknowledged %v in %d acks; want [a b c] and 3 of each", got, w.acked, w.acks)
	}
}

// TestCrashNeverLosesAReading: readings shuffled inside blocks of four seconds,
// the process dies at a random point without flushing. The server redelivers
// every message not acknowledged; a second run must see every reading the
// first run had not handled.
func TestCrashNeverLosesAReading(t *testing.T) {
	for seed := int64(0); seed < 200; seed++ {
		r := rand.New(rand.NewSource(seed))
		const n = 120
		w := newWorld()
		var script []fakeMsg
		for blk := 0; blk < n; blk += 4 {
			for _, i := range r.Perm(4) {
				script = append(script, w.msg(uint64(len(script)+1), fmt.Sprintf("r-%d", blk+i), blk+i))
			}
		}
		cut := 1 + r.Intn(len(script)-1)
		cfg := config{maxDelay: 5 * time.Second, window: time.Hour}

		seen := map[string]int{}
		first := &fakeSource{script: script[:cut], batch: 1 + r.Intn(5), crash: true}
		if err := consume(context.Background(), first, cfg, func(rd Reading) { seen[rd.ID]++ }); !errors.Is(err, errCrash) {
			t.Fatalf("seed %d: first run ended with %v", seed, err)
		}
		var redelivered []fakeMsg
		for _, m := range script {
			if !w.acked[m.seq] {
				redelivered = append(redelivered, m)
			}
		}
		second := &fakeSource{script: redelivered, batch: 3}
		if err := consume(context.Background(), second, cfg, func(rd Reading) { seen[rd.ID]++ }); err != nil {
			t.Fatalf("seed %d: second run: %v", seed, err)
		}
		var missing []string
		for s := 0; s < n; s++ {
			if id := fmt.Sprintf("r-%d", s); seen[id] == 0 {
				missing = append(missing, id)
			}
		}
		sort.Strings(missing)
		if len(missing) > 0 {
			t.Fatalf("seed %d (cut %d): lost %v", seed, cut, missing)
		}
	}
}

// idleSource serves its messages, then returns an empty fetch once (the stream
// went quiet), calls resumed when the consumer fetches again, and then ends.
type idleSource struct {
	fakeSource
	quiet   bool
	resumed func()
}

func (f *idleSource) Fetch(ctx context.Context) ([]message, error) {
	if f.next >= len(f.script) {
		if !f.quiet {
			f.quiet = true
			return nil, nil
		}
		f.resumed()
	}
	return f.fakeSource.Fetch(ctx)
}

func TestIdleReleasesTheTailBeforeTheStop(t *testing.T) {
	w := newWorld()
	var got []string
	var atResume, acksAtResume int
	fs := &idleSource{fakeSource: fakeSource{script: []fakeMsg{w.msg(1, "b", 2), w.msg(2, "a", 1)}}}
	fs.resumed = func() { atResume, acksAtResume = len(got), w.acks }
	cfg := config{maxDelay: time.Hour, window: time.Hour, idle: true}
	if err := consume(context.Background(), fs, cfg, func(rd Reading) { got = append(got, rd.ID) }); err != nil {
		t.Fatal(err)
	}
	if atResume != 2 || acksAtResume == 0 || fmt.Sprint(got) != "[a b]" {
		t.Fatalf("handled %d and acknowledged %d times when the consumer resumed after idle; got %v", atResume, acksAtResume, got)
	}
}
