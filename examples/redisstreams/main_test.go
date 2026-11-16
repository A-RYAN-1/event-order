package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"sort"
	"testing"
	"time"
)

var t0 = time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)

var errCrash = errors.New("crash")

// fakeSource serves a script of entries in batches, then either reports a
// cancelled context (clean stop) or fails with errCrash. It remembers every ID
// acknowledged, as a consumer group's pending list would.
type fakeSource struct {
	script []entry
	next   int
	batch  int
	crash  bool
	acked  map[string]bool
	acks   int
}

func (f *fakeSource) Read(context.Context) ([]entry, error) {
	if f.next >= len(f.script) {
		if f.crash {
			return nil, errCrash
		}
		return nil, context.Canceled
	}
	end := min(f.next+max(f.batch, 1), len(f.script))
	out := f.script[f.next:end]
	f.next = end
	return out, nil
}

func (f *fakeSource) Ack(_ context.Context, ids ...string) error {
	if f.acked == nil {
		f.acked = map[string]bool{}
	}
	for _, id := range ids {
		f.acked[id] = true
	}
	f.acks++
	return nil
}

func ent(seq int, id string, sec int) entry {
	v, _ := json.Marshal(Reading{ID: id, Time: t0.Add(time.Duration(sec) * time.Second), Value: float64(sec)})
	return entry{ID: fmt.Sprintf("1700000000000-%d", seq), Data: string(v)}
}

func TestConsumeOrdersDedupsAndAcknowledgesEverything(t *testing.T) {
	fs := &fakeSource{batch: 2, script: []entry{
		ent(0, "c", 3), ent(1, "a", 1), ent(2, "b", 2), ent(3, "a", 1), // "a" delivered twice
		{ID: "1700000000000-4", Data: "not json"}, // unreadable: skipped, still acknowledged
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
	if len(fs.acked) != len(fs.script) {
		t.Fatalf("acknowledged %d of %d entries: %v", len(fs.acked), len(fs.script), fs.acked)
	}
}

func TestUnreadableEntryDoesNotBlockTheAck(t *testing.T) {
	fs := &fakeSource{script: []entry{{ID: "1-0", Data: "{"}, ent(1, "a", 1)}}
	if err := consume(context.Background(), fs, config{maxDelay: time.Second, window: time.Minute}, func(Reading) {}); err != nil {
		t.Fatal(err)
	}
	if !fs.acked["1-0"] || len(fs.acked) != 2 {
		t.Fatalf("acknowledged %v, want both entries", fs.acked)
	}
}

// TestCrashNeverLosesAReading: readings shuffled inside blocks of four seconds,
// the process dies at a random point without flushing. A second run reads what
// the group still holds as pending (everything not acknowledged) and must see
// every reading the first run had not handled.
func TestCrashNeverLosesAReading(t *testing.T) {
	for seed := int64(0); seed < 200; seed++ {
		r := rand.New(rand.NewSource(seed))
		const n = 120
		var script []entry
		for blk := 0; blk < n; blk += 4 {
			for _, i := range r.Perm(4) {
				script = append(script, ent(len(script), fmt.Sprintf("r-%d", blk+i), blk+i))
			}
		}
		cut := 1 + r.Intn(len(script)-1)
		cfg := config{maxDelay: 5 * time.Second, window: time.Hour}

		seen := map[string]int{}
		first := &fakeSource{script: script[:cut], batch: 1 + r.Intn(5), crash: true}
		if err := consume(context.Background(), first, cfg, func(rd Reading) { seen[rd.ID]++ }); !errors.Is(err, errCrash) {
			t.Fatalf("seed %d: first run ended with %v", seed, err)
		}
		var pending []entry // what XREADGROUP with ID 0 returns, then the rest of the stream
		for _, e := range script {
			if !first.acked[e.ID] {
				pending = append(pending, e)
			}
		}
		second := &fakeSource{script: pending, batch: 3}
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

// idleSource serves its entries, then returns an empty read once (the stream
// went quiet), calls resumed when the consumer reads again, and then ends.
type idleSource struct {
	fakeSource
	quiet   bool
	resumed func()
}

func (f *idleSource) Read(ctx context.Context) ([]entry, error) {
	if f.next >= len(f.script) {
		if !f.quiet {
			f.quiet = true
			return nil, nil
		}
		f.resumed()
	}
	return f.fakeSource.Read(ctx)
}

func TestIdleReleasesTheTailBeforeTheStop(t *testing.T) {
	var got []string
	var atResume, acksAtResume int
	fs := &idleSource{fakeSource: fakeSource{script: []entry{ent(0, "b", 2), ent(1, "a", 1)}}}
	fs.resumed = func() { atResume, acksAtResume = len(got), fs.acks }
	cfg := config{maxDelay: time.Hour, window: time.Hour, idle: true}
	if err := consume(context.Background(), fs, cfg, func(rd Reading) { got = append(got, rd.ID) }); err != nil {
		t.Fatal(err)
	}
	if atResume != 2 || acksAtResume == 0 || fmt.Sprint(got) != "[a b]" {
		t.Fatalf("handled %d and acknowledged %d times when the consumer resumed after idle; got %v", atResume, acksAtResume, got)
	}
}

func TestWithoutIdleTheTailWaitsForTheStop(t *testing.T) {
	var atResume int
	var got []string
	fs := &idleSource{fakeSource: fakeSource{script: []entry{ent(0, "b", 2), ent(1, "a", 1)}}}
	fs.resumed = func() { atResume = len(got) }
	cfg := config{maxDelay: time.Hour, window: time.Hour}
	if err := consume(context.Background(), fs, cfg, func(rd Reading) { got = append(got, rd.ID) }); err != nil {
		t.Fatal(err)
	}
	if atResume != 0 || fmt.Sprint(got) != "[a b]" {
		t.Fatalf("handled %d before the stop, got %v; want 0 and [a b]", atResume, got)
	}
}
