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

	"github.com/segmentio/kafka-go"
)

var t0 = time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)

var errCrash = errors.New("crash")

// fakeReader serves a script of messages, then either blocks until the context
// is cancelled (clean stop) or fails with errCrash.
type fakeReader struct {
	script  []kafka.Message
	next    int
	crash   bool
	commits []kafka.Message
}

func (f *fakeReader) FetchMessage(ctx context.Context) (kafka.Message, error) {
	if f.next < len(f.script) {
		m := f.script[f.next]
		f.next++
		return m, nil
	}
	if f.crash {
		return kafka.Message{}, errCrash
	}
	return kafka.Message{}, context.Canceled
}

func (f *fakeReader) CommitMessages(_ context.Context, msgs ...kafka.Message) error {
	f.commits = append(f.commits, msgs...)
	return nil
}

func msg(partition int, offset int64, id string, sec int) kafka.Message {
	v, _ := json.Marshal(Reading{ID: id, Time: t0.Add(time.Duration(sec) * time.Second), Value: float64(sec)})
	return kafka.Message{Topic: "readings", Partition: partition, Offset: offset, Value: v}
}

func TestConsumeOrdersDedupsAndCommitsEverything(t *testing.T) {
	fr := &fakeReader{script: []kafka.Message{
		msg(0, 0, "c", 3), msg(0, 1, "a", 1), msg(0, 2, "b", 2), msg(0, 3, "a", 1), // "a" delivered twice
		msg(1, 0, "x", 9),
	}}
	got := map[int][]string{}
	err := consume(context.Background(), fr, config{maxDelay: 5 * time.Second, window: time.Minute},
		func(p int, rd Reading) { got[p] = append(got[p], rd.ID) })
	if err != nil {
		t.Fatal(err)
	}
	if fmt.Sprint(got[0]) != "[a b c]" || fmt.Sprint(got[1]) != "[x]" {
		t.Fatalf("handled %v", got)
	}
	last := map[int]int64{}
	for _, c := range fr.commits {
		last[c.Partition] = c.Offset
	}
	if last[0] != 3 || last[1] != 0 { // kafka-go commits Offset+1: next to read is 4 and 1
		t.Fatalf("last committed message per partition %v, want {0:3 1:0}", last)
	}
}

// TestCrashNeverLosesAReading: shuffled readings on three partitions, the
// process dies at a random point without flushing. A second run starting from
// the committed offsets must see every reading the first run had not handled.
func TestCrashNeverLosesAReading(t *testing.T) {
	for seed := int64(0); seed < 200; seed++ {
		r := rand.New(rand.NewSource(seed))
		const perPart, parts = 60, 3
		var script []kafka.Message
		offsets := make([]int64, parts)
		secs := make([][]int, parts)
		for p := range secs { // shuffle inside blocks of 4: at most 3 s out of order
			secs[p] = make([]int, 0, perPart)
			for blk := 0; blk < perPart; blk += 4 {
				for _, i := range r.Perm(4) {
					secs[p] = append(secs[p], blk+i)
				}
			}
		}
		for i := 0; i < perPart; i++ {
			for p := 0; p < parts; p++ {
				script = append(script, msg(p, offsets[p], fmt.Sprintf("p%d-%d", p, secs[p][i]), secs[p][i]))
				offsets[p]++
			}
		}
		cut := 1 + r.Intn(len(script)-1)
		cfg := config{maxDelay: 5 * time.Second, window: time.Hour}

		seen := map[string]int{}
		first := &fakeReader{script: script[:cut], crash: true}
		if err := consume(context.Background(), first, cfg, func(_ int, rd Reading) { seen[rd.ID]++ }); !errors.Is(err, errCrash) {
			t.Fatalf("seed %d: first run ended with %v", seed, err)
		}
		next := map[int]int64{}
		for _, c := range first.commits {
			next[c.Partition] = c.Offset + 1
		}
		var replay []kafka.Message
		for _, m := range script { // what a broker redelivers: from the committed offset on
			if m.Offset >= next[m.Partition] {
				replay = append(replay, m)
			}
		}
		second := &fakeReader{script: replay}
		if err := consume(context.Background(), second, cfg, func(_ int, rd Reading) { seen[rd.ID]++ }); err != nil {
			t.Fatalf("seed %d: second run: %v", seed, err)
		}
		var missing []string
		for p := 0; p < parts; p++ {
			for s := 0; s < perPart; s++ {
				if id := fmt.Sprintf("p%d-%d", p, s); seen[id] == 0 {
					missing = append(missing, id)
				}
			}
		}
		sort.Strings(missing)
		if len(missing) > 0 {
			t.Fatalf("seed %d (cut %d): lost %v", seed, cut, missing)
		}
	}
}

// idleReader serves its messages, then reports a fetch timeout once (the topic
// went quiet), calls resumed when the consumer fetches again, and then ends.
type idleReader struct {
	fakeReader
	timedOut bool
	resumed  func()
}

func (f *idleReader) FetchMessage(ctx context.Context) (kafka.Message, error) {
	if f.next >= len(f.script) {
		if !f.timedOut {
			f.timedOut = true
			<-ctx.Done()
			return kafka.Message{}, ctx.Err()
		}
		f.resumed()
	}
	return f.fakeReader.FetchMessage(ctx)
}

func TestIdleReleasesTheTailBeforeTheStop(t *testing.T) {
	var got []string
	var atResume int
	var commitsAtResume int
	fr := &idleReader{fakeReader: fakeReader{script: []kafka.Message{msg(0, 0, "b", 2), msg(0, 1, "a", 1)}}}
	fr.resumed = func() { atResume, commitsAtResume = len(got), len(fr.commits) }
	cfg := config{maxDelay: time.Hour, window: time.Hour, idle: 20 * time.Millisecond}
	if err := consume(context.Background(), fr, cfg, func(_ int, rd Reading) { got = append(got, rd.ID) }); err != nil {
		t.Fatal(err)
	}
	if atResume != 2 || commitsAtResume == 0 || fmt.Sprint(got) != "[a b]" {
		t.Fatalf("handled %d and committed %d times when the consumer resumed after idle; got %v", atResume, commitsAtResume, got)
	}
}
