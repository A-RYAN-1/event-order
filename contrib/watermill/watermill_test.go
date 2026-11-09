package watermill

import (
	"context"
	"fmt"
	"math/rand"
	"testing"
	"time"

	eventorder "github.com/A-RYAN-1/event-order"
	wm "github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/ThreeDotsLabs/watermill/pubsub/gochannel"
)

var t0 = time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)

func newMsg(sec int) *message.Message {
	m := message.NewMessage(wm.NewUUID(), []byte(fmt.Sprint(sec)))
	m.Metadata.Set(EventTimeKey, t0.Add(time.Duration(sec)*time.Second).Format(time.RFC3339Nano))
	return m
}

// start runs a router with re between "in" and "out" and returns the pub/sub,
// a channel of output and a context that ends with the test.
func start(t *testing.T, re *Reorderer) (*gochannel.GoChannel, <-chan *message.Message, context.Context) {
	t.Helper()
	logger := wm.NopLogger{}
	// Blocking publish keeps messages in publish order; by default gochannel
	// delivers each one from its own goroutine.
	ps := gochannel.NewGoChannel(gochannel.Config{BlockPublishUntilSubscriberAck: true}, logger)
	router, err := message.NewRouter(message.RouterConfig{}, logger)
	if err != nil {
		t.Fatal(err)
	}
	router.AddHandler("reorder", "in", ps, "out", ps, re.Handle)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	out, err := ps.Subscribe(ctx, "out")
	if err != nil {
		t.Fatal(err)
	}
	go func() { _ = router.Run(ctx) }()
	<-router.Running()
	return ps, out, ctx
}

func collect(t *testing.T, out <-chan *message.Message, n int) []string {
	t.Helper()
	var got []string
	for len(got) < n {
		select {
		case m := <-out:
			got = append(got, string(m.Payload))
			m.Ack()
		case <-time.After(5 * time.Second):
			t.Fatalf("got %d of %d messages: %v", len(got), n, got)
		}
	}
	return got
}

func publish(t *testing.T, ps *gochannel.GoChannel, secs ...int) {
	t.Helper()
	for _, s := range secs {
		if err := ps.Publish("in", newMsg(s)); err != nil {
			t.Fatal(err)
		}
	}
}

func TestRouterOutputIsInEventTimeOrder(t *testing.T) {
	re := NewReorderer(5 * time.Second)
	ps, out, ctx := start(t, re)
	go re.Drain(ctx, ps, "out", time.Second)

	r := rand.New(rand.NewSource(7))
	const n = 40
	go func() {
		for blk := 0; blk < n; blk += 4 { // at most 3 s out of order
			publish(t, ps, shift(r.Perm(4), blk)...)
		}
	}()
	got := collect(t, out, n)
	for i, v := range got {
		if v != fmt.Sprint(i) {
			t.Fatalf("position %d holds %s: %v", i, v, got)
		}
	}
}

func shift(p []int, by int) []int {
	for i := range p {
		p[i] += by
	}
	return p
}

func TestDrainReleasesTheTailOfAQuietTopic(t *testing.T) {
	re := NewReorderer(time.Hour)
	ps, out, ctx := start(t, re)
	go re.Drain(ctx, ps, "out", time.Second)
	publish(t, ps, 3, 1, 2)
	// With a one hour delay nothing is released by arrival; only the idle drain can.
	if got := collect(t, out, 3); fmt.Sprint(got) != "[1 2 3]" {
		t.Fatalf("got %v", got)
	}
}

func TestMessageWithoutEventTimeIsDroppedNotRetried(t *testing.T) {
	re := NewReorderer(0)
	ps, out, ctx := start(t, re)
	go re.Drain(ctx, ps, "out", time.Second)
	bad := message.NewMessage(wm.NewUUID(), []byte("bad"))
	if err := ps.Publish("in", bad); err != nil { // acknowledged: the router did not nack it
		t.Fatal(err)
	}
	publish(t, ps, 1)
	if got := collect(t, out, 1); fmt.Sprint(got) != "[1]" {
		t.Fatalf("got %v", got)
	}
}

func TestLateMessageIsDroppedAndCounted(t *testing.T) {
	re := NewReorderer(time.Second)
	ps, out, ctx := start(t, re)
	go re.Drain(ctx, ps, "out", time.Second)
	publish(t, ps, 10, 1, 11) // 1 is nine seconds behind 10
	if got := collect(t, out, 2); fmt.Sprint(got) != "[10 11]" {
		t.Fatalf("got %v", got)
	}
	if st := re.Stats(); st.Late != 1 {
		t.Fatalf("Late = %d, want 1", st.Late)
	}
}

func TestEmitPolicyPassesLateMessageThrough(t *testing.T) {
	re := NewReorderer(time.Second, WithBuffer(eventorder.WithLatePolicy(eventorder.Emit)))
	ps, out, ctx := start(t, re)
	go re.Drain(ctx, ps, "out", time.Second)
	// Under Emit the late message is published at once, so the output must be
	// read while the input is still being published.
	go func() {
		for _, s := range []int{10, 1, 11} {
			if err := ps.Publish("in", newMsg(s)); err != nil {
				t.Error(err)
			}
		}
	}()
	got := collect(t, out, 3)
	seen := map[string]bool{}
	for _, v := range got {
		seen[v] = true
	}
	if !seen["1"] || !seen["10"] || !seen["11"] {
		t.Fatalf("a message is missing: %v", got)
	}
}

func TestShutdownFlushesWhatIsHeld(t *testing.T) {
	re := NewReorderer(time.Hour)
	ps, out, ctx := start(t, re)
	drainCtx, stop := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() { re.Drain(drainCtx, ps, "out", time.Hour); close(done) }()
	publish(t, ps, 2, 1)
	stop() // the idle drain would need an hour; the shutdown flush does not
	// The flush publishes into a blocking pub/sub, so read before waiting.
	if got := collect(t, out, 2); fmt.Sprint(got) != "[1 2]" {
		t.Fatalf("got %v", got)
	}
	<-done
}

func TestCustomTimeKey(t *testing.T) {
	re := NewReorderer(time.Second, WithTimeKey("ts"))
	at := func(sec int, payload string) *message.Message {
		m := message.NewMessage(wm.NewUUID(), []byte(payload))
		m.Metadata.Set("ts", t0.Add(time.Duration(sec)*time.Second).Format(time.RFC3339Nano))
		return m
	}
	if out, err := re.Handle(at(0, "a")); err != nil || len(out) != 0 {
		t.Fatalf("first Handle = %v, %v", out, err)
	}
	out, err := re.Handle(at(5, "b")) // five seconds on: "a" is past the delay
	if err != nil || len(out) != 1 || string(out[0].Payload) != "a" {
		t.Fatalf("second Handle = %v, %v", out, err)
	}
}
