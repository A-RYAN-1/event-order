package main

import (
	"context"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/ThreeDotsLabs/watermill/pubsub/gochannel"
)

var t0 = time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)

func newMsg(sec int) *message.Message {
	m := message.NewMessage(watermill.NewUUID(), []byte(fmt.Sprint(sec)))
	m.Metadata.Set(EventTimeKey, t0.Add(time.Duration(sec)*time.Second).Format(time.RFC3339Nano))
	return m
}

// start runs a router with the Reorderer between "in" and "out" and returns the
// pub/sub, the Reorderer and a channel of ordered output.
func start(t *testing.T, delay time.Duration) (*gochannel.GoChannel, *Reorderer, <-chan *message.Message, context.Context) {
	t.Helper()
	logger := watermill.NopLogger{}
	ps := gochannel.NewGoChannel(gochannel.Config{BlockPublishUntilSubscriberAck: true}, logger)
	router, err := message.NewRouter(message.RouterConfig{}, logger)
	if err != nil {
		t.Fatal(err)
	}
	re := NewReorderer(delay, logger)
	router.AddHandler("reorder", "in", ps, "out", ps, re.Handle)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	out, err := ps.Subscribe(ctx, "out")
	if err != nil {
		t.Fatal(err)
	}
	go func() { _ = router.Run(ctx) }()
	<-router.Running()
	return ps, re, out, ctx
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

func TestRouterOutputIsInEventTimeOrder(t *testing.T) {
	ps, re, out, ctx := start(t, 5*time.Second)
	go re.Drain(ctx, ps, "out", time.Second)

	r := rand.New(rand.NewSource(7))
	const n = 40
	go func() {
		for blk := 0; blk < n; blk += 4 { // at most 3 s out of order
			for _, i := range r.Perm(4) {
				if err := ps.Publish("in", newMsg(blk+i)); err != nil {
					t.Error(err)
				}
			}
		}
	}()
	got := collect(t, out, n)
	for i, v := range got {
		if v != fmt.Sprint(i) {
			t.Fatalf("position %d holds %s: %v", i, v, got)
		}
	}
}

func TestDrainReleasesTheTailOfAQuietTopic(t *testing.T) {
	ps, re, out, ctx := start(t, time.Hour)
	go re.Drain(ctx, ps, "out", time.Second)
	for _, s := range []int{3, 1, 2} {
		if err := ps.Publish("in", newMsg(s)); err != nil {
			t.Fatal(err)
		}
	}
	// With a one hour delay nothing is released by arrival; only the idle drain can.
	if got := collect(t, out, 3); fmt.Sprint(got) != "[1 2 3]" {
		t.Fatalf("got %v", got)
	}
}

func TestMessageWithoutEventTimeIsDroppedNotRetried(t *testing.T) {
	ps, re, out, ctx := start(t, 0)
	go re.Drain(ctx, ps, "out", time.Second)
	bad := message.NewMessage(watermill.NewUUID(), []byte("bad"))
	if err := ps.Publish("in", bad); err != nil { // acknowledged: the router did not nack it
		t.Fatal(err)
	}
	if err := ps.Publish("in", newMsg(1)); err != nil {
		t.Fatal(err)
	}
	if got := collect(t, out, 1); fmt.Sprint(got) != "[1]" {
		t.Fatalf("got %v", got)
	}
}
