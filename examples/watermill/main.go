// Command watermill shows event-order inside a Watermill router. Messages on
// the input topic arrive in any order; the handler holds them, and the output
// topic receives them in event-time order.
//
//	go run .
//
// The event time travels in the message metadata under the key event_time, in
// RFC 3339 format. The demo uses the in-memory gochannel pub/sub; replace it
// with Kafka, NATS, RabbitMQ or any other Watermill publisher and subscriber.
//
// Limit: Watermill acknowledges the input message when the handler returns, and
// delivers the next one only after that. A message the Reorderer is holding is
// therefore already acknowledged, and a crash loses it. Where that matters,
// consume with the broker's own client and use eventorder.Stream, which keeps
// the offset uncommitted until the event is processed (examples/kafka).
package main

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"time"

	eowm "github.com/A-RYAN-1/event-order/contrib/watermill"
	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/ThreeDotsLabs/watermill/pubsub/gochannel"
)

func main() {
	logger := watermill.NopLogger{}
	// Blocking publish keeps messages in publish order; by default gochannel
	// delivers each one from its own goroutine, which would reorder them.
	pubsub := gochannel.NewGoChannel(gochannel.Config{BlockPublishUntilSubscriberAck: true}, logger)
	router, err := message.NewRouter(message.RouterConfig{}, logger)
	if err != nil {
		log.Fatal(err)
	}
	reorderer := eowm.NewReorderer(5*time.Second, eowm.WithLogger(logger))
	router.AddHandler("reorder", "readings", pubsub, "readings.ordered", pubsub, reorderer.Handle)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	out, err := pubsub.Subscribe(ctx, "readings.ordered")
	if err != nil {
		log.Fatal(err)
	}
	go func() {
		if err := router.Run(ctx); err != nil {
			log.Print(err)
		}
	}()
	<-router.Running()
	go reorderer.Drain(ctx, pubsub, "readings.ordered", time.Second)

	base := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	r := rand.New(rand.NewSource(1))
	go func() {
		for blk := 0; blk < 12; blk += 4 {
			for _, i := range r.Perm(4) {
				m := message.NewMessage(watermill.NewUUID(), []byte(fmt.Sprintf("reading %d", blk+i)))
				m.Metadata.Set(eowm.EventTimeKey, base.Add(time.Duration(blk+i)*time.Second).Format(time.RFC3339Nano))
				if err := pubsub.Publish("readings", m); err != nil {
					log.Print(err)
				}
			}
		}
	}()

	for n := 0; n < 12; n++ {
		m := <-out
		fmt.Printf("%s  %s\n", m.Metadata.Get(eowm.EventTimeKey), m.Payload)
		m.Ack()
	}
}
