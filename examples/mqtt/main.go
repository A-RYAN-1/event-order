// Command mqtt subscribes to an MQTT topic and prints readings in event-time
// order. Broker clusters and QoS 0 do not promise one order across topics or
// nodes, so the subscriber puts the readings back in the order they happened.
//
//	go run . -broker tcp://localhost:1883 -topic 'sensors/#'
//
// Each payload is JSON: {"device":"d1","t":"2026-01-01T12:00:03Z","v":21.5}.
// The ordering lives in ordered, which takes a channel so the tests run
// without a broker.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"time"

	eventorder "github.com/A-RYAN-1/event-order"
	mqtt "github.com/eclipse/paho.mqtt.golang"
)

// Reading is the message payload.
type Reading struct {
	Device string    `json:"device"`
	Time   time.Time `json:"t"`
	Value  float64   `json:"v"`
}

// ordered decodes payloads from in and returns readings in event-time order.
// A quiet broker does not strand the last readings: after idle without a
// message everything held is released. Payloads that are not valid readings
// are skipped.
func ordered(ctx context.Context, in <-chan []byte, maxDelay, idle time.Duration) <-chan eventorder.Event[Reading] {
	events := make(chan eventorder.Event[Reading])
	go func() {
		defer close(events)
		for {
			select {
			case <-ctx.Done():
				return
			case payload, ok := <-in:
				if !ok {
					return
				}
				var rd Reading
				if json.Unmarshal(payload, &rd) != nil {
					continue
				}
				select {
				case events <- eventorder.Event[Reading]{Time: rd.Time, Value: rd}:
				case <-ctx.Done():
					return
				}
			}
		}
	}()
	return eventorder.Pipe(ctx, events,
		eventorder.WithMaxDelay(maxDelay),
		eventorder.WithIdleTimeout(idle))
}

func main() {
	broker := flag.String("broker", "tcp://localhost:1883", "MQTT broker URL")
	topic := flag.String("topic", "sensors/#", "topic filter")
	delay := flag.Duration("delay", 5*time.Second, "how far out of order readings may arrive")
	idle := flag.Duration("idle", 30*time.Second, "release held readings after this much silence")
	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	in := make(chan []byte, 256)
	opts := mqtt.NewClientOptions().AddBroker(*broker).SetClientID("event-order-demo")
	client := mqtt.NewClient(opts)
	if tok := client.Connect(); tok.Wait() && tok.Error() != nil {
		log.Fatal(tok.Error())
	}
	defer client.Disconnect(250)
	tok := client.Subscribe(*topic, 0, func(_ mqtt.Client, m mqtt.Message) {
		select {
		case in <- append([]byte(nil), m.Payload()...):
		case <-ctx.Done():
		}
	})
	if tok.Wait() && tok.Error() != nil {
		log.Fatal(tok.Error())
	}

	for ev := range ordered(ctx, in, *delay, *idle) {
		fmt.Printf("%s  %s  %.1f\n", ev.Time.Format(time.RFC3339), ev.Value.Device, ev.Value.Value)
	}
}
