//go:build integration

package main

// Real-broker test. It needs an MQTT broker, for example
//
//	docker run -d --name mosquitto -p 1883:1883 eclipse-mosquitto:2 mosquitto -c /mosquitto-no-auth.conf
//	MQTT_BROKER=tcp://localhost:1883 go test -tags integration -count=1 -v .
//
// CI runs it against the same image.

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"sync"
	"testing"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)

func connect(t *testing.T, broker, id string) mqtt.Client {
	t.Helper()
	c := mqtt.NewClient(mqtt.NewClientOptions().AddBroker(broker).SetClientID(id))
	if tok := c.Connect(); !tok.WaitTimeout(10*time.Second) || tok.Error() != nil {
		t.Fatalf("connect %s: %v", id, tok.Error())
	}
	t.Cleanup(func() { c.Disconnect(100) })
	return c
}

// Readings of four devices are published by four separate clients on different
// topics, each client sending its own readings in a shuffled order. The broker
// gives the subscriber no single order across them; ordered() must.
func TestIntegrationOrdersReadingsFromSeveralPublishers(t *testing.T) {
	broker := os.Getenv("MQTT_BROKER")
	if broker == "" {
		t.Skip("MQTT_BROKER not set")
	}
	const devices, perDevice = 4, 40
	prefix := fmt.Sprintf("eo-it-%d", time.Now().UnixNano())

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	in := make(chan []byte, 1024)
	sub := connect(t, broker, prefix+"-sub")
	if tok := sub.Subscribe(prefix+"/#", 1, func(_ mqtt.Client, m mqtt.Message) {
		in <- append([]byte(nil), m.Payload()...)
	}); !tok.WaitTimeout(10*time.Second) || tok.Error() != nil {
		t.Fatal(tok.Error())
	}

	out := ordered(ctx, in, 10*time.Second, 2*time.Second)

	clients := make([]mqtt.Client, devices)
	for d := range clients {
		clients[d] = connect(t, broker, fmt.Sprintf("%s-pub%d", prefix, d))
	}
	// The watermark is shared by all devices, so the publishers advance in
	// lock step: every device sends its block, all acknowledged, before the
	// next block starts. Inside a block each device shuffles its own readings
	// (at most 3 s out of order). Without the barrier a fast publisher would
	// push the watermark past a slow one and its readings would be late.
	for blk := 0; blk < perDevice; blk += 4 {
		var wg sync.WaitGroup
		for d := 0; d < devices; d++ {
			wg.Add(1)
			go func(d int) {
				defer wg.Done()
				r := rand.New(rand.NewSource(int64(d*1000 + blk)))
				for _, i := range r.Perm(4) {
					sec := blk + i
					payload, _ := json.Marshal(Reading{Device: fmt.Sprintf("d%d", d), Time: t0.Add(time.Duration(sec) * time.Second), Value: float64(sec)})
					tok := clients[d].Publish(fmt.Sprintf("%s/d%d", prefix, d), 1, false, payload)
					if !tok.WaitTimeout(10*time.Second) || tok.Error() != nil {
						t.Errorf("publish: %v", tok.Error())
						return
					}
				}
			}(d)
		}
		wg.Wait()
	}

	var got []Reading
	var last time.Time
	for ev := range out {
		if ev.Time.Before(last) {
			t.Fatalf("reading at %v came after %v", ev.Time, last)
		}
		last = ev.Time
		got = append(got, ev.Value)
		if len(got) == devices*perDevice {
			break
		}
	}
	if len(got) != devices*perDevice {
		t.Fatalf("received %d of %d readings", len(got), devices*perDevice)
	}
	perDev := map[string]int{}
	for _, rd := range got {
		perDev[rd.Device]++
	}
	for d := 0; d < devices; d++ {
		if perDev[fmt.Sprintf("d%d", d)] != perDevice {
			t.Fatalf("device counts %v", perDev)
		}
	}
}
