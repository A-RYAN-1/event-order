// Package otel exposes event-order counters as OpenTelemetry metrics. It is a
// separate module so the root module keeps zero dependencies.
//
// Wire a buffer and an aligned watermark in three lines:
//
//	reg, _ := otel.ObserveBuffer(meter, "orders", safe.Stats)
//	defer reg.Unregister()
//	areg, _ := otel.ObserveAligned(meter, "orders", aligned)
//
// Every instrument carries a "name" attribute so several buffers can share one
// meter.
package otel

import (
	"context"
	"fmt"
	"time"

	eventorder "github.com/A-RYAN-1/event-order"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// Now is the clock used for watermark lag; tests replace it.
var Now = time.Now

// ObserveBuffer registers observable instruments that read stats on every
// collection. Pass Safe.Stats for a buffer shared between goroutines; a plain
// Buffer's Stats is not safe to call from the collector goroutine.
//
// Instruments: eventorder.buffer.buffered and .peak (gauges, events),
// eventorder.buffer.released and .late (counters, events) and
// eventorder.buffer.watermark_lag (gauge, seconds; not reported until the
// buffer has a watermark).
func ObserveBuffer(m metric.Meter, name string, stats func() eventorder.Stats) (metric.Registration, error) {
	buffered, err := m.Int64ObservableGauge("eventorder.buffer.buffered", metric.WithUnit("{event}"), metric.WithDescription("events currently held"))
	if err != nil {
		return nil, err
	}
	peak, err := m.Int64ObservableGauge("eventorder.buffer.peak", metric.WithUnit("{event}"), metric.WithDescription("most events held at once"))
	if err != nil {
		return nil, err
	}
	released, err := m.Int64ObservableCounter("eventorder.buffer.released", metric.WithUnit("{event}"), metric.WithDescription("events released in order"))
	if err != nil {
		return nil, err
	}
	late, err := m.Int64ObservableCounter("eventorder.buffer.late", metric.WithUnit("{event}"), metric.WithDescription("events that arrived late"))
	if err != nil {
		return nil, err
	}
	lag, err := m.Float64ObservableGauge("eventorder.buffer.watermark_lag", metric.WithUnit("s"), metric.WithDescription("wall clock minus the buffer watermark"))
	if err != nil {
		return nil, err
	}
	attrs := metric.WithAttributes(attribute.String("name", name))
	return m.RegisterCallback(func(_ context.Context, o metric.Observer) error {
		s := stats()
		o.ObserveInt64(buffered, int64(s.Buffered), attrs)
		o.ObserveInt64(peak, int64(s.Peak), attrs)
		o.ObserveInt64(released, int64(s.Released), attrs)
		o.ObserveInt64(late, int64(s.Late), attrs)
		if !s.Watermark.IsZero() {
			o.ObserveFloat64(lag, Now().Sub(s.Watermark).Seconds(), attrs)
		}
		return nil
	}, buffered, peak, released, late, lag)
}

// ObserveAligned registers per-source lag for an Aligned. Instruments:
// eventorder.aligned.source_behind (gauge, seconds, attribute "source"),
// eventorder.aligned.sources_holding and eventorder.aligned.sources_idle
// (gauges, sources). The source attribute is fmt.Sprint of the source key, so
// use a bounded set of sources.
func ObserveAligned[S comparable](m metric.Meter, name string, a *eventorder.Aligned[S]) (metric.Registration, error) {
	behind, err := m.Float64ObservableGauge("eventorder.aligned.source_behind", metric.WithUnit("s"), metric.WithDescription("how far a source is behind the newest active source"))
	if err != nil {
		return nil, err
	}
	holding, err := m.Int64ObservableGauge("eventorder.aligned.sources_holding", metric.WithUnit("{source}"), metric.WithDescription("sources holding the combined watermark back"))
	if err != nil {
		return nil, err
	}
	idle, err := m.Int64ObservableGauge("eventorder.aligned.sources_idle", metric.WithUnit("{source}"), metric.WithDescription("sources left out of the minimum"))
	if err != nil {
		return nil, err
	}
	nameAttr := attribute.String("name", name)
	return m.RegisterCallback(func(_ context.Context, o metric.Observer) error {
		var h, i int64
		for _, st := range a.Lag() {
			if st.Holding {
				h++
			}
			if st.Idle {
				i++
			}
			if st.Reported && !st.Idle {
				o.ObserveFloat64(behind, st.Behind.Seconds(), metric.WithAttributes(nameAttr, attribute.String("source", fmt.Sprint(st.Source))))
			}
		}
		o.ObserveInt64(holding, h, metric.WithAttributes(nameAttr))
		o.ObserveInt64(idle, i, metric.WithAttributes(nameAttr))
		return nil
	}, behind, holding, idle)
}
