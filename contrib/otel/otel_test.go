package otel

import (
	"context"
	"testing"
	"time"

	eventorder "github.com/A-RYAN-1/event-order"
	sdk "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

var t0 = time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)

func collect(t *testing.T, r *sdk.ManualReader) map[string][]metricdata.DataPoint[float64] {
	t.Helper()
	var rm metricdata.ResourceMetrics
	if err := r.Collect(context.Background(), &rm); err != nil {
		t.Fatal(err)
	}
	out := map[string][]metricdata.DataPoint[float64]{}
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			switch d := m.Data.(type) {
			case metricdata.Gauge[int64]:
				for _, p := range d.DataPoints {
					out[m.Name] = append(out[m.Name], metricdata.DataPoint[float64]{Attributes: p.Attributes, Value: float64(p.Value)})
				}
			case metricdata.Gauge[float64]:
				out[m.Name] = append(out[m.Name], d.DataPoints...)
			case metricdata.Sum[int64]:
				for _, p := range d.DataPoints {
					out[m.Name] = append(out[m.Name], metricdata.DataPoint[float64]{Attributes: p.Attributes, Value: float64(p.Value)})
				}
			}
		}
	}
	return out
}

func TestObserveBuffer(t *testing.T) {
	old := Now
	Now = func() time.Time { return t0.Add(10 * time.Second) }
	defer func() { Now = old }()

	tests := []struct {
		name  string
		stats eventorder.Stats
		want  map[string]float64
	}{
		{"empty", eventorder.Stats{}, map[string]float64{
			"eventorder.buffer.buffered": 0, "eventorder.buffer.peak": 0,
			"eventorder.buffer.released": 0, "eventorder.buffer.late": 0}},
		{"busy", eventorder.Stats{Buffered: 3, Released: 40, Late: 2, Peak: 9, Watermark: t0}, map[string]float64{
			"eventorder.buffer.buffered": 3, "eventorder.buffer.peak": 9,
			"eventorder.buffer.released": 40, "eventorder.buffer.late": 2,
			"eventorder.buffer.watermark_lag": 10}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			r := sdk.NewManualReader()
			mp := sdk.NewMeterProvider(sdk.WithReader(r))
			reg, err := ObserveBuffer(mp.Meter("test"), "orders", func() eventorder.Stats { return tc.stats })
			if err != nil {
				t.Fatal(err)
			}
			got := collect(t, r)
			if len(got) != len(tc.want) {
				t.Fatalf("got %d instruments, want %d: %v", len(got), len(tc.want), got)
			}
			for k, v := range tc.want {
				pts := got[k]
				if len(pts) != 1 || pts[0].Value != v {
					t.Errorf("%s = %v, want %v", k, pts, v)
				}
				if n, _ := pts[0].Attributes.Value("name"); n.AsString() != "orders" {
					t.Errorf("%s name attribute = %q", k, n.AsString())
				}
			}
			if err := reg.Unregister(); err != nil {
				t.Fatal(err)
			}
			if after := collect(t, r); len(after) != 0 {
				t.Errorf("after Unregister still reporting: %v", after)
			}
		})
	}
}

func TestObserveAligned(t *testing.T) {
	a := eventorder.NewAligned[string]()
	a.Register("a")
	a.Register("b")
	a.Observe("a", t0.Add(30*time.Second))
	a.Observe("b", t0)
	a.Register("c") // never reports, so it holds the watermark

	r := sdk.NewManualReader()
	mp := sdk.NewMeterProvider(sdk.WithReader(r))
	if _, err := ObserveAligned(mp.Meter("test"), "feeds", a); err != nil {
		t.Fatal(err)
	}
	got := collect(t, r)

	behind := map[string]float64{}
	for _, p := range got["eventorder.aligned.source_behind"] {
		s, _ := p.Attributes.Value("source")
		behind[s.AsString()] = p.Value
	}
	if len(behind) != 2 || behind["a"] != 0 || behind["b"] != 30 {
		t.Errorf("source_behind = %v, want a=0 b=30 and no c", behind)
	}
	if v := got["eventorder.aligned.sources_holding"]; len(v) != 1 || v[0].Value != 1 {
		t.Errorf("sources_holding = %v, want 1 (c)", v)
	}
	if v := got["eventorder.aligned.sources_idle"]; len(v) != 1 || v[0].Value != 0 {
		t.Errorf("sources_idle = %v, want 0", v)
	}
}
