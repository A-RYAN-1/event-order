# contrib/otel

OpenTelemetry metrics for event-order, in a separate module so the root module keeps zero dependencies.

```go
import eo "github.com/A-RYAN-1/event-order"
import eootel "github.com/A-RYAN-1/event-order/contrib/otel"

reg, _ := eootel.ObserveBuffer(meter, "orders", safe.Stats)   // safe is an *eo.Safe
areg, _ := eootel.ObserveAligned(meter, "orders", aligned)    // aligned is an *eo.Aligned[string]
defer func() { reg.Unregister(); areg.Unregister() }()
```

| Instrument | Kind | Unit | Meaning |
|---|---|---|---|
| `eventorder.buffer.buffered` | gauge | event | events held now |
| `eventorder.buffer.peak` | gauge | event | most held at once |
| `eventorder.buffer.released` | counter | event | released in order |
| `eventorder.buffer.late` | counter | event | arrived late |
| `eventorder.buffer.watermark_lag` | gauge | s | wall clock minus watermark; absent until a watermark exists |
| `eventorder.aligned.source_behind` | gauge | s | per source, behind the newest active source |
| `eventorder.aligned.sources_holding` | gauge | source | sources holding the combined watermark back |
| `eventorder.aligned.sources_idle` | gauge | source | sources left out of the minimum |

Every point has a `name` attribute; `source_behind` also has `source` (the key printed with `fmt.Sprint`, so keep the set of sources small). Pass `Safe.Stats` for a buffer used from several goroutines; a plain `Buffer` is not safe to read from the collector.
