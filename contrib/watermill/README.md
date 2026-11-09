# contrib/watermill

A Watermill handler that publishes messages in event-time order, in a separate module so the root module keeps zero dependencies.

```go
import eowm "github.com/A-RYAN-1/event-order/contrib/watermill"

re := eowm.NewReorderer(5*time.Second, eowm.WithLogger(logger))
router.AddHandler("reorder", "in", sub, "out", pub, re.Handle)
go re.Drain(ctx, pub, "out", time.Second) // idle drain, and a flush when ctx ends
```

The event time is read from message metadata (`event_time`, RFC 3339; change it with `WithTimeKey`). A message without a valid time is dropped and logged, because returning an error would make the broker redeliver it forever. Pass buffer options with `WithBuffer`, for example `eventorder.WithLatePolicy(eventorder.Emit)` to let late messages through.

## Limit

Watermill acknowledges the input message when the handler returns, so a message the Reorderer is holding is already acknowledged and a crash loses it. Where that matters, consume with the broker client and use `eventorder.Stream`, which keeps the offset uncommitted until the event is processed (see `examples/kafka`).

Use `NewGoChannel` with `BlockPublishUntilSubscriberAck: true` in tests; by default it delivers each message from its own goroutine, which reorders them.
