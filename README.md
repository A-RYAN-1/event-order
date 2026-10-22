# eventorder

Put out-of-order events back in event-time order, in plain Go, with no dependencies.

Sensors, brokers and mobile clients deliver events late and shuffled. Stream processors such as Flink solve
this with watermarks, but pulling one in for a service that only needs "give me these readings in the order
they happened" is a lot. `eventorder` is that one idea as a small generic library: hold events until a
watermark says nothing earlier is expected, then release them sorted.

```go
b := eventorder.New[Reading](
    eventorder.WithMaxDelay(5*time.Second), // how late an event may be
    eventorder.WithMaxBuffered(10_000),     // hard memory cap
)

for msg := range incoming {
    for _, ev := range b.Add(msg.Time, msg.Reading) {
        process(ev.Value) // arrives sorted by event time
    }
}
for _, ev := range b.Flush() { // end of input
    process(ev.Value)
}
```

## Install

```
go get github.com/A-RYAN-1/eventorder
```

Needs Go 1.23 or newer (generics and `iter`).

## What it gives you

| Piece | Use it for |
|---|---|
| `Buffer[T]` | One stream: `Add`, `AdvanceTo` (push the watermark from a heartbeat), `Flush`, `Stats` |
| `Keyed[K, T]` | Many streams (devices, users, partitions) reordered independently; one slow key never holds back the others |
| `Reorder(seq, opts...)` | Wrap an `iter.Seq` of events and range over it in order |
| `WithMaxDelay` | How far out of order events may arrive; bigger tolerates more and holds events longer |
| `WithMaxBuffered` | Cap on held events; the oldest are released early instead of growing memory |
| `WithLatePolicy` | `Drop` (counted in `Stats`) or `Emit` (returned immediately with `Late` set) |

## How it decides

The watermark is the largest event time seen minus `MaxDelay`. Everything at or before the watermark is released in
event-time order; an event that arrives at or before the watermark is late. Events with equal times keep their
arrival order. `AdvanceTo` lets a timer or heartbeat move the watermark when a source goes quiet, and it never moves
backwards.

## Behaviour you should know

- Not safe for concurrent use. Wrap it in a mutex or give each buffer one goroutine.
- A larger `MaxDelay` is a latency cost: an event waits until a later event moves the watermark past it, or until `AdvanceTo` or `Flush`.
- `WithMaxBuffered` trades correctness for memory: events released early can make later arrivals late.
- Late events are never reordered into the past; choose `Emit` to see them and decide yourself.
- `Keyed` keeps a buffer per key until you `Flush` that key. Flush finished keys.

## Measured

`go test -bench` on the author's laptop (Windows, 16 threads, Go 1.27): `Add` with a 1 s delay and a 1024-event cap runs
at about 415 ns per event with 2 allocations. Tests cover ordering, ties, lateness, the cap, watermark movement, the
keyed and iterator forms, and a 5,000-event shuffle that must come out sorted with nothing late (95 percent statement
coverage).

## Development

```
go vet ./...
go test -race -count=1 ./...
go test -bench . -run xxx
```

CI runs vet, gofmt and the race-enabled tests on the oldest and newest supported Go.
