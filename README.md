<h1 align="center">event-order</h1>

<p align="center">
Put out-of-order events back in event-time order, in plain Go.<br>
Zero dependencies. Bounded memory. Safe offsets to commit while events are held. Drops repeats from at-least-once sources.
</p>

<p align="center">
<a href="#the-problem">The problem</a> |
<a href="#try-it">Try it</a> |
<a href="#what-is-in-it">What is in it</a> |
<a href="#why-not-containerheap">Why not container/heap</a> |
<a href="#kafka-and-mqtt">Kafka and MQTT</a> |
<a href="#proof">Proof</a> |
<a href="#related-work">Related work</a>
</p>

<p align="center"><sub>Status: v0.x, the API may change between minor versions. Last updated 2026-10-27.</sub></p>

---

## The problem

Brokers, retrying producers, sensors and mobile clients deliver events late, shuffled and sometimes twice. Code that
trusts arrival order then computes the wrong thing: a reading applied before an older one, a total counted twice, an
offset committed past events that were never processed.

Stream processors such as Flink solve this with watermarks, but a Go service that only needs "give me these in the order
they happened" should not need a cluster. `event-order` is that one step as a library: add events as they arrive, get
back the ones that are now safe to process, sorted by event time.

## Try it

```go
b := eventorder.New[Reading](
    eventorder.WithMaxDelay(5*time.Second), // how far out of order events may arrive
    eventorder.WithMaxBuffered(10_000),     // memory bound
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

Install with `go get github.com/A-RYAN-1/event-order` (Go 1.23 or newer). The import path ends in `event-order`; the package
is `eventorder`. To see it work first, clone the repository and run `go run ./examples/sensors`: six readings arrive out
of order and the last one is too late. Change `WithMaxDelay(3*time.Second)` to `10*time.Second` and the late one is in order.

## What is in it

| You have | Use |
|---|---|
| One stream | `Buffer[T]`: `Add` returns what is ready, `AdvanceTo` moves the watermark from a timer, `Flush` empties it |
| Many streams (devices, users, partitions) | `Keyed[K, T]`: one buffer per key, `Flush(key)` and `AdvanceAll` forget finished keys |
| A channel | `Pipe`: channel in, ordered channel out, with a context and `WithIdleTimeout` for quiet sources |
| Several goroutines | `Safe`, `SafeKeyed`: the same with a mutex |
| Kafka or MQTT consumer glue | `Stream`: duplicates, per-key ordering and safe commit offsets in one type |
| One watermark over several sources | `Aligned`: minimum of the source watermarks, idle sources excluded, `Lag` names the holder |
| Several ordered sources | `Merge` over `iter.Seq`; `Reorder` wraps one `iter.Seq` |
| A broker that needs commits | `Tracker`, `KeyedTracker`: the offset that is safe to commit while events are held, `Revoke` for rebalances |
| An at-least-once source | `Dedup`: drops repeated ids inside a bounded window of event time |
| A log or CSV in a shell | the `event-order` command, streaming |

Options: `WithMaxDelay` (how late is allowed), `WithMaxBuffered` (memory cap; the oldest events are released early),
`WithLatePolicy` (`Drop` and count, or `Emit` flagged `Late`), `WithDelayStrategy(PercentileDelay(0.99, 1024))` (the
delay follows the disorder actually seen), `OnLate` (every late event, for a dead-letter queue or a metric).

Rules, so there are no surprises: the watermark is the largest event time seen minus the delay, inclusive of events at
exactly that time; everything at or before it is released, sorted; equal times keep arrival order; an event at or before
the watermark when it arrives is late. The package guide on pkg.go.dev (`doc.go`) says how to choose a type and a delay.

```go
// Safe offset to commit while events are held for reordering.
tr := eventorder.NewTracker()
tr.Track(offset)               // when an event enters the buffer
tr.Done(ev.Value.Offset)       // when it was processed, or dropped as late
next, _ := tr.Commit()         // commit this: the lowest offset not done

// Drop repeats before they reach the buffer.
d := eventorder.NewDedup[string](time.Minute, 100_000)
if !d.Seen(msg.Time, msg.ID) {
    b.Add(msg.Time, msg.Reading)
}
```

## Why not container/heap

The honest answer: you can write the ordering part yourself in about forty lines, and a hand-rolled `container/heap` loop
is not slower in a way you will notice. An earlier hand-rolled `container/heap` loop measured within noise of this library
(163 to 429 ns against 192 ns, 2026-10-24, not kept in the repo, so no current figure is claimed). `Buffer.Add` costs
105 to 131 ns per event with 0 allocations (table below). Speed is not the reason to use this library.

The reason is the decisions around the heap that are easy to get subtly wrong, each tested here against a reference
implementation that shares no code with it: the exact lateness boundary; ties kept in arrival order; the memory cap never
exceeded; no event lost or invented; per-key buffers that forget idle keys; a quiet source that does not hold its tail
forever; a commit offset that never passes a held event; a duplicate window that stays bounded. If you only need to sort a
slice you already have, use `slices.SortFunc`.

## Kafka and MQTT

Each is its own Go module under `examples/`, so this library stays dependency-free, and each takes a small interface or a
channel so its tests run without a broker.

- `examples/kafka` (`segmentio/kafka-go`): per-partition ordering, `Dedup`, and commits only up to the lowest event not
  yet processed. Its test simulates a crash at a random point in a shuffled three-partition stream and restarts from the
  committed offsets: across 200 random crashes no reading is lost; the same consumer that commits the offset of every fetched
  message instead loses readings in all 200.
- `examples/mqtt` (`eclipse/paho.mqtt.golang`): a subscriber that orders readings with `Pipe` and releases the tail when
  the broker goes quiet.
- `examples/watermill` (`ThreeDotsLabs/watermill`): a router handler that holds messages and publishes them in event-time
  order, plus an idle drain for the tail of a quiet topic. Limit: Watermill acknowledges a message when the handler
  returns, so a message being held is lost on a crash; where that matters use `Stream` with the broker client, as in the
  Kafka example.

Rebalances: `kafka-go` can deliver messages fetched before a rebalance after it (its issue
[1398](https://github.com/segmentio/kafka-go/issues/1398) describes it, still open when this was written). `Tracker`
cannot fix that, but it keeps your commit position correct while you handle it: flush the partition with `Keyed.Flush`,
mark the flushed events `Done`, then `KeyedTracker.Revoke`.

## Command line

```
go install github.com/A-RYAN-1/event-order/cmd/event-order@latest
tail -f app.log | event-order -field ts -delay 5s          # JSON lines, "ts" key
event-order -col 1 -sep , -delay 1m events.csv > sorted.csv  # CSV, first column
```

Times may be RFC 3339, Unix seconds or Unix milliseconds. Late lines are dropped, printed in place (`-late emit`) or sent to
stderr (`-late stderr`); `-max-buffered N` caps memory; `-stats` prints counters. Measured 2026-10-23 on a 95 MB,
2,000,000-line JSON log shuffled out of order (`-delay 1h`): output fully sorted, none lost, 4.2 s, 13 MB peak memory.

## Proof

Measured on Windows, amd64, Go 1.27.0, i5-13450HX: the tests and fuzz rows on 2026-10-24, the cost rows on 2026-10-27. CI runs the same tests with `-race` on Linux, plus Windows and macOS.

| Check | Result |
|---|---|
| Statement coverage | 97.8 percent (`go test -cover .`) |
| Reference-model property tests | 3,000 random operation sequences for `Buffer`, 1,000 for `Keyed`, 2,000 each for `Tracker` and `Dedup`, output identical to a sort-every-step reference, call for call |
| Crash safety | 300 random runs of `Buffer` plus `Tracker`: the commit offset never passes an unprocessed event |
| Mutation check | removing the boundary nanosecond, the arrival tie-break, the head advance of `Tracker`, the expiry bound or the cap of `Dedup` each fails tests |
| Fuzz | `FuzzKeyed` 857,827 executions, `FuzzMerge` 45,326, `FuzzTracker` 3,852,990, `FuzzDedup` 3,684,370, no failure |
| Memory | about 100 bytes per held event; `Len` never above the cap over 200,000 adds; `Dedup` holds about window-many ids |
| Cost per call | median of 10 runs, 0 allocations each: `Buffer.Add` 105 ns (jitter 16) and 131 ns (jitter 256); `Aligned.Observe` 109 ns; `Stream.Ingest` 230 ns; `Tracker` Track plus Done plus Commit 50 ns; `Dedup.Seen` 237 ns. Run-to-run spread is 1 to 5 percent for the first four rows and 31 to 75 percent for `Tracker` and `Dedup` on this laptop, so read those two as rough |
| Against sort-every-add | 131 ns against 1,493 ns at a disorder window of 256 (3 allocations per add for the baseline) |

Reproduce with `go test -count=1 -cover ./...`, `scripts/bench.sh` (`-count 10` plus `benchstat`; install with `go install golang.org/x/perf/cmd/benchstat@latest`) and
`go test -run xxx -fuzz FuzzTracker -fuzztime 20s`.

## Limits

- Events are held in memory only: a crash loses what is held. Keep the broker offset (`Tracker`) and replay.
- Ordering by event time trusts the producers' clocks. If clocks drift, use a sequence number where you control the
  producer; this library does not repair skew.
- `Dedup` is in memory and windowed: a duplicate after its window, or after a restart, is not caught. Keep processing
  idempotent where a repeat costs money.
- `Buffer` and `Keyed` are not goroutine safe by design; use `Safe`, `SafeKeyed` or `Pipe`.
- v0.x: until v1.0.0 a minor version may change the API.

## Related work

Searched 2026-10-24 on pkg.go.dev and GitHub. An honest map of what else exists:

| Project | What it is | How it differs |
|---|---|---|
| [`jamesrom/order/chansort`](https://pkg.go.dev/github.com/jamesrom/order/chansort) | sliding-window channel sorter | orders on a wall-clock window, no watermark or late policy, last updated 2022 |
| [`teivah/resequencer`](https://github.com/teivah/resequencer) | resequencer | sequence numbers, not event time; no license, last pushed 2022 |
| [`reugn/go-streams`](https://github.com/reugn/go-streams) | stream processing framework | windows over a pipeline, not a standalone reorder stage |
| [eKuiper](https://github.com/lf-edge/ekuiper), [StreamSQL](https://github.com/rulego/streamsql) | stream engines with a watermark inside | engines you run, not a library you import |
| Flink, Beam, Kafka Streams | full stream processors | JVM or a cluster; this is the one watermark idea in-process |

## Run it locally

Needs Go 1.23 or newer.

```
git clone https://github.com/A-RYAN-1/event-order.git
cd event-order
go test -count=1 ./...
go run ./examples/sensors
cd examples/kafka && go test ./...    # examples are separate modules
```

No environment variables, no network calls. It is a library: nothing to deploy. See `CONTRIBUTING.md` before opening a
pull request and `SECURITY.md` to report a problem.

## License

MIT, see `LICENSE`.

## Credits

Designed and written by Aryan Deshmukh (A-RYAN-1).
