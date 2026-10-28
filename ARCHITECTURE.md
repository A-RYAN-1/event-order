# Architecture

A map of the code for contributors. The package is one flat Go package, `eventorder`, with no dependencies; broker clients live in `examples/`, each its own module. Behaviour is specified by the doc comments (`go doc -all`) and the tests, this file says how the pieces connect.

## The core idea

Events carry an event time. A **watermark** is the largest event time seen minus a bounded delay. An event at or before the watermark is released in time order; an event that arrives with a time at or before the watermark is **late** and goes through the late policy. Everything else is built from this plus bounded memory.

## Files

| File | Type | Job |
|---|---|---|
| `buffer.go` | `Buffer` | One stream. A typed binary min-heap ordered by event time, then arrival number, so equal times keep arrival order. Options: delay, `WithMaxBuffered` cap, late policy, idle timeout. Not goroutine safe. |
| `strategy.go` | `DelayStrategy`, `PercentileDelay` | Lets the delay follow observed disorder instead of a fixed value. |
| `keyed.go` | `Keyed` | One `Buffer` per key (device, user, partition). A slow key never holds back another. |
| `safe.go` | `Safe`, `SafeKeyed` | A mutex around `Buffer` and `Keyed`. |
| `pipe.go` | `Pipe` | Channel in, channel out, context cancellation, idle release. |
| `reorder.go`, `merge.go` | `Reorder`, `Merge` | `iter.Seq` adapters; `Merge` is a k-way merge of already ordered sequences. |
| `tracker.go` | `Tracker`, `KeyedTracker` | The broker offset that is safe to commit while events are held: the lowest offset not yet done. `Revoke` forgets a partition on rebalance. |
| `dedup.go` | `Dedup` | A bounded window of seen ids, measured in event time, with an optional cap. |
| `aligned.go` | `Aligned` | One watermark over several sources: the minimum of the source watermarks, idle sources left out, never moving back. |
| `stream.go` | `Stream` | The consumer glue: `Dedup`, then `Keyed`, with a `KeyedTracker` behind them, in the order that is correct by construction. |
| `cmd/event-order` | command | Sorts JSON or delimited lines from stdin or a file with a bounded delay. |

## How a message flows through `Stream`

```
Ingest -> Dedup (drop repeats) -> Keyed buffer (hold per key) -> Records out in event order
                                        |                              |
                                 KeyedTracker.Track             caller processes, then Done
                                                                       |
                                                    Commit = lowest offset not done
```

`Aligned` sits beside this: feed it each source's watermark and use its minimum to advance one downstream `Buffer` that orders the merged stream.

## Invariants the tests defend

- No event is lost or invented: everything added comes out once, either released, flushed or counted late.
- Output is sorted by event time; equal times keep arrival order.
- Memory never exceeds the configured caps (`WithMaxBuffered`, the `Dedup` window and capacity).
- The commit offset never passes an event that is still held, and never moves back.
- The combined watermark of `Aligned` never moves back.

## How correctness is checked

Each type has a simple reference implementation in `reference_test.go` and property tests that compare the real type against it on random input (`property_test.go`). `Add`-style entry points and parsers have fuzz tests (`fuzz_test.go`, `fuzz_more_test.go`), concurrency is covered by `stress_test.go` under `-race`, and `memory_test.go` bounds memory. Mutation checks (deliberately breaking the code and confirming a test fails) back the claims in the README. `scripts/bench.sh` produces the benchmark table; the README quotes only numbers it produced.

## Adding something

1. Open an issue so the scope is agreed (`CONTRIBUTING.md`).
2. Write the doc comment first: what it promises and what it does not.
3. Add a reference model and a property test before the optimised code, then a fuzz test if it parses or takes arbitrary input.
4. Keep the root module dependency-free. A broker or framework adapter goes in its own module under `examples/` (or a separate module for a larger adapter).
5. Update `CHANGELOG.md` under Unreleased.
