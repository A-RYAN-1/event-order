# FAQ

## What does it do in one sentence?

It holds events that arrive late and shuffled and releases them in event-time order once a watermark says no earlier event is expected, and it tells you which broker offset is safe to commit while events are held.

## How do I choose the delay?

Measure the largest gap between an event's time and the newest time seen when it arrives, then set `WithMaxDelay` a little above it. A larger delay tolerates more disorder and holds events longer; a smaller one releases sooner and turns more events into late ones. If the disorder changes over time, use `WithDelayStrategy` with `PercentileDelay`. See "Choosing a delay" in `go doc eventorder`.

## What happens to a late event?

The default policy drops it and counts it in `Stats.Late`; `Emit` returns it at once with `Late` set. `OnLate` sees every late event whatever the policy, so you can send it to a dead-letter queue or count it.

## Is a crash safe?

Held events are in memory and are lost on a crash. That is why `Tracker` exists: commit only the offset it returns, so after a restart the broker replays what was held. You get replay, not loss, and no exactly-once guarantee.

## Does it remove duplicates?

`Dedup` drops repeated ids inside a bounded window of event time, in memory. A duplicate after its window, or after a restart, is not caught. Keep processing idempotent where a repeat costs money.

## Can I use it from several goroutines?

`Buffer` and `Keyed` are not goroutine safe by design. Use `Safe`, `SafeKeyed` or `Pipe`, or serialise calls. `Tracker`, `Dedup`, `Aligned` and `Stream` are safe for concurrent use.

## Why not container/heap, Flink or Kafka Streams?

You can write the heap in forty lines; the library is the decisions around it (exact lateness boundary, ties, memory cap, idle sources, commit offsets, bounded duplicate window), each tested against a reference. Flink, Beam and Kafka Streams are full stream processors for a cluster or a JVM stack; this is the one watermark idea in-process. Details and numbers are in the README.

## Do my events need a timestamp?

Yes, an event time from the producer. The library trusts the producer clock and does not repair skew. If you control the producer, a sequence number is more reliable (an ordering-by-sequence type is a planned contribution, see the issues).

## Does it work with Kafka, MQTT or Watermill?

Yes, through the example modules in `examples/`, each its own module so the root has no dependencies. They are tested without a broker, and the Kafka and MQTT ones also against real brokers in CI.

## Which Go versions?

Go 1.23 and the latest stable, on Linux, Windows and macOS (see CI).

## Where do I report a bug or ask a question?

See `SUPPORT.md`.
