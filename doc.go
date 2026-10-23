// Package eventorder puts out-of-order events back in event-time order.
//
// Events from sensors, message brokers and mobile clients arrive late and
// shuffled. A Buffer holds them until a watermark says no earlier event is
// expected, then releases them sorted by event time. The watermark is the
// largest event time seen so far minus a bounded delay, the same model stream
// processors use, without a stream processor.
//
// # Choosing a type
//
//   - Buffer orders one stream. Add returns what is ready, AdvanceTo moves the
//     watermark from a timer, Flush empties it at shutdown.
//   - Keyed orders many streams (devices, users, partitions) independently, so
//     one slow key never holds back the others.
//   - Pipe wraps a channel and, with WithIdleTimeout, releases the tail when the
//     source goes quiet.
//   - Safe and SafeKeyed add a mutex for several goroutines.
//   - Reorder wraps an iter.Seq of events.
//
// Buffer and Keyed are not safe for concurrent use; serialise calls with a
// mutex or a single goroutine, or use Safe, SafeKeyed or Pipe.
//
// # Choosing a delay
//
// WithMaxDelay is how far out of order an event may arrive and still be placed
// correctly. Measure the largest gap between an event's time and the newest
// time seen when it arrives, and set the delay a little above it. A larger
// delay tolerates more disorder and holds events longer; a smaller one releases
// sooner and turns more events into late ones. WithMaxBuffered bounds memory
// when the delay is generous: the oldest events are released early and the
// watermark moves up to them.
//
// # Late events
//
// An event whose time is at or before the watermark when it arrives is late.
// The default policy drops it and counts it in Stats.Late; Emit returns it at
// once with Late set. OnLate sees every late event whatever the policy, for a
// dead-letter queue or a metric.
//
// # Compared with a stream processor
//
// Flink and similar systems offer watermarks, allowed lateness and state
// backends for whole pipelines. This package is the ordering step alone, in
// memory, in one process. It does not persist what it holds: a crash loses the
// buffered events, so keep the broker offset and replay after a restart.
package eventorder
