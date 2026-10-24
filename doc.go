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
//   - Reorder wraps an iter.Seq of events, and Merge joins several ordered
//     sequences into one.
//   - Tracker and KeyedTracker say which broker offset is safe to commit while
//     events are still held, see Committing offsets.
//   - Dedup drops repeated events from an at-least-once source, see Duplicates.
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
// When the disorder changes over time, WithDelayStrategy with PercentileDelay
// follows it: the delay becomes a percentile of the lags seen in the last
// window events, and WithMaxDelay stays as the floor. A rare very late event
// is then late instead of stretching the delay for everyone.
//
// # Late events
//
// An event whose time is at or before the watermark when it arrives is late.
// The default policy drops it and counts it in Stats.Late; Emit returns it at
// once with Late set. OnLate sees every late event whatever the policy, for a
// dead-letter queue or a metric.
//
// # Committing offsets
//
// A broker commit means everything before an offset is done. Events held in a
// Buffer are not done, so committing the newest offset seen loses them if the
// process crashes. Call Tracker.Track when an event enters the buffer and
// Tracker.Done when it has been processed or dropped as late (OnLate), and
// commit what Tracker.Commit returns: the lowest offset not yet done. After a
// restart the consumer re-reads from there, so nothing is lost; events released
// before the crash but after the commit are read again, so processing should
// tolerate a repeat. With one Keyed buffer and one KeyedTracker per partition,
// a rebalance revoke is Keyed.Flush(partition), Done for each flushed event,
// then KeyedTracker.Revoke(partition) for the offset to commit.
//
// # Duplicates
//
// At-least-once delivery repeats events, often out of order. Dedup.Seen(t, id)
// reports whether id was seen within a window of event time, and bounds its
// memory with the window and an optional cap on ids. Call it before Buffer.Add
// with a window at least as long as the delay plus the longest retry. It lives
// in memory only: after a restart the earlier ids are gone.
//
// # Compared with a stream processor
//
// Flink and similar systems offer watermarks, allowed lateness and state
// backends for whole pipelines. This package is the ordering step alone, in
// memory, in one process. It does not persist what it holds: a crash loses the
// buffered events, so keep the broker offset and replay after a restart.
package eventorder
