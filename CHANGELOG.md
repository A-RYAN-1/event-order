# Changelog

All notable changes are listed here, newest first. The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and the project follows [Semantic Versioning](https://semver.org/); until `v1.0.0` a minor version may change the API.

## [Unreleased]

## [0.2.0] - 2026-10-28

### Added

- Releases with GoReleaser (command binaries for Linux, macOS and Windows, zip and tar.gz, checksums); the release notes are the matching CHANGELOG section, and a tag must be on `main`. A snapshot build runs on pull requests that touch it.
- `scripts/apply-settings.sh` and `.github/rulesets/`: branch and tag rulesets and repository settings applied with `gh api` (dry run by default).
- `ARCHITECTURE.md`, `GOVERNANCE.md`, `SUPPORT.md` and `docs/FAQ.md`.
- Repository bots: PR labels by path and size, Conventional Commits title check, first-time contributor welcome, stale nudge (60 days, then 14 before close; roadmap and good first issues exempt), DCO sign-off check, all-contributors config.
- `examples/watermill`: a Watermill router handler (`Reorderer`) that publishes in event-time order, with an idle drain; its own module, tested over the in-memory gochannel.
- `scripts/bench.sh` and benchmarks for `Aligned` and `Stream`; README numbers come from its `-count 10` benchstat table.
- `Stream.Skip` for messages finished without processing (undecodable payloads).
- Real-broker integration tests (build tag `integration`): Kafka restart from committed offsets and a consumer-group rebalance against `apache/kafka`, MQTT ordering from several publishers against `eclipse-mosquitto`, both run in CI.
- Kafka example uses `Stream` and an idle drain (`-idle`) so the last readings of a quiet topic are released.
- `Stream`: the consumer glue in one type. `Ingest` takes a message (key, offset, time, id, value), drops duplicates, holds it per key and tracks its offset; `Done`, `Commit`, `Revoke`, `Flush`, `AdvanceAll` and `Watermark` complete the flow, so the order of Dedup, Buffer and Tracker calls is right by construction. Proof: crash-and-restart property test (nothing lost, commit never moves back), partitions aligned into one globally ordered stream, `FuzzStream`, mutation checks.
- `Aligned`: one watermark over several sources (the minimum of the source watermarks, idle sources left out by hand or by timeout, never moving back) and a per-source lag report. Proof: reference model, fuzz, race test, mutation checks.
- `examples/kafka` (kafka-go) and `examples/mqtt` (paho): integration examples, each its own module, tested without a broker.
- MIT `LICENSE`.
- `Tracker` and `KeyedTracker`: the broker offset that is safe to commit while events are held for reordering (lowest offset not done), with `Revoke` for partition rebalances.
- `Dedup`: bounded event-time window of seen ids to drop repeats from at-least-once sources, with an optional id cap.
- `WithDelayStrategy`, the `DelayStrategy` interface and `PercentileDelay`: the delay follows the observed disorder (a percentile of recent lags), with `WithMaxDelay` as the floor.
- `Merge`: k-way merge of several already-ordered `iter.Seq` sources.
- `Pipe`: channel in, channel out stage with context cancellation and `WithIdleTimeout`.
- `Safe` and `SafeKeyed`: goroutine-safe wrappers.
- `Buffer.AddTo` (append into a reused slice), `OnLate`, `Len`, `Watermark` and `Stats.Peak`.
- `Keyed.OnLate` and `Keyed.AdvanceAll` (forgets keys that went quiet).
- `event-order` command: streaming sort of JSON-lines or delimited text by timestamp.
- Fuzz test for the ordering and no-loss properties; benchmarks against a sort-on-every-add baseline.
- `doc.go`: package guide covering which type to choose, how to choose a delay, late events and how this compares with a stream processor.
- CI runs the build and tests on Windows and macOS, and `golangci-lint` (errcheck, govet, ineffassign, staticcheck, unused, misspell, bodyclose, unconvert).
- More command tests (numeric and quoted times, bad lines, column range, `-max-buffered`, multiple input files).

### Changed

- `Aligned.Lag` and the command's `run` split into smaller functions (cyclomatic complexity 22 and 16 down to 15 or less); behaviour unchanged, covered by the existing reference and property tests.
- README with a logo, light and dark figures for the three gaps, the architecture, the benchmark and the example output; `scripts/gen_docs_assets.py` regenerates them. Discussions link in the issue chooser.
- The heap is typed, so `Add` makes no allocation once the output slice has grown (previously 2 per call).
- Module path is `github.com/A-RYAN-1/event-order`; the package name stays `eventorder`.

## [0.1.0] - not yet tagged

First version: `Buffer`, `Keyed`, `Reorder`, `WithMaxDelay`, `WithMaxBuffered`, `WithLatePolicy`.
