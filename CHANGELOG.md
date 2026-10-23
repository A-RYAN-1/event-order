# Changelog

All notable changes are listed here, newest first. The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and the project follows [Semantic Versioning](https://semver.org/); until `v1.0.0` a minor version may change the API.

## [Unreleased]

### Added

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

- The heap is typed, so `Add` makes no allocation once the output slice has grown (previously 2 per call).
- Module path is `github.com/A-RYAN-1/event-order`; the package name stays `eventorder`.

## [0.1.0] - not yet tagged

First version: `Buffer`, `Keyed`, `Reorder`, `WithMaxDelay`, `WithMaxBuffered`, `WithLatePolicy`.
