#!/bin/sh
# Runs every benchmark ten times and summarises them with benchstat, so each
# number in the README has one source: the table this prints.
#
#   scripts/bench.sh                 # prints the summary
#   scripts/bench.sh before.txt      # also saves the raw runs, to compare later:
#                                    #   benchstat before.txt after.txt
#
# Needs benchstat: go install golang.org/x/perf/cmd/benchstat@latest
set -eu
out="${1:-$(mktemp)}"
go test -run xxx -bench . -benchmem -count 10 . | tee "$out" >/dev/null
echo "go $(go env GOVERSION) $(go env GOOS)/$(go env GOARCH), $(date -u +%Y-%m-%d)"
benchstat "$out"
