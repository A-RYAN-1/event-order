#!/bin/sh
# Mutation check: applies one small source mutation at a time, runs the tests
# and fails if any mutation survives (tests still pass). The README's proof
# claim "each mutation fails tests" is this script.
#
#   scripts/mutate.sh            # run every mutation
#   scripts/mutate.sh 3 7        # run only mutations 3 and 7
#
# Each file is restored from a backup after every mutation and on exit, even
# on failure or interrupt. Needs python3 (for exact-text replacement).
set -u
cd "$(dirname "$0")/.."

# file | exact original text (must occur once) | replacement | what it breaks
MUTATIONS='
buffer.go|return h[i].seq < h[j].seq|return h[i].seq > h[j].seq|equal times lose arrival order
buffer.go|if b.hasWM && !t.After(b.wm) {|if b.hasWM && t.Before(b.wm) {|event exactly at the watermark is no longer late
buffer.go|!b.h[0].ev.Time.After(b.wm)|b.h[0].ev.Time.Before(b.wm)|event exactly at the watermark is not released
buffer.go|len(b.h) > b.opt.maxBuf {|len(b.h) > b.opt.maxBuf+1 {|buffer cap off by one
buffer.go|if t.After(b.maxSeen) {|if t.Before(b.maxSeen) {|newest time tracked backwards
tracker.go|offset <= t.last|offset < t.last|repeated offset accepted
tracker.go|t.entries[t.head].done {|!t.entries[t.head].done {|commit head advances over unfinished work
dedup.go|d.newest.Sub(t) > d.window {|d.newest.Sub(t) >= d.window {|id at the window edge treated as expired
dedup.go|d.newest.Sub(d.h[0].t) > d.window {|d.newest.Sub(d.h[0].t) >= d.window {|ids expire one tick early
dedup.go|return a.seq < b.seq|return a.seq > b.seq|equal-time ids evicted in the wrong order
dedup.go|len(d.h) > d.maxIDs {|len(d.h) > d.maxIDs+1 {|id cap off by one
aligned.go|now.Sub(s.lastSeen) <= a.timeout|now.Sub(s.lastSeen) < a.timeout|source at the idle timeout edge dropped early
aligned.go|s.wm.Equal(min)|!s.wm.Equal(min)|holding source misreported
'

only=" $* "
work=$(mktemp -d)
cur=""
restore() {
	if [ -n "$cur" ] && [ -f "$work/backup" ]; then cp "$work/backup" "$cur"; fi
	rm -rf "$work"
}
trap restore EXIT
trap 'exit 130' INT TERM

n=0; killed=0; survived=0; skipped=0
survivors=""
while IFS='|' read -r file old new why; do
	[ -z "$file" ] && continue
	n=$((n + 1))
	case "$only" in " "*) if [ "$#" -gt 0 ] && ! echo "$only" | grep -q " $n "; then continue; fi ;; esac
	cp "$file" "$work/backup"; cur="$file"
	if ! OLD="$old" NEW="$new" python3 - "$file" <<'PY'
import os, sys
p = sys.argv[1]
s = open(p, encoding="utf8", newline="").read()
old, new = os.environ["OLD"], os.environ["NEW"]
if s.count(old) != 1:
    sys.exit(1)
open(p, "w", encoding="utf8", newline="").write(s.replace(old, new))
PY
	then
		echo "SKIP     #$n $file: original text not found exactly once ($why)"
		skipped=$((skipped + 1)); cp "$work/backup" "$file"; cur=""; continue
	fi
	if go test -count=1 . >/dev/null 2>&1; then
		echo "SURVIVED #$n $file: $why"
		survived=$((survived + 1)); survivors="$survivors #$n"
	else
		echo "killed   #$n $file: $why"
		killed=$((killed + 1))
	fi
	cp "$work/backup" "$file"; cur=""
done <<EOF
$MUTATIONS
EOF

echo "mutations: $killed killed, $survived survived, $skipped skipped"
[ "$survived" -eq 0 ] && [ "$skipped" -eq 0 ]
