// Command event-order sorts a stream of timestamped lines into event-time
// order using a bounded delay, without reading the whole input into memory.
//
//	tail -f app.log | event-order -field ts -delay 5s
//	event-order -col 1 -sep , -delay 1m events.csv > sorted.csv
//
// Lines are JSON objects (-field NAME) or delimited text (-col N). A time is
// RFC 3339, Unix seconds (fractions allowed) or Unix milliseconds. Memory is
// bounded by how many lines fall inside the delay window, or by -max-buffered.
package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"time"

	eventorder "github.com/A-RYAN-1/event-order"
)

type config struct {
	field  string
	col    int
	sep    string
	delay  time.Duration
	late   string
	maxBuf int
	stats  bool
}

func main() {
	var c config
	flag.StringVar(&c.field, "field", "", "JSON key that holds the event time")
	flag.IntVar(&c.col, "col", 0, "1-based column that holds the event time (delimited text)")
	flag.StringVar(&c.sep, "sep", ",", "column separator for -col; use \"ws\" for runs of whitespace")
	flag.DurationVar(&c.delay, "delay", 5*time.Second, "how far out of order lines may arrive")
	flag.StringVar(&c.late, "late", "drop", "late lines: drop, emit (print in place) or stderr")
	flag.IntVar(&c.maxBuf, "max-buffered", 0, "cap on lines held in memory; 0 means no cap")
	flag.BoolVar(&c.stats, "stats", false, "print counters to stderr at the end")
	flag.Usage = func() {
		fmt.Fprintln(os.Stderr, "usage: event-order (-field NAME | -col N) [-sep S] [-delay D] [-late drop|emit|stderr] [-max-buffered N] [-stats] [file ...]")
		flag.PrintDefaults()
	}
	flag.Parse()

	var in io.Reader = os.Stdin
	if flag.NArg() > 0 {
		var rs []io.Reader
		for _, name := range flag.Args() {
			f, err := os.Open(name)
			if err != nil {
				fmt.Fprintln(os.Stderr, "event-order:", err)
				os.Exit(2)
			}
			defer f.Close()
			rs = append(rs, f)
		}
		in = io.MultiReader(rs...)
	}
	out := bufio.NewWriter(os.Stdout)
	err := run(in, out, os.Stderr, c)
	if ferr := out.Flush(); err == nil {
		err = ferr
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "event-order:", err)
		os.Exit(2)
	}
}

func run(in io.Reader, out, errw io.Writer, c config) error {
	if (c.field == "") == (c.col == 0) {
		return fmt.Errorf("give exactly one of -field or -col")
	}
	if c.late != "drop" && c.late != "emit" && c.late != "stderr" {
		return fmt.Errorf("-late must be drop, emit or stderr")
	}
	opts := []eventorder.Option{eventorder.WithMaxDelay(c.delay), eventorder.WithMaxBuffered(c.maxBuf)}
	if c.late == "emit" {
		opts = append(opts, eventorder.WithLatePolicy(eventorder.Emit))
	}
	b := eventorder.New[string](opts...)
	if c.late == "stderr" {
		b.OnLate(func(e eventorder.Event[string]) { fmt.Fprintln(errw, e.Value) })
	}

	var bad int
	var buf []eventorder.Event[string]
	write := func(evs []eventorder.Event[string]) error {
		for _, e := range evs {
			if _, err := io.WriteString(out, e.Value+"\n"); err != nil {
				return err
			}
		}
		return nil
	}
	sc := bufio.NewScanner(in)
	sc.Buffer(make([]byte, 0, 64*1024), 16*1024*1024)
	for n := 1; sc.Scan(); n++ {
		line := sc.Text()
		if strings.TrimSpace(line) == "" {
			continue
		}
		t, err := lineTime(line, c)
		if err != nil {
			bad++
			fmt.Fprintf(errw, "event-order: line %d skipped: %v\n", n, err)
			continue
		}
		buf = b.AddTo(buf[:0], t, line)
		if err := write(buf); err != nil {
			return err
		}
	}
	if err := sc.Err(); err != nil {
		return err
	}
	if err := write(b.Flush()); err != nil {
		return err
	}
	if c.stats {
		s := b.Stats()
		fmt.Fprintf(errw, "released %d, late %d, skipped %d, peak held %d\n", s.Released, s.Late, bad, s.Peak)
	}
	return nil
}

func lineTime(line string, c config) (time.Time, error) {
	if c.field != "" {
		var m map[string]any
		if err := json.Unmarshal([]byte(line), &m); err != nil {
			return time.Time{}, fmt.Errorf("not a JSON object: %v", err)
		}
		v, ok := m[c.field]
		if !ok {
			return time.Time{}, fmt.Errorf("no %q field", c.field)
		}
		switch x := v.(type) {
		case string:
			return parseTime(x)
		case float64:
			return fromNumber(x), nil
		}
		return time.Time{}, fmt.Errorf("field %q is not a time", c.field)
	}
	var cols []string
	if c.sep == "ws" {
		cols = strings.Fields(line)
	} else {
		cols = strings.Split(line, c.sep)
	}
	if c.col > len(cols) {
		return time.Time{}, fmt.Errorf("only %d columns", len(cols))
	}
	return parseTime(strings.TrimSpace(cols[c.col-1]))
}

func parseTime(s string) (time.Time, error) {
	s = strings.Trim(s, `"`)
	if t, err := time.Parse(time.RFC3339Nano, s); err == nil {
		return t, nil
	}
	if f, err := strconv.ParseFloat(s, 64); err == nil {
		return fromNumber(f), nil
	}
	return time.Time{}, fmt.Errorf("unrecognised time %q", s)
}

// fromNumber reads Unix seconds, or milliseconds when the number is too large
// to be seconds for any date before the year 5000.
func fromNumber(f float64) time.Time {
	if f > 1e11 {
		f /= 1000
	}
	sec := int64(f)
	return time.Unix(sec, int64((f-float64(sec))*1e9))
}
