package main

import (
	"strings"
	"testing"
	"time"
)

func runString(t *testing.T, in string, c config) (out, errs string) {
	t.Helper()
	var o, e strings.Builder
	if err := run(strings.NewReader(in), &o, &e, c); err != nil {
		t.Fatal(err)
	}
	return o.String(), e.String()
}

func TestJSONLinesSorted(t *testing.T) {
	in := `{"ts":"2026-01-01T00:00:03Z","m":"c"}
{"ts":"2026-01-01T00:00:01Z","m":"a"}
{"ts":"2026-01-01T00:00:02Z","m":"b"}
`
	out, _ := runString(t, in, config{field: "ts", delay: 5 * time.Second, late: "drop"})
	want := `{"ts":"2026-01-01T00:00:01Z","m":"a"}
{"ts":"2026-01-01T00:00:02Z","m":"b"}
{"ts":"2026-01-01T00:00:03Z","m":"c"}
`
	if out != want {
		t.Fatalf("got\n%s", out)
	}
}

func TestCSVColumnAndUnixTimes(t *testing.T) {
	in := "x,1700000003\ny,1700000001\nz,1700000002500\n"
	out, _ := runString(t, in, config{col: 2, sep: ",", delay: time.Minute, late: "drop"})
	// 1700000002500 is milliseconds, so it lands between the other two.
	if out != "y,1700000001\nz,1700000002500\nx,1700000003\n" {
		t.Fatalf("got %q", out)
	}
}

func TestLatePoliciesAndBadLines(t *testing.T) {
	in := "10 a\n1 old\nnot-a-time b\n20 c\n"
	for _, tc := range []struct {
		late, wantOut, wantErr string
	}{
		{"drop", "10 a\n20 c\n", "line 3 skipped"},
		{"emit", "1 old\n10 a\n20 c\n", "line 3 skipped"},
		{"stderr", "10 a\n20 c\n", "1 old"},
	} {
		out, errs := runString(t, in, config{col: 1, sep: "ws", delay: 0, late: tc.late, stats: true})
		if out != tc.wantOut {
			t.Errorf("%s: out %q", tc.late, out)
		}
		if !strings.Contains(errs, tc.wantErr) || !strings.Contains(errs, "late 1, skipped 1") {
			t.Errorf("%s: stderr %q", tc.late, errs)
		}
	}
}

func TestConfigErrors(t *testing.T) {
	var o, e strings.Builder
	if run(strings.NewReader(""), &o, &e, config{}) == nil {
		t.Error("expected error without -field or -col")
	}
	if run(strings.NewReader(""), &o, &e, config{col: 1, late: "nope"}) == nil {
		t.Error("expected error for bad -late")
	}
}
