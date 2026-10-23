package main

import (
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestOpenInputsJoinsFilesInOrder(t *testing.T) {
	dir := t.TempDir()
	a, b := filepath.Join(dir, "a.log"), filepath.Join(dir, "b.log")
	if err := os.WriteFile(a, []byte("2 x\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(b, []byte("1 y\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	in, closeAll, err := openInputs([]string{a, b}, strings.NewReader("unused"))
	if err != nil {
		t.Fatal(err)
	}
	defer closeAll()
	var o, e strings.Builder
	if err := run(in, &o, &e, config{col: 1, sep: "ws", delay: time.Minute, late: "drop"}); err != nil {
		t.Fatal(err)
	}
	if o.String() != "1 y\n2 x\n" {
		t.Fatalf("got %q", o.String())
	}
}

func TestOpenInputsStdinAndMissingFile(t *testing.T) {
	in, closeAll, err := openInputs(nil, strings.NewReader("hello"))
	if err != nil {
		t.Fatal(err)
	}
	closeAll()
	if data, _ := io.ReadAll(in); string(data) != "hello" {
		t.Fatalf("stdin not passed through: %q", data)
	}
	if _, _, err := openInputs([]string{filepath.Join(t.TempDir(), "missing.log")}, nil); err == nil {
		t.Fatal("expected an error for a missing file")
	}
}

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

func TestJSONNumericTimeAndEmptyLines(t *testing.T) {
	in := "\n{\"ts\":1700000002,\"m\":\"b\"}\n   \n{\"ts\":1700000001.5,\"m\":\"a\"}\n"
	out, _ := runString(t, in, config{field: "ts", delay: time.Minute, late: "drop"})
	want := "{\"ts\":1700000001.5,\"m\":\"a\"}\n{\"ts\":1700000002,\"m\":\"b\"}\n"
	if out != want {
		t.Fatalf("got %q", out)
	}
}

func TestBadJSONLinesAreSkippedAndCounted(t *testing.T) {
	in := "not json\n{\"other\":1}\n{\"ts\":true}\n{\"ts\":\"2026-01-01T00:00:01Z\"}\n"
	out, errs := runString(t, in, config{field: "ts", delay: time.Second, late: "drop", stats: true})
	if out != "{\"ts\":\"2026-01-01T00:00:01Z\"}\n" {
		t.Fatalf("out %q", out)
	}
	for _, want := range []string{"line 1 skipped", "no \"ts\" field", "is not a time", "skipped 3"} {
		if !strings.Contains(errs, want) {
			t.Errorf("stderr missing %q: %q", want, errs)
		}
	}
}

func TestColumnOutOfRangeAndQuotedTime(t *testing.T) {
	in := "a\n\"2026-01-01T00:00:02Z\",x\n\"2026-01-01T00:00:01Z\",y\n"
	out, errs := runString(t, in, config{col: 1, sep: ",", delay: time.Minute, late: "drop"})
	if out != "\"2026-01-01T00:00:01Z\",y\n\"2026-01-01T00:00:02Z\",x\n" {
		t.Fatalf("out %q", out)
	}
	if !strings.Contains(errs, "unrecognised time") {
		t.Errorf("stderr %q", errs)
	}
	_, errs = runString(t, "a\n", config{col: 3, sep: ",", delay: time.Second, late: "drop"})
	if !strings.Contains(errs, "only 1 columns") {
		t.Errorf("stderr %q", errs)
	}
}

func TestMaxBufferedReleasesEarly(t *testing.T) {
	in := "1 a\n2 b\n3 c\n4 d\n"
	out, errs := runString(t, in, config{col: 1, sep: "ws", delay: time.Hour, late: "drop", maxBuf: 2, stats: true})
	if out != in {
		t.Fatalf("out %q", out)
	}
	if !strings.Contains(errs, "peak held 2") && !strings.Contains(errs, "peak held 3") {
		t.Errorf("stderr %q", errs)
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
