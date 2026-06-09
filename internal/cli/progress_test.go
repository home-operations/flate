package cli

import (
	"bytes"
	"strings"
	"testing"
)

// TestStderrRouter_PaintWriteRepaint: painting a frame then writing a
// permanent line erases the bar, prints the line, and repaints the bar beneath
// it — so scroll-back (slog, failure lines) never tangles with the sticky bar.
func TestStderrRouter_PaintWriteRepaint(t *testing.T) {
	var buf bytes.Buffer
	r := newStderrRouter(&buf)

	r.Paint("BAR")
	if got := buf.String(); got != eraseLine+"BAR" {
		t.Fatalf("after Paint = %q, want erase+frame", got)
	}
	buf.Reset()

	n, err := r.Write([]byte("log line\n"))
	if err != nil || n != len("log line\n") {
		t.Fatalf("Write n=%d err=%v, want full payload count", n, err)
	}
	// erase the old bar, emit the line, repaint the bar below it.
	if got, want := buf.String(), eraseLine+"log line\n"+"BAR"; got != want {
		t.Fatalf("Write sequence = %q, want %q", got, want)
	}
}

// TestStderrRouter_WritePassthrough: with no bar painted, Write is a plain
// passthrough (no stray control bytes) — the e2e/non-TTY path.
func TestStderrRouter_WritePassthrough(t *testing.T) {
	var buf bytes.Buffer
	r := newStderrRouter(&buf)
	if _, err := r.Write([]byte("hello\n")); err != nil {
		t.Fatal(err)
	}
	if got := buf.String(); got != "hello\n" {
		t.Fatalf("passthrough Write = %q, want bare line", got)
	}
}

// TestStderrRouter_Stop erases the bar and forgets it: a later Write is a clean
// passthrough again.
func TestStderrRouter_Stop(t *testing.T) {
	var buf bytes.Buffer
	r := newStderrRouter(&buf)
	r.Paint("BAR")
	buf.Reset()

	r.Stop()
	if got := buf.String(); got != eraseLine {
		t.Fatalf("Stop = %q, want lone erase", got)
	}
	buf.Reset()
	_, _ = r.Write([]byte("after\n"))
	if got := buf.String(); got != "after\n" {
		t.Fatalf("post-Stop Write = %q, want bare line (bar forgotten)", got)
	}
}

func TestProgressDetail_Truncation(t *testing.T) {
	if got := progressDetail("short"); got != "short" {
		t.Errorf("short passthrough = %q", got)
	}
	if got := progressDetail("first line\nsecond"); got != "first line" {
		t.Errorf("multi-line not reduced to first line: %q", got)
	}
	long := strings.Repeat("x", 200)
	if got := progressDetail(long); len([]rune(got)) != 120 || !strings.HasSuffix(got, "…") {
		t.Errorf("long message not capped at 120 runes with ellipsis: len=%d", len([]rune(got)))
	}
}

// TestWriterIsTTY_NonFile: buffers (the e2e harness, pipes-as-buffers) are
// never TTYs, so the bar stays off without a flag.
func TestWriterIsTTY_NonFile(t *testing.T) {
	if writerIsTTY(&bytes.Buffer{}) {
		t.Error("bytes.Buffer reported as a TTY")
	}
}
