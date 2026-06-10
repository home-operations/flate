package cli

import (
	"bytes"
	"testing"
)

// TestBarWriter_NoProgramWritesThrough: with no Bubble Tea program attached, the
// slog adapter is a plain passthrough to the underlying stderr (the non-TTY /
// pre-bar / post-bar path) — no stray control bytes.
func TestBarWriter_NoProgramWritesThrough(t *testing.T) {
	var buf bytes.Buffer
	w := &barWriter{out: &buf}
	if _, err := w.Write([]byte("hello\n")); err != nil {
		t.Fatal(err)
	}
	if got := buf.String(); got != "hello\n" {
		t.Errorf("passthrough Write = %q, want bare line", got)
	}
}

// TestWriterIsTTY_NonFile: buffers (the e2e harness, pipes-as-buffers) are
// never TTYs, so the bar stays off without a flag.
func TestWriterIsTTY_NonFile(t *testing.T) {
	if writerIsTTY(&bytes.Buffer{}) {
		t.Error("bytes.Buffer reported as a TTY")
	}
}

// TestProgressBarEnabled covers the bar's on/off gate, including the --stream
// collision: a sticky stderr bar can't coexist with raw streamed stdout on the
// same terminal, so --stream suppresses the bar there but keeps it when stdout
// is redirected.
func TestProgressBarEnabled(t *testing.T) {
	cases := []struct {
		name                                     string
		noProgress, stream, stdoutTTY, stderrTTY bool
		want                                     bool
	}{
		{"plain interactive", false, false, true, true, true},
		{"stderr not a tty (pipe/CI)", false, false, true, false, false},
		{"--no-progress", true, false, true, true, false},
		{"stream sharing the terminal with stdout", false, true, true, true, false},
		{"stream with stdout redirected keeps the bar", false, true, false, true, true},
		{"stream but stderr not a tty", false, true, true, false, false},
		{"--no-progress wins over a redirected stream", true, true, false, true, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := progressBarEnabled(c.noProgress, c.stream, c.stdoutTTY, c.stderrTTY); got != c.want {
				t.Errorf("progressBarEnabled(noProgress=%v, stream=%v, stdoutTTY=%v, stderrTTY=%v) = %v, want %v",
					c.noProgress, c.stream, c.stdoutTTY, c.stderrTTY, got, c.want)
			}
		})
	}
}
