package cli

import (
	"bytes"
	"fmt"
	"os"
	"slices"
	"sync"
	"testing"
)

// TestCaptureStderr verifies the stray-fd capture: lines written straight to
// os.Stderr (as kustomize does for its deprecation warnings) are delivered to
// emit one per line — including a final line with no trailing newline, flushed
// at EOF — and os.Stderr is restored afterward. Not parallel: it swaps the
// process-wide os.Stderr.
func TestCaptureStderr(t *testing.T) {
	orig := os.Stderr
	var mu sync.Mutex
	var got []string
	restore := captureStderr(func(line string) {
		mu.Lock()
		got = append(got, line)
		mu.Unlock()
	})
	fmt.Fprintln(os.Stderr, "# Warning: 'commonLabels' is deprecated.")
	fmt.Fprint(os.Stderr, "tail-without-newline")
	restore()

	if os.Stderr != orig {
		t.Error("captureStderr did not restore os.Stderr")
	}
	mu.Lock()
	defer mu.Unlock()
	want := []string{"# Warning: 'commonLabels' is deprecated.", "tail-without-newline"}
	if !slices.Equal(got, want) {
		t.Errorf("captured lines = %q, want %q", got, want)
	}
}

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

// TestQueryFilterFile: the bar's output wrapper strips Bubble Tea's startup
// terminal probes (mode 2026/2027 DECRQM and the renderer's Kitty keyboard
// query) — whose replies nothing reads, since the program runs input-less —
// while passing every other byte through and reporting the full input length
// as written.
func TestQueryFilterFile(t *testing.T) {
	f, err := os.CreateTemp(t.TempDir(), "bar")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	w := &queryFilterFile{File: f}

	// The Kitty *set* sequence (\x1b[=0;1u) rides in the same frame flush as
	// the query (\x1b[?u) and must survive the strip.
	in := []byte("\x1b[?2026$p\x1b[?2027$pframe \x1b[?2026h\x1b[=0;1u\x1b[?u+content")
	n, err := w.Write(in)
	if err != nil {
		t.Fatal(err)
	}
	if n != len(in) {
		t.Errorf("Write returned n=%d, want full length %d", n, len(in))
	}
	// A flush that is nothing but probes writes no bytes at all.
	if _, err := w.Write([]byte("\x1b[?2026$p\x1b[?u")); err != nil {
		t.Fatal(err)
	}

	got, err := os.ReadFile(f.Name())
	if err != nil {
		t.Fatal(err)
	}
	// The set sequences (2026h, =0;1u) and frame text survive; only the
	// query probes are dropped.
	if want := "frame \x1b[?2026h\x1b[=0;1u+content"; string(got) != want {
		t.Errorf("filtered output = %q, want %q", got, want)
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
