package cli

import (
	"io"
	"os"
	"strings"
	"sync"
)

// stderrSink is the terminal router live progress is painted through. Set by
// the root command's PersistentPreRunE when stderr is an interactive terminal
// and --no-progress is unset; nil disables the status bar entirely (pipes, CI,
// the in-process e2e harness). When set, slog is also routed through it so log
// lines interleave cleanly above the bar. stdout is never touched — the
// rendered output stays byte-deterministic.
var stderrSink *stderrRouter

// writerIsTTY reports whether w is a character device (an interactive
// terminal). Buffers and pipes — CI, redirections, the e2e harness's
// bytes.Buffer — are not, so the bar stays off there without a flag.
func writerIsTTY(w io.Writer) bool {
	f, ok := w.(*os.File)
	if !ok {
		return false
	}
	fi, err := f.Stat()
	return err == nil && fi.Mode()&os.ModeCharDevice != 0
}

// stderrRouter multiplexes a sticky single-line status bar with the ordinary
// scroll-back stream (slog records, failure lines, the final summary) on one
// terminal. It is the single point that knows whether a bar frame is currently
// painted, so every permanent write can erase the bar, print, and repaint it
// beneath — keeping the bar pinned to the bottom line without ever tangling
// with log output.
//
//	Paint(frame) — repaint the sticky bar in place (the spinner ticker).
//	Write(p)     — emit permanent output above the bar (slog + bar lines).
//	Stop()       — erase the bar for good (run teardown).
type stderrRouter struct {
	w  io.Writer
	mu sync.Mutex
	// bar is the frame currently painted on the bottom line ("" = none).
	bar string
}

func newStderrRouter(w io.Writer) *stderrRouter { return &stderrRouter{w: w} }

// Write emits p as permanent scroll-back, repainting the sticky bar beneath it
// so the bar never scrolls away. slog, the bar's own failure lines, and the
// final summary all flow through here. n counts bytes of p (the erase/repaint
// control bytes are bookkeeping, not the caller's payload).
func (r *stderrRouter) Write(p []byte) (int, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.bar != "" {
		_, _ = io.WriteString(r.w, eraseLine)
	}
	n, err := r.w.Write(p)
	if r.bar != "" {
		_, _ = io.WriteString(r.w, r.bar)
	}
	return n, err
}

// Paint repaints the sticky bar in place (carriage-return + clear-to-EOL, then
// the frame, no trailing newline so the line stays sticky).
func (r *stderrRouter) Paint(frame string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	_, _ = io.WriteString(r.w, eraseLine+frame)
	r.bar = frame
}

// Stop erases the bar and forgets it, leaving the cursor at column 0 of a
// clean line for the caller's final output.
func (r *stderrRouter) Stop() {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.bar != "" {
		_, _ = io.WriteString(r.w, eraseLine)
		r.bar = ""
	}
}

// width reports the terminal's column count for frame truncation, falling back
// to 80 when the underlying writer isn't a sized terminal.
func (r *stderrRouter) width() int { return terminalWidth(r.w) }

// eraseLine returns the cursor to column 0 and clears to end of line.
const eraseLine = "\r\x1b[K"

// progressDetail reduces a status message to a one-line hint: first line only,
// capped at 120 runes. Full failure detail belongs to the final report, not
// the live ticker.
func progressDetail(msg string) string {
	if i := strings.IndexByte(msg, '\n'); i >= 0 {
		msg = msg[:i]
	}
	if r := []rune(msg); len(r) > 120 {
		return string(r[:119]) + "…"
	}
	return msg
}
