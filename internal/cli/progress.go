package cli

import (
	"io"
	"os"
	"strings"
	"sync"

	tea "charm.land/bubbletea/v2"

	"github.com/home-operations/flate/internal/style"
)

// barSink is the slog routing adapter active while the live status bar runs; nil
// disables the bar entirely (pipes, CI, the in-process e2e harness,
// --no-progress, or a --stream run that shares the terminal with stdout). Set by
// the root command's PersistentPreRunE when progressBarEnabled allows it, and
// pointed at by slog so log records interleave cleanly above the bar. stdout is
// never touched — rendered output stays byte-deterministic.
var barSink *barWriter

// barWriter routes slog output around the Bubble Tea status bar. With a Program
// attached (the bar is live), each record prints above the sticky frame via
// Program.Println; without one, it writes straight through to the underlying
// stderr. It is the io.Writer the root command points slog at, so log lines
// never corrupt the bar.
type barWriter struct {
	out   io.Writer // underlying stderr (a *os.File TTY)
	color bool      // whether the bar/report should emit ANSI on this stderr

	mu   sync.Mutex
	prog *tea.Program
}

func newBarWriter(out io.Writer) *barWriter {
	return &barWriter{out: out, color: style.ColorEnabled(out)}
}

// setProgram attaches (or, with nil, detaches) the live Bubble Tea program so
// Write knows whether to route records above the frame or straight to stderr.
func (w *barWriter) setProgram(p *tea.Program) {
	w.mu.Lock()
	w.prog = p
	w.mu.Unlock()
}

func (w *barWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	prog := w.prog
	w.mu.Unlock()
	if prog != nil {
		// Println places the line above the live frame and owns the newline.
		prog.Println(strings.TrimRight(string(p), "\n"))
		return len(p), nil
	}
	return w.out.Write(p)
}

// writerIsTTY reports whether w is a character device (an interactive terminal).
// Buffers and pipes — CI, redirections, the e2e harness's bytes.Buffer — are
// not, so the bar stays off there without a flag.
func writerIsTTY(w io.Writer) bool {
	f, ok := w.(*os.File)
	if !ok {
		return false
	}
	fi, err := f.Stat()
	return err == nil && fi.Mode()&os.ModeCharDevice != 0
}

// progressBarEnabled reports whether the live status bar should paint. It is
// off when --no-progress is set, when stderr isn't an interactive terminal
// (pipes/CI/e2e buffers), or when --stream shares that terminal with stdout:
// the bar repaints a sticky stderr line, while --stream writes raw YAML to
// stdout outside the bar's renderer, so on one terminal the two interleave and
// corrupt each other — the stream wins. A --stream run whose stdout is
// redirected (file/pipe) keeps the bar: it paints cleanly to stderr with no
// collision.
func progressBarEnabled(noProgress, stream, stdoutTTY, stderrTTY bool) bool {
	if noProgress || !stderrTTY {
		return false
	}
	return !stream || !stdoutTTY
}
