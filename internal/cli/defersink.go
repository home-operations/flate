package cli

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"sync"

	"github.com/home-operations/flate/internal/report"
	"github.com/home-operations/flate/internal/style"
)

// deferSink is a slog.Handler that holds back the chatter a failing run emits —
// the scattered Warn/Info lines (chiefly "resource orphaned") that otherwise
// interleave with, and bury, the final report — and hands them to the command
// to render in a quiet footer instead. Error records (panics) always pass
// through inline so a crash stays loud. Only `flate test` drains and renders
// the held-back notes; a data-producing verb leaves them buffered (and they're
// dropped at exit), so its output stays clean.
//
// When buffering is off the sink is transparent — every record passes straight
// to the inner handler, live. setLogLevel turns it off for an explicitly chosen
// --log-level: that's the user asking for logs, which must stream to stderr on
// every command, not vanish into a footer only `test` renders.
//
// One shared buffer backs every handler slog derives via WithAttrs/WithGroup, so
// attribute-scoped loggers buffer into the same place. drain() returns the
// collected notes (identical lines collapsed with a count) and stops buffering,
// so anything logged afterwards passes straight through.
type deferSink struct {
	inner slog.Handler
	buf   *noteBuffer
}

type noteBuffer struct {
	mu     sync.Mutex
	on     bool
	order  []string
	counts map[string]int
}

// newDeferSink wraps inner. When buffer is true it holds back Warn/Info chatter
// for `flate test` to render; when false it is transparent (live pass-through).
func newDeferSink(inner slog.Handler, buffer bool) *deferSink {
	return &deferSink{inner: inner, buf: &noteBuffer{on: buffer, counts: map[string]int{}}}
}

func (d *deferSink) Enabled(ctx context.Context, l slog.Level) bool { return d.inner.Enabled(ctx, l) }

func (d *deferSink) Handle(ctx context.Context, r slog.Record) error {
	d.buf.mu.Lock()
	buffering := d.buf.on
	d.buf.mu.Unlock()
	if !buffering || r.Level >= slog.LevelError {
		return d.inner.Handle(ctx, r)
	}
	text := formatRecord(r)
	d.buf.mu.Lock()
	if _, seen := d.buf.counts[text]; !seen {
		d.buf.order = append(d.buf.order, text)
	}
	d.buf.counts[text]++
	d.buf.mu.Unlock()
	return nil
}

func (d *deferSink) WithAttrs(as []slog.Attr) slog.Handler {
	return &deferSink{inner: d.inner.WithAttrs(as), buf: d.buf}
}

func (d *deferSink) WithGroup(name string) slog.Handler {
	return &deferSink{inner: d.inner.WithGroup(name), buf: d.buf}
}

// drain stops buffering and returns the collected notes in first-seen order.
// Idempotent: a second call returns nothing.
func (d *deferSink) drain() []report.Note {
	d.buf.mu.Lock()
	defer d.buf.mu.Unlock()
	d.buf.on = false
	notes := make([]report.Note, 0, len(d.buf.order))
	for _, text := range d.buf.order {
		notes = append(notes, report.Note{Text: text, Count: d.buf.counts[text]})
	}
	d.buf.order, d.buf.counts = nil, map[string]int{}
	return notes
}

// formatRecord renders a buffered record as a single compact line: the message
// followed by its attributes, each value truncated so a multi-line reason (e.g.
// an orphan's cascaded dependency chain) can't reintroduce the wall of text the
// footer exists to avoid.
func formatRecord(r slog.Record) string {
	var b strings.Builder
	b.WriteString(r.Message)
	r.Attrs(func(a slog.Attr) bool {
		fmt.Fprintf(&b, " %s=%s", a.Key, style.Truncate(oneLineValue(a.Value.String()), 80))
		return true
	})
	return b.String()
}

func oneLineValue(s string) string {
	if i := strings.IndexAny(s, "\r\n"); i >= 0 {
		return strings.TrimSpace(s[:i]) + " …"
	}
	return s
}
