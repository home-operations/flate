package cli

import (
	"context"
	"log/slog"
	"testing"
)

type captureHandler struct{ msgs *[]string }

func (h captureHandler) Enabled(context.Context, slog.Level) bool { return true }
func (h captureHandler) Handle(_ context.Context, r slog.Record) error {
	*h.msgs = append(*h.msgs, r.Message)
	return nil
}
func (h captureHandler) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h captureHandler) WithGroup(string) slog.Handler      { return h }

// TestDeferSink_BuffersWarnPassesError pins the footer mechanism: Warn/Info are
// held back (and identical lines collapse with a count) while Error passes
// through inline; after drain, buffering is off so later logs flow normally.
func TestDeferSink_BuffersWarnPassesError(t *testing.T) {
	var inline []string
	sink := newDeferSink(captureHandler{&inline}, true)
	log := slog.New(sink)

	log.Warn("resource orphaned", "id", "a")
	log.Warn("resource orphaned", "id", "a") // identical → collapses
	log.Info("note")
	log.Error("boom") // inline

	if len(inline) != 1 || inline[0] != "boom" {
		t.Fatalf("only Error should pass through inline, got %v", inline)
	}

	notes := sink.drain()
	if len(notes) != 2 {
		t.Fatalf("drain = %+v, want 2 distinct notes", notes)
	}
	if notes[0].Count != 2 {
		t.Errorf("identical warns should collapse with a count: %+v", notes[0])
	}

	log.Warn("after-drain")
	if len(inline) != 2 || inline[1] != "after-drain" {
		t.Errorf("post-drain logs should pass through inline, got %v", inline)
	}
}
