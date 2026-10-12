package cli

import (
	"context"
	"log/slog"
	"sync"
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/report"
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
	assert.Diff(t, notes, []report.Note{
		{Text: "note", Count: 1},
		{Text: "resource orphaned id=a", Count: 2},
	})

	log.Warn("after-drain")
	if len(inline) != 2 || inline[1] != "after-drain" {
		t.Errorf("post-drain logs should pass through inline, got %v", inline)
	}
}

func TestDeferSink_DrainOrderIsDeterministic(t *testing.T) {
	want := make([]report.Note, 0, 8)
	for _, text := range []string{"a", "b", "c", "d", "e", "f", "g", "h"} {
		want = append(want, report.Note{Text: text, Count: 1})
	}
	for range 50 {
		var inline []string
		sink := newDeferSink(captureHandler{&inline}, true)
		log := slog.New(sink)
		var wg sync.WaitGroup
		for _, note := range want {
			wg.Go(func() {
				log.WarnContext(t.Context(), note.Text)
			})
		}
		wg.Wait()
		assert.Diff(t, sink.drain(), want)
	}
}
