package cli

import (
	"bytes"
	"log"
	"log/slog"
	"os"
	"strings"
	"testing"
)

// TestSetLogLevel_StdlibLogDetachedFromNotes pins the determinism fix: Go's
// slog↔log bridge (installed by slog.SetDefault) would otherwise route a
// dependency's stdlib log.Printf — chiefly Helm's values-coalesce "destination
// … is a table" warnings — into the notes footer. Those fire only when a chart
// actually renders, so on a render-cache HIT they vanish, making the footer
// differ between otherwise-identical runs (looks like a race). At non-debug
// levels the stdlib logger is detached, so dependency chatter never reaches
// notes; flate's own slog diagnostics still do. At debug it's reattached for
// troubleshooting.
func TestSetLogLevel_StdlibLogDetachedFromNotes(t *testing.T) {
	saved := slog.Default()
	t.Cleanup(func() { slog.SetDefault(saved); log.SetOutput(os.Stderr) })

	var sink bytes.Buffer
	if err := setLogLevel("warn", &sink); err != nil {
		t.Fatalf("setLogLevel(warn): %v", err)
	}
	log.Printf("warning: destination for x is a table. Ignoring non-table value") // dependency stdlib log
	slog.Warn("resource orphaned", "id", "x")                                     // flate's own slog

	var notes strings.Builder
	for _, n := range drainLogNotes() {
		notes.WriteString(n.Text)
		notes.WriteByte('\n')
	}
	if got := notes.String(); strings.Contains(got, "is a table") {
		t.Errorf("dependency stdlib log must NOT reach the notes footer at non-debug:\n%s", got)
	}
	if got := notes.String(); !strings.Contains(got, "resource orphaned") {
		t.Errorf("flate's own slog Warn must still be captured as a note:\n%s", got)
	}

	// At debug the stdlib logger is reattached to the sink (visible for
	// troubleshooting) rather than discarded.
	var dbg bytes.Buffer
	if err := setLogLevel("debug", &dbg); err != nil {
		t.Fatalf("setLogLevel(debug): %v", err)
	}
	log.Printf("helm-coalesce-debug-line")
	if !strings.Contains(dbg.String(), "helm-coalesce-debug-line") {
		t.Errorf("at --log-level debug, stdlib log should reach the sink writer; got %q", dbg.String())
	}
}
