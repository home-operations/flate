package cli

import (
	"bytes"
	"strings"
	"testing"

	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

func progressID(name string) manifest.NamedResource {
	return manifest.NamedResource{Kind: manifest.KindKustomization, Namespace: "ns", Name: name}
}

// TestProgressReporter_TerminalLines drives the reporter through the store's
// real OnStatus path: Pending arrivals print nothing, terminal statuses print
// exactly once each, and the [done/total] counters track unique ids.
func TestProgressReporter_TerminalLines(t *testing.T) {
	var buf bytes.Buffer
	s := store.New()
	defer newProgressReporter(&buf).attach(s)()

	a, b := progressID("a"), progressID("b")
	s.UpdateStatus(a, store.StatusPending, "resolving dependencies")
	if buf.Len() != 0 {
		t.Fatalf("Pending printed a line: %q", buf.String())
	}
	s.UpdateStatus(b, store.StatusPending, "rendering")
	s.UpdateStatus(a, store.StatusReady, "")
	s.UpdateStatus(b, store.StatusFailed, "boom: chart not found\nsecond line of detail")

	lines := strings.Split(strings.TrimSpace(buf.String()), "\n")
	if len(lines) != 2 {
		t.Fatalf("got %d lines, want 2:\n%s", len(lines), buf.String())
	}
	if !strings.HasPrefix(lines[0], "[1/2] ✓ Kustomization/ns/a") {
		t.Errorf("ready line = %q, want [1/2] ✓ prefix", lines[0])
	}
	if !strings.HasPrefix(lines[1], "[2/2] ✗ Kustomization/ns/b") ||
		!strings.Contains(lines[1], "boom: chart not found") {
		t.Errorf("failed line = %q, want [2/2] ✗ with first-line reason", lines[1])
	}
	if strings.Contains(lines[1], "second line") {
		t.Errorf("failed line leaked past the first message line: %q", lines[1])
	}
}

// TestProgressReporter_DedupAndFlip: an idempotent terminal re-write is
// suppressed; a genuine Ready→Failed flip prints a fresh line without
// inflating the done counter.
func TestProgressReporter_DedupAndFlip(t *testing.T) {
	var buf bytes.Buffer
	p := newProgressReporter(&buf)

	id := progressID("a")
	p.onStatus(id, store.StatusInfo{Status: store.StatusReady})
	p.onStatus(id, store.StatusInfo{Status: store.StatusReady}) // duplicate — suppressed
	if got := strings.Count(buf.String(), "\n"); got != 1 {
		t.Fatalf("duplicate Ready printed %d lines, want 1:\n%s", got, buf.String())
	}
	p.onStatus(id, store.StatusInfo{Status: store.StatusFailed, Message: "late failure"})
	lines := strings.Split(strings.TrimSpace(buf.String()), "\n")
	if len(lines) != 2 || !strings.HasPrefix(lines[1], "[1/1] ✗") {
		t.Fatalf("flip line = %v, want a second [1/1] ✗ line", lines)
	}
}

// TestProgressReporter_SkippedVariants: suspended/unchanged/soft-skip Ready
// statuses print the – mark with the message as the hint.
func TestProgressReporter_SkippedVariants(t *testing.T) {
	var buf bytes.Buffer
	p := newProgressReporter(&buf)

	p.onStatus(progressID("s"), store.StatusInfo{Status: store.StatusReady, Message: store.MsgSuspended})
	p.onStatus(progressID("u"), store.StatusInfo{Status: store.StatusReady, Message: store.MsgUnchanged})
	p.onStatus(progressID("k"), store.StatusInfo{Status: store.StatusReady, Message: store.SkippedPrefix + " missing secret"})

	out := buf.String()
	if got := strings.Count(out, "– "); got != 3 {
		t.Fatalf("want 3 skipped marks, got %d:\n%s", got, out)
	}
	for _, want := range []string{"(suspended)", "(unchanged)", "(skipped: missing secret)"} {
		if !strings.Contains(out, want) {
			t.Errorf("missing %q in:\n%s", want, out)
		}
	}
}

func TestProgressDetail_Truncation(t *testing.T) {
	if got := progressDetail("short"); got != "short" {
		t.Errorf("short passthrough = %q", got)
	}
	long := strings.Repeat("x", 200)
	if got := progressDetail(long); len([]rune(got)) != 120 || !strings.HasSuffix(got, "…") {
		t.Errorf("long message not capped at 120 runes with ellipsis: len=%d", len([]rune(got)))
	}
}

// TestWriterIsTTY_NonFile: buffers (the e2e harness, pipes-as-buffers) are
// never TTYs, so progress stays off without a flag.
func TestWriterIsTTY_NonFile(t *testing.T) {
	if writerIsTTY(&bytes.Buffer{}) {
		t.Error("bytes.Buffer reported as a TTY")
	}
}
