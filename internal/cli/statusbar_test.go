package cli

import (
	"bytes"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

var ansiRE = regexp.MustCompile("\x1b\\[[0-9;]*m")

// visibleLen counts the runes a frame actually occupies on screen — ANSI color
// codes are zero-width and must not count against the terminal budget.
func visibleLen(s string) int { return len([]rune(ansiRE.ReplaceAllString(s, ""))) }

func barID(name string) manifest.NamedResource {
	return manifest.NamedResource{Kind: manifest.KindKustomization, Namespace: "ns", Name: name}
}

// TestRenderFrame_FitsWidth: the frame never exceeds the terminal width at any
// size, so it can repaint in place without wrapping (a wrap breaks \r repaint).
func TestRenderFrame_FitsWidth(t *testing.T) {
	names := []string{"alpha", "bravo", "charlie", "delta", "echo"}
	for _, width := range []int{1, 3, 8, 20, 40, 80, 200} {
		for _, color := range []bool{false, true} {
			got := renderFrame("⠹", 42, 86, names, 12300*time.Millisecond, width, color)
			if vl := visibleLen(got); vl > width {
				t.Errorf("width=%d color=%v: visible len %d > width (%q)", width, color, vl, got)
			}
		}
	}
}

// TestRenderFrame_PlainNoColor: with color off the frame carries no escape
// codes and shows the counts, spinner, names, and elapsed.
func TestRenderFrame_PlainNoColor(t *testing.T) {
	got := renderFrame("⠹", 3, 10, []string{"plex"}, 1500*time.Millisecond, 80, false)
	if strings.Contains(got, "\x1b") {
		t.Errorf("color=false leaked an escape code: %q", got)
	}
	for _, want := range []string{"⠹", "[3/10]", "plex", "1.5s"} {
		if !strings.Contains(got, want) {
			t.Errorf("frame %q missing %q", got, want)
		}
	}
}

// TestRenderFrame_ColorInvisibleToWidth: the same logical content fits the same
// width whether or not it is colorized — codes don't consume columns.
func TestRenderFrame_ColorInvisibleToWidth(t *testing.T) {
	plain := renderFrame("⠹", 3, 10, []string{"plex"}, time.Second, 40, false)
	color := renderFrame("⠹", 3, 10, []string{"plex"}, time.Second, 40, true)
	if visibleLen(plain) != visibleLen(color) {
		t.Errorf("visible lengths differ: plain=%d color=%d", visibleLen(plain), visibleLen(color))
	}
	if !strings.Contains(color, ansiCyan) {
		t.Errorf("color frame missing the cyan spinner code: %q", color)
	}
}

func TestSummarizeInflight(t *testing.T) {
	cases := []struct {
		names []string
		max   int
		want  string
	}{
		{nil, 2, ""},
		{[]string{"a"}, 2, "a"},
		{[]string{"a", "b"}, 2, "a, b"},
		{[]string{"a", "b", "c"}, 2, "a, b +1"},
		{[]string{"a", "b", "c", "d", "e"}, 2, "a, b +3"},
	}
	for _, c := range cases {
		if got := summarizeInflight(c.names, c.max); got != c.want {
			t.Errorf("summarizeInflight(%v, %d) = %q, want %q", c.names, c.max, got, c.want)
		}
	}
}

func TestFmtElapsed(t *testing.T) {
	cases := map[time.Duration]string{
		0:                        "0.0s",
		1500 * time.Millisecond:  "1.5s",
		59900 * time.Millisecond: "59.9s",
		90 * time.Second:         "1m30s",
		3725 * time.Second:       "62m05s",
	}
	for d, want := range cases {
		if got := fmtElapsed(d); got != want {
			t.Errorf("fmtElapsed(%v) = %q, want %q", d, got, want)
		}
	}
}

// TestStatusBar_CountsAndFailureLine drives onStatus through realistic events:
// counters track unique terminal ids, but the bar writes nothing permanent —
// it is a pure loading indicator.
func TestStatusBar_CountsNoPermanentOutput(t *testing.T) {
	var buf bytes.Buffer
	bar := newStatusBar(newStderrRouter(&buf))
	bar.color = false

	a, b, s := barID("a"), barID("b"), barID("suspended")
	bar.onStatus(a, store.StatusInfo{Status: store.StatusPending})
	bar.onStatus(b, store.StatusInfo{Status: store.StatusPending})
	bar.onStatus(a, store.StatusInfo{Status: store.StatusReady})
	bar.onStatus(b, store.StatusInfo{Status: store.StatusFailed, Message: "boom: chart not found"})
	bar.onStatus(s, store.StatusInfo{Status: store.StatusReady, Message: store.MsgSuspended})

	// Ready, Failed, and skipped all count as "done loading" — and none of
	// them write a permanent line.
	if buf.Len() != 0 {
		t.Fatalf("bar wrote permanent output (it must stay silent): %q", buf.String())
	}
	if bar.done != 3 || bar.total != 3 {
		t.Errorf("counts: done=%d total=%d; want 3/3", bar.done, bar.total)
	}
	if got := bar.inflightLabels(); len(got) != 0 {
		t.Errorf("all resources terminal but still in-flight: %v", got)
	}
}

// TestStatusBar_DedupAndInflight: an idempotent terminal re-write doesn't
// double-count, and a resource drops out of the in-flight set the moment it
// reaches a terminal status.
func TestStatusBar_DedupAndInflight(t *testing.T) {
	bar := newStatusBar(newStderrRouter(&bytes.Buffer{}))
	bar.color = false

	a := barID("a")
	bar.onStatus(a, store.StatusInfo{Status: store.StatusPending})
	if got := bar.inflightLabels(); len(got) != 1 || got[0] != "a" {
		t.Fatalf("in-flight after Pending = %v, want [a]", got)
	}
	bar.onStatus(a, store.StatusInfo{Status: store.StatusReady})
	bar.onStatus(a, store.StatusInfo{Status: store.StatusReady}) // duplicate
	if bar.done != 1 {
		t.Errorf("duplicate Ready double-counted: done=%d, want 1", bar.done)
	}
	if got := bar.inflightLabels(); len(got) != 0 {
		t.Errorf("terminal resource still in-flight: %v", got)
	}
}

// TestStatusBar_StartFinish: the live lifecycle paints at least one frame and
// then vanishes completely — finish erases the bar and leaves no trace
// (exercises the ticker + router under the race detector).
func TestStatusBar_StartFinish(t *testing.T) {
	var buf bytes.Buffer
	bar := newStatusBar(newStderrRouter(&buf))
	bar.color = false

	bar.onStatus(barID("a"), store.StatusInfo{Status: store.StatusPending})
	bar.start()
	bar.onStatus(barID("a"), store.StatusInfo{Status: store.StatusReady})
	time.Sleep(250 * time.Millisecond) // let the ticker paint a few frames
	bar.finish()

	out := buf.String()
	if !strings.Contains(out, eraseLine) {
		t.Errorf("no in-place repaint observed: %q", out)
	}
	// Pure loading indicator: nothing permanent survives the run.
	if strings.Contains(out, "flate:") {
		t.Errorf("bar left a summary/trace behind; it must vanish: %q", out)
	}
	// finish ends with a bare erase, leaving the cursor on a clean empty line.
	if !strings.HasSuffix(out, eraseLine) {
		t.Errorf("bar not erased on finish; want trailing erase: %q", out)
	}
}
