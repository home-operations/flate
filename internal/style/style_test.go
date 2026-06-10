package style

import (
	"bytes"
	"strings"
	"testing"

	"github.com/charmbracelet/x/ansi"
)

// TestColorEnabled_NonTTY: a buffer (pipe/CI/e2e sink) is never a terminal, so
// styling stays off without a flag.
func TestColorEnabled_NonTTY(t *testing.T) {
	if ColorEnabled(&bytes.Buffer{}) {
		t.Error("bytes.Buffer reported as color-capable")
	}
}

// TestRenderers_ColorGating: color=false is a verbatim passthrough (no escapes);
// color=true wraps the text in ANSI while preserving it.
func TestRenderers_ColorGating(t *testing.T) {
	const s = "hello"
	for _, f := range []func(string, bool) string{Pass, Fail, Skip, Dim, Bold, Cyan} {
		if got := f(s, false); got != s {
			t.Errorf("plain render = %q, want %q", got, s)
		}
		colored := f(s, true)
		if !strings.Contains(colored, "\x1b") {
			t.Errorf("colored render emitted no ANSI: %q", colored)
		}
		if !strings.Contains(colored, s) {
			t.Errorf("colored render dropped the text: %q", colored)
		}
	}
}

// TestTruncate: width-aware, ANSI-aware truncation with an ellipsis on overflow.
func TestTruncate(t *testing.T) {
	if got := Truncate("hello world", 100); got != "hello world" {
		t.Errorf("under-width truncate should be a no-op: %q", got)
	}
	if got := Truncate("hello world", 5); ansi.StringWidth(got) > 5 || !strings.Contains(got, "…") {
		t.Errorf("overflow truncate = %q (width %d), want ≤5 cols with ellipsis", got, ansi.StringWidth(got))
	}
	// A fully-styled string truncates by VISIBLE width, ignoring escape bytes.
	if w := ansi.StringWidth(Truncate(Pass("hello world", true), 5)); w > 5 {
		t.Errorf("styled truncate visible width = %d, want ≤5", w)
	}
}
