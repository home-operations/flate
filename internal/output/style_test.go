package output

import (
	"bytes"
	"strings"
	"testing"
	"time"

	"github.com/charmbracelet/x/ansi"
	"github.com/home-operations/flate/internal/assert"
)

func TestElapsed_CompactDuration(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		d    time.Duration
		want string
	}{
		{name: "zero", want: "0.0s"},
		{name: "fractional_seconds", d: 1500 * time.Millisecond, want: "1.5s"},
		{name: "under_minute", d: 59900 * time.Millisecond, want: "59.9s"},
		{name: "minutes", d: 90 * time.Second, want: "1m30s"},
		{name: "over_hour", d: 3725 * time.Second, want: "62m05s"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, Elapsed(tc.d), tc.want)
		})
	}
}

func TestColorEnabled_NonTTY(t *testing.T) {
	t.Parallel()
	if ColorEnabled(&bytes.Buffer{}) {
		t.Error("bytes.Buffer reported as color-capable")
	}
}

func TestRenderers_ColorGating(t *testing.T) {
	t.Parallel()
	const s = "hello"
	cases := []struct {
		name   string
		render func(string, bool) string
	}{
		{name: "pass", render: Pass},
		{name: "fail", render: Fail},
		{name: "warn", render: Warn},
		{name: "dim", render: Dim},
		{name: "bold", render: Bold},
		{name: "cyan", render: Cyan},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.render(s, false), s)
			colored := tc.render(s, true)
			if !strings.Contains(colored, "\x1b") {
				t.Errorf("colored render emitted no ANSI: %q", colored)
			}
			assert.Equal(t, ansi.Strip(colored), s)
		})
	}
}

func TestTruncate_VisibleWidth(t *testing.T) {
	t.Parallel()
	if got := Truncate("hello world", 100); got != "hello world" {
		t.Errorf("under-width truncate = %q, want unchanged text", got)
	}
	if got := Truncate("hello world", 5); ansi.StringWidth(got) > 5 || !strings.Contains(got, "…") {
		t.Errorf("overflow truncate = %q (width %d), want ≤5 cols with ellipsis", got, ansi.StringWidth(got))
	}
	if w := ansi.StringWidth(Truncate(Pass("hello world", true), 5)); w > 5 {
		t.Errorf("styled truncate visible width = %d, want ≤5", w)
	}
}
