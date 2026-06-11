// Package style is flate's terminal presentation layer: the glyphs, lipgloss
// color helpers, ANSI-aware truncation, and duration formatting shared by the
// CLI status bar and the `flate test` report, so both surfaces speak one
// vocabulary instead of hand-rolling escape codes and symbols.
//
// Color is gated per output writer: callers resolve ColorEnabled(w) once (a
// pipe, NO_COLOR, CLICOLOR=0, or a dumb terminal renders plain) and thread the
// bool through, keeping the renderers pure and deterministic for tests.
package style

import (
	"fmt"
	"io"
	"os"
	"time"

	"charm.land/lipgloss/v2"
	"github.com/charmbracelet/colorprofile"
	"github.com/charmbracelet/x/ansi"
)

// Status glyphs shared across surfaces: a passing/success mark, a failure mark,
// and a skipped/secondary dash. Pair with Pass/Fail/Skip for color.
const (
	GlyphPass    = "✓"
	GlyphFail    = "✗"
	GlyphSkip    = "‒"
	GlyphBlocked = "⊘"
)

// Semantic styles, by ANSI base color (0-7) so they downsample cleanly on
// 16-color terminals: 1 red, 2 green, 6 cyan; faint and bold carry no hue.
var (
	greenStyle = lipgloss.NewStyle().Foreground(lipgloss.Color("2"))
	redStyle   = lipgloss.NewStyle().Foreground(lipgloss.Color("1"))
	cyanStyle  = lipgloss.NewStyle().Foreground(lipgloss.Color("6"))
	boldStyle  = lipgloss.NewStyle().Bold(true)
	faintStyle = lipgloss.NewStyle().Faint(true)
)

// ColorEnabled reports whether w should receive ANSI styling. colorprofile.Detect
// honors NO_COLOR / CLICOLOR(_FORCE) and TTY-ness; anything at or below Ascii (a
// pipe, a redirect, a dumb terminal) renders plain.
func ColorEnabled(w io.Writer) bool {
	return colorprofile.Detect(w, os.Environ()) > colorprofile.Ascii
}

// render applies st when color is on; otherwise returns s untouched so plain
// output carries no escape codes.
func render(st lipgloss.Style, s string, color bool) string {
	if !color {
		return s
	}
	return st.Render(s)
}

// Pass renders s as a success (green) when color is on, else verbatim.
func Pass(s string, color bool) string { return render(greenStyle, s, color) }

// Fail renders s as a failure (red) when color is on, else verbatim.
func Fail(s string, color bool) string { return render(redStyle, s, color) }

// Skip renders s as skipped/secondary (faint) when color is on, else verbatim.
func Skip(s string, color bool) string { return render(faintStyle, s, color) }

// Dim renders s faint when color is on, else verbatim.
func Dim(s string, color bool) string { return render(faintStyle, s, color) }

// Bold renders s bold when color is on, else verbatim.
func Bold(s string, color bool) string { return render(boldStyle, s, color) }

// Cyan renders s cyan when color is on, else verbatim.
func Cyan(s string, color bool) string { return render(cyanStyle, s, color) }

// Truncate shortens s to width visible columns, appending an ellipsis on
// overflow. It is ANSI- and wide-rune-aware (escape sequences and CJK width are
// counted correctly), so a styled line truncates by what's actually shown.
func Truncate(s string, width int) string {
	if width < 1 {
		width = 1
	}
	if ansi.StringWidth(s) <= width {
		return s
	}
	return ansi.Truncate(s, width, "…")
}

// Elapsed renders a duration compactly for live and summary timing: tenths of a
// second under a minute, m+ss above it.
func Elapsed(d time.Duration) string {
	if d < time.Minute {
		return fmt.Sprintf("%.1fs", d.Seconds())
	}
	return fmt.Sprintf("%dm%02ds", int(d/time.Minute), int(d%time.Minute/time.Second))
}
