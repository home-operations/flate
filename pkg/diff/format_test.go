package diff

import "testing"

// TestBodyStyle pins that the aggregating formats (yaml/json/markdown)
// and the zero value fall back to the github body style, while the
// plain-text styles render their own.
func TestBodyStyle(t *testing.T) {
	for _, f := range []Format{FormatYAML, FormatJSON, FormatMarkdown, ""} {
		if got := bodyStyle(f); got != FormatGitHub {
			t.Errorf("bodyStyle(%q) = %q, want github", f, got)
		}
	}
	for _, f := range []Format{FormatDiff, FormatHuman, FormatBrief, FormatGitLab, FormatGitea, FormatGitHub} {
		if got := bodyStyle(f); got != f {
			t.Errorf("bodyStyle(%q) = %q, want %q", f, got, f)
		}
	}
}
