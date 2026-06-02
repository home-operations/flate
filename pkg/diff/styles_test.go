package diff

import (
	"strings"
	"testing"
)

// TestRun_Styles pins that each output style renders a distinctive
// per-resource body for the same value change. The dyff styles mirror
// `dyff between --output <style>`; diff is a plain unified diff.
func TestRun_Styles(t *testing.T) {
	left := []Doc{cm("a", "ns", "owner", "v1")}
	right := []Doc{cm("a", "ns", "owner", "v2")}

	cases := []struct {
		format   Format
		contains []string
	}{
		{FormatGitHub, []string{"@@ data.k @@", "! ± value change", "- v1", "+ v2"}},
		{FormatGitea, []string{"@@ data.k @@", "- v1", "+ v2"}},
		{FormatGitLab, []string{"= data.k", "# ± value change", "- v1", "+ v2"}},
		{FormatHuman, []string{"data.k", "value change", "- v1", "+ v2"}},
		{FormatBrief, []string{"change detected"}},
		{FormatDiff, []string{"--- ConfigMap ns/a", "+++ ConfigMap ns/a", "@@ -", "-  k: v1", "+  k: v2"}},
	}
	for _, tc := range cases {
		t.Run(string(tc.format), func(t *testing.T) {
			diffs, err := Run(left, right, Options{Format: tc.format})
			if err != nil {
				t.Fatalf("Run: %v", err)
			}
			if len(diffs) != 1 {
				t.Fatalf("expected 1 diff, got %d", len(diffs))
			}
			body := diffs[0].Diff
			for _, want := range tc.contains {
				if !strings.Contains(body, want) {
					t.Errorf("%s body missing %q:\n%s", tc.format, want, body)
				}
			}
		})
	}
}

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

// TestRender_TextFormatsConcatenate pins that every plain-text format
// (and the zero value) routes through renderText: a `# <resource>`
// header followed by the pre-rendered body verbatim.
func TestRender_TextFormatsConcatenate(t *testing.T) {
	d := ResourceDiff{Kind: "ConfigMap", Namespace: "ns", Name: "a", Diff: "BODY-LINE\n"}
	for _, f := range []Format{"", FormatGitHub, FormatDiff, FormatHuman, FormatBrief, FormatGitLab, FormatGitea} {
		out, err := Render([]ResourceDiff{d}, f)
		if err != nil {
			t.Fatalf("Render(%q): %v", f, err)
		}
		s := string(out)
		if !strings.Contains(s, "# ConfigMap: ns/a") {
			t.Errorf("Render(%q) missing header:\n%s", f, s)
		}
		if !strings.Contains(s, "BODY-LINE") {
			t.Errorf("Render(%q) missing body:\n%s", f, s)
		}
	}
}

func TestRender_UnknownFormat(t *testing.T) {
	if _, err := Render(nil, Format("bogus")); err == nil {
		t.Error("expected error for unknown format")
	}
}
