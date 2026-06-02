package diff

import (
	"strings"
	"testing"
)

// TestRender_Unified pins that the unified diff concatenates bodies
// verbatim with no flate header — its own `--- `/`+++ ` labels name the
// resource — and that Render rejects the dyff text styles, which are
// rendered natively (renderNative), not from a []ResourceDiff.
func TestRender_Unified(t *testing.T) {
	d := ResourceDiff{Kind: "ConfigMap", Namespace: "ns", Name: "a", Diff: "BODY-LINE\n"}

	out, err := Render([]ResourceDiff{d}, FormatDiff)
	if err != nil {
		t.Fatalf("Render(diff): %v", err)
	}
	s := string(out)
	if strings.Contains(s, "#") {
		t.Errorf("Render(diff) should not add a header:\n%s", s)
	}
	if !strings.Contains(s, "BODY-LINE") {
		t.Errorf("Render(diff) missing body:\n%s", s)
	}

	for _, f := range []Format{"", FormatGitHub, FormatHuman, FormatBrief, FormatGitLab, FormatGitea} {
		if _, err := Render([]ResourceDiff{d}, f); err == nil {
			t.Errorf("Render(%q) should be rejected (dyff styles render natively)", f)
		}
	}
}

// TestRender_Markdown pins the FormatMarkdown shape:
//   - Top-level `# Diff` heading,
//   - A pipe-table summary classifying entries as added/modified/removed,
//   - One H3 + ```diff fence per ResourceDiff, with the body passed
//     through verbatim inside the fence,
//   - An empty diff set renders as the empty document so callers (e.g.
//     PR-comment automation) can skip posting entirely.
func TestRender_Markdown(t *testing.T) {
	t.Run("empty diffs render as the empty document", func(t *testing.T) {
		out, err := Render(nil, FormatMarkdown)
		if err != nil {
			t.Fatalf("Render: %v", err)
		}
		if len(out) != 0 {
			t.Errorf("expected empty output for empty diff set, got:\n%s", out)
		}
	})

	t.Run("mixed added/modified/removed", func(t *testing.T) {
		// Modified resource: same name in both, value differs.
		left := []Doc{
			cm("modme", "ns", "owner", "v1"),
			cm("removeme", "ns", "owner", "v1"), // removal: only on left
		}
		right := []Doc{
			cm("modme", "ns", "owner", "v2"),
			cm("addme", "ns", "owner", "v1"), // addition: only on right
		}
		diffs, err := Run(left, right, Options{})
		if err != nil {
			t.Fatalf("Run: %v", err)
		}
		if len(diffs) != 3 {
			t.Fatalf("expected 3 diff entries (add + mod + remove), got %d:\n%+v", len(diffs), diffs)
		}
		out, err := Render(diffs, FormatMarkdown)
		if err != nil {
			t.Fatalf("Render: %v", err)
		}
		s := string(out)

		// Top-level heading.
		if !strings.Contains(s, "# Diff") {
			t.Errorf("missing top-level heading; got:\n%s", s)
		}

		// Summary table classifying one of each.
		if !strings.Contains(s, "| Added | Modified | Removed | Total |") {
			t.Errorf("missing summary table header; got:\n%s", s)
		}
		if !strings.Contains(s, "| 1 | 1 | 1 | 3 |") {
			t.Errorf("missing/incorrect summary row (expected 1 added, 1 modified, 1 removed, 3 total); got:\n%s", s)
		}

		// Per-resource sections: one H3 heading + ```diff fence each.
		for _, d := range diffs {
			heading := "### " + d.Header()
			if !strings.Contains(s, heading) {
				t.Errorf("missing per-resource heading %q; got:\n%s", heading, s)
			}
			if !strings.Contains(s, d.Diff) {
				t.Errorf("body for %s missing from output; got:\n%s", d.Header(), s)
			}
		}
		if !strings.Contains(s, "```diff\n") {
			t.Errorf("missing ```diff fence opener; got:\n%s", s)
		}
		if !strings.Contains(s, "\n```\n") {
			t.Errorf("missing closing fence; got:\n%s", s)
		}
	})
}

func TestRender_JSON(t *testing.T) {
	diffs := []ResourceDiff{{Kind: "ConfigMap", Name: "a", Diff: "..."}}
	out, err := Render(diffs, FormatJSON)
	if err != nil {
		t.Fatalf("Render: %v", err)
	}
	if !strings.Contains(string(out), `"kind": "ConfigMap"`) {
		t.Errorf("json output: %s", out)
	}
}

func TestRender_UnknownFormat(t *testing.T) {
	if _, err := Render(nil, Format("bogus")); err == nil {
		t.Error("expected error for unknown format")
	}
}
