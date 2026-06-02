package diff

import (
	"strings"
	"testing"
)

// TestRender_DiffHeaderAlwaysEmitted pins the per-resource header
// policy for the dyff styles: every body gets a `#`-prefixed identifier,
// even when there's only one diff. dyff's `@@ <path> @@` identifies the
// data path but not the owning resource, so a reviewer scanning a PR
// comment shouldn't have to infer it. Critically: NO flux-local-style
// `---/+++` twin banner — the single `#` line is the load-bearing
// identifier.
func TestRender_DiffHeaderAlwaysEmitted(t *testing.T) {
	mkDiff := func(name string) ResourceDiff {
		return ResourceDiff{
			Parent: Parent{Kind: "HelmRelease", Namespace: "media", Name: name},
			Kind:   "Deployment", Namespace: "media", Name: name,
			Diff: "\n@@ spec.replicas @@\n! ± value change\n- 1\n+ 2\n",
		}
	}

	t.Run("single resource still gets a header", func(t *testing.T) {
		out, err := Render([]ResourceDiff{mkDiff("qui")}, FormatGitHub)
		if err != nil {
			t.Fatalf("Render: %v", err)
		}
		s := string(out)
		if !strings.Contains(s, "# HelmRelease: media/qui Deployment: media/qui") {
			t.Errorf("single-resource output must still include the header; got:\n%s", s)
		}
		if !strings.Contains(s, "@@ spec.replicas @@") {
			t.Errorf("body should pass through verbatim; got:\n%s", s)
		}
	})

	t.Run("multiple resources each get a header", func(t *testing.T) {
		out, err := Render([]ResourceDiff{mkDiff("qui"), mkDiff("plex")}, FormatGitHub)
		if err != nil {
			t.Fatalf("Render: %v", err)
		}
		s := string(out)
		if !strings.Contains(s, "# HelmRelease: media/qui Deployment: media/qui") {
			t.Errorf("missing qui header:\n%s", s)
		}
		if !strings.Contains(s, "# HelmRelease: media/plex Deployment: media/plex") {
			t.Errorf("missing plex header:\n%s", s)
		}
		// Critically: NO flux-local-style `--- / +++` twin banners.
		if strings.Contains(s, "--- HelmRelease") || strings.Contains(s, "+++ HelmRelease") {
			t.Errorf("output reintroduced the --- / +++ twin banner:\n%s", s)
		}
	})
}

// TestRender_TextFormatsConcatenate pins that the dyff text styles (and
// the zero value) prepend a `# <resource>` header to each body, while
// the plain unified diff omits it — its own `--- `/`+++ ` labels already
// name the resource.
func TestRender_TextFormatsConcatenate(t *testing.T) {
	d := ResourceDiff{Kind: "ConfigMap", Namespace: "ns", Name: "a", Diff: "BODY-LINE\n"}

	for _, f := range []Format{"", FormatGitHub, FormatHuman, FormatBrief, FormatGitLab, FormatGitea} {
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

	// Unified diff: body only, no flate header line.
	out, err := Render([]ResourceDiff{d}, FormatDiff)
	if err != nil {
		t.Fatalf("Render(diff): %v", err)
	}
	s := string(out)
	if strings.Contains(s, "# ConfigMap") {
		t.Errorf("Render(diff) should omit the header:\n%s", s)
	}
	if !strings.Contains(s, "BODY-LINE") {
		t.Errorf("Render(diff) missing body:\n%s", s)
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
