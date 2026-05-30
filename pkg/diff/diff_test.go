package diff

import (
	"strings"
	"testing"

	"github.com/home-operations/flate/internal/assert"
)

func cm(name, ns, key, value string) Doc {
	return Doc{
		Manifest: map[string]any{
			"kind": "ConfigMap",
			"metadata": map[string]any{
				"name": name, "namespace": ns,
			},
			"data": map[string]any{"k": value},
		},
		Parent: Parent{Kind: "HelmRelease", Namespace: ns, Name: key},
	}
}

// TestRun_Simple exercises a value change in a single ConfigMap. The
// body should be dyff's github-mode syntax: `@@ <path> @@` header,
// `! ± value change` marker, `-old` / `+new` value lines.
func TestRun_Simple(t *testing.T) {
	left := []Doc{cm("a", "ns", "owner", "v1")}
	right := []Doc{cm("a", "ns", "owner", "v2")}
	diffs, err := Run(left, right, Options{})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if len(diffs) != 1 {
		t.Fatalf("expected 1 diff, got %d", len(diffs))
	}
	body := diffs[0].Diff
	if !strings.Contains(body, "@@ data.k @@") {
		t.Errorf("dyff path header missing: %s", body)
	}
	if !strings.Contains(body, "- v1") || !strings.Contains(body, "+ v2") {
		t.Errorf("dyff +/- value lines missing: %s", body)
	}
}

func TestRun_NoChange(t *testing.T) {
	d := cm("a", "", "owner", "v1")
	diffs, err := Run([]Doc{d}, []Doc{d}, Options{})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	assert.Equal(t, len(diffs), 0)
}

// TestRun_AddedAndRemoved covers the case where the only change is
// one resource added and another removed — should produce two diff
// entries, one per affected resource.
func TestRun_AddedAndRemoved(t *testing.T) {
	left := []Doc{cm("a", "", "owner", "v")}
	right := []Doc{cm("b", "", "owner", "v")}
	diffs, err := Run(left, right, Options{})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	assert.Equal(t, len(diffs), 2) // one add + one remove
}

// TestRun_Deletion verifies a wholly-removed resource produces a
// diff whose body shows the full removed content (so reviewers
// don't have to chase the original file separately). dyff renders
// this as "@@ (root level) @@" with a "! - N map entries removed:"
// marker followed by `-`-prefixed YAML.
func TestRun_Deletion(t *testing.T) {
	left := []Doc{cm("a", "ns", "owner", "v1")}
	diffs, err := Run(left, nil, Options{})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if len(diffs) != 1 {
		t.Fatalf("expected 1 diff for deletion, got %d", len(diffs))
	}
	body := diffs[0].Diff
	if !strings.Contains(body, "(root level)") {
		t.Errorf("expected root-level removal header; got:\n%s", body)
	}
	if !strings.Contains(body, "removed") {
		t.Errorf("expected 'removed' marker in deletion body; got:\n%s", body)
	}
	if !strings.Contains(body, "- kind: ConfigMap") {
		t.Errorf("expected full removed content with leading `- `; got:\n%s", body)
	}
}

// TestRun_Addition is the symmetric case to TestRun_Deletion.
func TestRun_Addition(t *testing.T) {
	right := []Doc{cm("a", "ns", "owner", "v1")}
	diffs, err := Run(nil, right, Options{})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if len(diffs) != 1 {
		t.Fatalf("expected 1 diff for addition, got %d", len(diffs))
	}
	body := diffs[0].Diff
	if !strings.Contains(body, "(root level)") {
		t.Errorf("expected root-level addition header; got:\n%s", body)
	}
	if !strings.Contains(body, "added") {
		t.Errorf("expected 'added' marker in addition body; got:\n%s", body)
	}
	if !strings.Contains(body, "+ kind: ConfigMap") {
		t.Errorf("expected full added content with leading `+ `; got:\n%s", body)
	}
}

// TestRun_ContainerReorderHasZeroValueChanges locks the K8s-aware
// payoff of the dyff swap: reordering containers by name in a
// Deployment produces an `⇆ order changed` marker — NOT a wall of
// per-line +/- value churn like a text-based diff would. This is
// the main reason we moved off pmezard/go-difflib.
func TestRun_ContainerReorderHasZeroValueChanges(t *testing.T) {
	containers := func(order ...string) []any {
		nameToImg := map[string]string{"nginx": "nginx:1.20", "sidecar": "envoy:1.30", "init": "busybox:1.36"}
		var out []any
		for _, n := range order {
			out = append(out, map[string]any{"name": n, "image": nameToImg[n]})
		}
		return out
	}
	deploy := func(order ...string) []Doc {
		return []Doc{{
			Manifest: map[string]any{
				"kind":     "Deployment",
				"metadata": map[string]any{"name": "x", "namespace": "ns"},
				"spec": map[string]any{
					"template": map[string]any{
						"spec": map[string]any{"containers": containers(order...)},
					},
				},
			},
		}}
	}
	diffs, err := Run(deploy("nginx", "sidecar", "init"), deploy("init", "nginx", "sidecar"), Options{})
	if err != nil {
		t.Fatal(err)
	}
	if len(diffs) != 1 {
		t.Fatalf("expected 1 diff entry (order change), got %d", len(diffs))
	}
	body := diffs[0].Diff
	if !strings.Contains(body, "order changed") {
		t.Errorf("expected 'order changed' marker; got:\n%s", body)
	}
	// Critical: no per-image churn — `image:` should not appear in
	// the body, because dyff matched by container name and saw the
	// values were identical.
	if strings.Contains(body, "image:") {
		t.Errorf("reorder produced spurious image-value churn (dyff list-by-name match broken):\n%s", body)
	}
}

// TestRun_ContainerByNameImageChange complements the reorder test:
// when a named container's image actually changes, dyff reports it
// at the named-path (containers.<name>.image), not by array index.
func TestRun_ContainerByNameImageChange(t *testing.T) {
	mkDeploy := func(image string) []Doc {
		return []Doc{{
			Manifest: map[string]any{
				"kind":     "Deployment",
				"metadata": map[string]any{"name": "x", "namespace": "ns"},
				"spec": map[string]any{
					"template": map[string]any{
						"spec": map[string]any{"containers": []any{
							map[string]any{"name": "nginx", "image": image},
							map[string]any{"name": "sidecar", "image": "envoy:1.30"},
						}},
					},
				},
			},
		}}
	}
	diffs, err := Run(mkDeploy("nginx:1.20"), mkDeploy("nginx:1.21"), Options{})
	if err != nil {
		t.Fatal(err)
	}
	if len(diffs) != 1 {
		t.Fatalf("expected 1 diff, got %d", len(diffs))
	}
	body := diffs[0].Diff
	if !strings.Contains(body, "containers.nginx.image") {
		t.Errorf("expected by-name path (containers.nginx.image); got:\n%s", body)
	}
	if !strings.Contains(body, "- nginx:1.20") || !strings.Contains(body, "+ nginx:1.21") {
		t.Errorf("expected image value change lines; got:\n%s", body)
	}
}

// TestRender_DiffHeaderAlwaysEmitted pins the per-resource header
// policy: every diff body gets a `#`-prefixed identifier, even when
// there's only one diff in the output. dyff's `@@ <path> @@`
// identifies the data path but not the owning resource, so a
// reviewer scanning a PR comment shouldn't have to infer the
// resource from the body. Critically: NO flux-local-style `---/+++`
// twin banner — the single `#` line is the load-bearing identifier.
func TestRender_DiffHeaderAlwaysEmitted(t *testing.T) {
	mkDiff := func(name string) ResourceDiff {
		return ResourceDiff{
			Parent: Parent{Kind: "HelmRelease", Namespace: "media", Name: name},
			Kind:   "Deployment", Namespace: "media", Name: name,
			Diff: "\n@@ spec.replicas @@\n! ± value change\n- 1\n+ 2\n",
		}
	}

	t.Run("single resource still gets a header", func(t *testing.T) {
		out, err := Render([]ResourceDiff{mkDiff("qui")}, FormatDiff)
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
		out, err := Render([]ResourceDiff{mkDiff("qui"), mkDiff("plex")}, FormatDiff)
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

// TestRender_Markdown pins the FormatMarkdown shape:
//   - Top-level `# Diff` heading,
//   - A pipe-table summary classifying entries as added/modified/removed
//     (derived from the dyff body),
//   - One H3 + ```diff fence per ResourceDiff, with the dyff body
//     passed through verbatim inside the fence,
//   - An empty diff set renders as the empty document so callers
//     (e.g. PR-comment automation) can skip posting entirely.
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
			// Body must appear verbatim inside the fence.
			if !strings.Contains(s, d.Diff) {
				t.Errorf("dyff body for %s missing from output; got:\n%s", d.Header(), s)
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

func TestFormat_JSON(t *testing.T) {
	diffs := []ResourceDiff{{Kind: "ConfigMap", Name: "a", Diff: "..."}}
	out, err := Render(diffs, FormatJSON)
	if err != nil {
		t.Fatalf("Render: %v", err)
	}
	if !strings.Contains(string(out), `"kind": "ConfigMap"`) {
		t.Errorf("json output: %s", out)
	}
}

func TestResourceDiff_Header(t *testing.T) {
	hrDiff := ResourceDiff{
		Parent: Parent{Kind: "HelmRelease", Namespace: "media", Name: "qui"},
		Kind:   "Deployment", Namespace: "media", Name: "qui",
	}
	assert.Equal(t, hrDiff.Header(), "HelmRelease: media/qui Deployment: media/qui")

	ksDiff := ResourceDiff{
		Parent: Parent{
			Kind: "Kustomization", Namespace: "media", Name: "qui",
			Path: "kubernetes/apps/media/qui/app",
		},
		Kind: "HelmRelease", Namespace: "media", Name: "qui",
	}
	// Parent.Path is preserved on the struct (for JSON/YAML
	// consumers) but deliberately omitted from the human header so
	// KS-owned and HR-owned entries render symmetrically.
	assert.Equal(t, ksDiff.Header(), "Kustomization: media/qui HelmRelease: media/qui")
}

// TestRun_StripAttrsRemovesNoise pins that Options.StripAttrs is
// applied before the dyff comparison: rotating a stripped annotation
// without changing anything else yields zero diffs. This is the
// chart-bump noise filter — `helm.sh/chart` changes on every chart
// upgrade and would otherwise produce one entry per rendered
// resource even with the K8s-aware dyff backend.
func TestRun_StripAttrsRemovesNoise(t *testing.T) {
	mk := func(chartLabel string) []Doc {
		return []Doc{{
			Manifest: map[string]any{
				"kind": "Deployment",
				"metadata": map[string]any{
					"name":      "x",
					"namespace": "ns",
					"annotations": map[string]any{
						"helm.sh/chart": chartLabel,
					},
				},
			},
		}}
	}
	diffs, err := Run(mk("myapp-1.2.3"), mk("myapp-1.2.4"),
		Options{StripAttrs: []string{"helm.sh/chart"}})
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, len(diffs), 0) // stripped annotation produces no entries

	// Sanity: without --strip-attr, the same change DOES surface as
	// a diff — proves the strip is what's suppressing the entry.
	diffs, err = Run(mk("myapp-1.2.3"), mk("myapp-1.2.4"), Options{})
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, len(diffs), 1) // control: unstripped change surfaces
}

// TestRun_RedactsConfigMapBinaryData locks the ConfigMap.binaryData
// redaction: each value is replaced with a content-derived summary
// before dyff sees the manifest, so a chart bump that only rotates a
// 200 KB binary hook payload (kube-prometheus-stack's CRD upgrade
// bundle is the canonical case) still surfaces as a one-line "this
// changed" diff instead of two walls of base64. Sibling text fields
// in `data` keep their normal line-by-line diff.
func TestRun_RedactsConfigMapBinaryData(t *testing.T) {
	mk := func(blob, data string) []Doc {
		return []Doc{{
			Manifest: map[string]any{
				"kind": "ConfigMap",
				"metadata": map[string]any{
					"name": "binary", "namespace": "ns",
				},
				"binaryData": map[string]any{"payload.bin": blob},
				"data":       map[string]any{"visible": data},
			},
		}}
	}

	diffs, err := Run(mk("QUFBQQ==", "same"), mk("QkJCQg==", "same"), Options{})
	if err != nil {
		t.Fatal(err)
	}
	if len(diffs) != 1 {
		t.Fatalf("binaryData-only change should surface as a redacted diff, got %d diff(s)", len(diffs))
	}
	body := diffs[0].Diff
	if !strings.Contains(body, "@@ binaryData.payload.bin @@") {
		t.Errorf("expected binaryData path to remain; got:\n%s", body)
	}
	if !strings.Contains(body, "redacted binary data") {
		t.Errorf("expected redaction marker; got:\n%s", body)
	}
	if strings.Contains(body, "QUFBQQ==") || strings.Contains(body, "QkJCQg==") {
		t.Errorf("raw binaryData leaked into diff body:\n%s", body)
	}

	diffs, err = Run(mk("QUFBQQ==", "old"), mk("QkJCQg==", "new"), Options{})
	if err != nil {
		t.Fatal(err)
	}
	if len(diffs) != 1 {
		t.Fatalf("mixed data + binaryData change should surface as one diff, got %d diff(s)", len(diffs))
	}
	body = diffs[0].Diff
	if strings.Contains(body, "QUFBQQ==") || strings.Contains(body, "QkJCQg==") {
		t.Errorf("raw binaryData leaked into diff body:\n%s", body)
	}
	if !strings.Contains(body, "@@ data.visible @@") {
		t.Errorf("expected visible data change to remain; got:\n%s", body)
	}
}

// TestRun_DistinguishesByParent locks the parent-aware pairing: two
// HelmReleases each rendering a same-named Deployment are tracked
// independently — a change to HR/a's copy doesn't leak into a phantom
// "removed/added" pair against HR/b's copy.
func TestRun_DistinguishesByParent(t *testing.T) {
	left := []Doc{
		{
			Manifest: map[string]any{"kind": "Deployment", "metadata": map[string]any{"name": "x", "namespace": "ns"}, "spec": "old"},
			Parent:   Parent{Kind: "HelmRelease", Namespace: "ns", Name: "a"},
		},
		{
			Manifest: map[string]any{"kind": "Deployment", "metadata": map[string]any{"name": "x", "namespace": "ns"}, "spec": "old"},
			Parent:   Parent{Kind: "HelmRelease", Namespace: "ns", Name: "b"},
		},
	}
	right := []Doc{
		{
			Manifest: map[string]any{"kind": "Deployment", "metadata": map[string]any{"name": "x", "namespace": "ns"}, "spec": "new"},
			Parent:   Parent{Kind: "HelmRelease", Namespace: "ns", Name: "a"},
		},
		{
			Manifest: map[string]any{"kind": "Deployment", "metadata": map[string]any{"name": "x", "namespace": "ns"}, "spec": "old"},
			Parent:   Parent{Kind: "HelmRelease", Namespace: "ns", Name: "b"},
		},
	}
	diffs, err := Run(left, right, Options{})
	if err != nil {
		t.Fatal(err)
	}
	if len(diffs) != 1 {
		t.Fatalf("expected 1 diff (only HR=a's Deployment changed), got %d: %+v", len(diffs), diffs)
	}
	if diffs[0].Parent.Name != "a" {
		t.Errorf("wrong parent: %+v", diffs[0].Parent)
	}
}

// TestRun_PairKeyIncludesParentPath: two KS parents with identical
// (kind, ns, name) but different spec.path render the SAME-NAMED child
// document. Without spec.Path in the pair key, the two children would
// merge in `pair`, producing a misleading diff.
func TestRun_PairKeyIncludesParentPath(t *testing.T) {
	makeKS := func(name, ns, path, val string) Doc {
		return Doc{
			Manifest: map[string]any{
				"kind": "ConfigMap",
				"metadata": map[string]any{
					"name": "shared", "namespace": ns,
				},
				"data": map[string]any{"k": val},
			},
			Parent: Parent{Kind: "Kustomization", Namespace: ns, Name: name, Path: path},
		}
	}
	// Two KS parents both named "apps" in "flux-system" but with
	// distinct paths. Each parent renders a ConfigMap named "shared".
	left := []Doc{
		makeKS("apps", "flux-system", "main/apps", "from-main"),
		makeKS("apps", "flux-system", "test/apps", "from-test"),
	}
	right := []Doc{
		makeKS("apps", "flux-system", "main/apps", "from-main-CHANGED"),
		makeKS("apps", "flux-system", "test/apps", "from-test"),
	}
	diffs, err := Run(left, right, Options{})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	// Exactly one ConfigMap changed — the "main/apps" one.
	if len(diffs) != 1 {
		t.Fatalf("expected 1 diff scoped to main/apps parent, got %d: %+v", len(diffs), diffs)
	}
	if diffs[0].Parent.Path != "main/apps" {
		t.Errorf("diff attributed to wrong parent path: %+v", diffs[0].Parent)
	}
}
