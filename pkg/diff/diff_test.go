package diff

import (
	"strings"
	"testing"
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
	if len(diffs) != 0 {
		t.Errorf("expected zero diffs, got %d", len(diffs))
	}
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
	if len(diffs) != 2 {
		t.Errorf("expected 2 diffs for add+remove, got %d", len(diffs))
	}
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

// TestRender_DiffHeaderOnlyWhenMultiple pins the per-resource header
// policy: a single diff renders bare (no `# ...` line), but two or
// more diffs each get a `#`-prefixed identifier so the reader can
// tell which resource a `@@ <path> @@` block belongs to. dyff's
// path doesn't carry the owning resource, so headers are load-
// bearing exactly when ambiguity is possible — and only then.
func TestRender_DiffHeaderOnlyWhenMultiple(t *testing.T) {
	mkDiff := func(name string) ResourceDiff {
		return ResourceDiff{
			Parent: Parent{Kind: "HelmRelease", Namespace: "media", Name: name},
			Kind:   "Deployment", Namespace: "media", Name: name,
			Diff: "\n@@ spec.replicas @@\n! ± value change\n- 1\n+ 2\n",
		}
	}

	t.Run("single resource renders without header", func(t *testing.T) {
		out, err := Render([]ResourceDiff{mkDiff("qui")}, FormatDiff)
		if err != nil {
			t.Fatalf("Render: %v", err)
		}
		if strings.Contains(string(out), "# HelmRelease") {
			t.Errorf("single-resource output should not include a header line; got:\n%s", out)
		}
		if !strings.Contains(string(out), "@@ spec.replicas @@") {
			t.Errorf("body should pass through verbatim; got:\n%s", out)
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
		// And critically: NO flux-local-style `--- / +++` twin banners.
		if strings.Contains(s, "--- HelmRelease") || strings.Contains(s, "+++ HelmRelease") {
			t.Errorf("output reintroduced the --- / +++ twin banner:\n%s", s)
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
	want := "HelmRelease: media/qui Deployment: media/qui"
	if got := hrDiff.Header(); got != want {
		t.Errorf("HR header:\n got %q\nwant %q", got, want)
	}

	ksDiff := ResourceDiff{
		Parent: Parent{
			Kind: "Kustomization", Namespace: "media", Name: "qui",
			Path: "kubernetes/apps/media/qui/app",
		},
		Kind: "HelmRelease", Namespace: "media", Name: "qui",
	}
	want = "kubernetes/apps/media/qui/app Kustomization: media/qui HelmRelease: media/qui"
	if got := ksDiff.Header(); got != want {
		t.Errorf("KS header:\n got %q\nwant %q", got, want)
	}
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
	if len(diffs) != 0 {
		t.Errorf("stripped annotation should produce no diff entries; got %d:\n%+v", len(diffs), diffs)
	}

	// Sanity: without --strip-attr, the same change DOES surface as
	// a diff — proves the strip is what's suppressing the entry.
	diffs, err = Run(mk("myapp-1.2.3"), mk("myapp-1.2.4"), Options{})
	if err != nil {
		t.Fatal(err)
	}
	if len(diffs) != 1 {
		t.Errorf("control: unstripped annotation change should surface; got %d diffs", len(diffs))
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
