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
// payoff of the dyff backend: reordering containers by name in a
// Deployment produces an `⇆ order changed` marker — NOT a wall of
// per-line +/- value churn like a text-based diff would.
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

// TestRun_Styles pins the two per-resource body styles Run resolves to:
// the github body (embedded in yaml/json/markdown) and the plain unified
// diff. The dyff text styles render through renderNative instead — see
// native_test.go.
func TestRun_Styles(t *testing.T) {
	left := []Doc{cm("a", "ns", "owner", "v1")}
	right := []Doc{cm("a", "ns", "owner", "v2")}

	cases := []struct {
		format   Format
		contains []string
	}{
		{FormatGitHub, []string{"@@ data.k @@", "! ± value change", "- v1", "+ v2"}},
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
