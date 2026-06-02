package diff

import "testing"

// TestPair_DistinguishesByParent locks the parent-aware pairing: two
// HelmReleases each rendering a same-named Deployment are tracked
// independently — a change to HR/a's copy doesn't leak into a phantom
// "removed/added" pair against HR/b's copy.
func TestPair_DistinguishesByParent(t *testing.T) {
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

// TestPair_KeyIncludesParentPath: two KS parents with identical
// (kind, ns, name) but different spec.path render the SAME-NAMED child
// document. Without spec.Path in the pair key, the two children would
// merge in pair, producing a misleading diff.
func TestPair_KeyIncludesParentPath(t *testing.T) {
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
