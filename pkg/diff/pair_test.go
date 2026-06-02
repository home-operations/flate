package diff

import "testing"

// TestPair_DistinguishesByParent locks the parent-aware pairing: two
// HelmReleases each rendering a same-named Deployment are tracked
// independently, so a change to HR/a's copy doesn't leak into a phantom
// pair against HR/b's copy. Without the parent in the key the two would
// collapse into one entry.
func TestPair_DistinguishesByParent(t *testing.T) {
	dep := func(hr, spec string) Doc {
		return Doc{
			Manifest: map[string]any{"kind": "Deployment", "metadata": map[string]any{"name": "x", "namespace": "ns"}, "spec": spec},
			Parent:   Parent{Kind: "HelmRelease", Namespace: "ns", Name: hr},
		}
	}
	left := []Doc{dep("a", "old"), dep("b", "old")}
	right := []Doc{dep("a", "new"), dep("b", "old")}

	pairs := pair(left, right)
	if len(pairs) != 2 {
		t.Fatalf("expected 2 paired resources (one per parent), got %d: %+v", len(pairs), pairs)
	}
	for _, p := range pairs {
		changed := p.a["spec"] != p.b["spec"]
		switch p.parent.Name {
		case "a":
			if !changed {
				t.Errorf("HR a's Deployment should show a spec change: %+v", p)
			}
		case "b":
			if changed {
				t.Errorf("HR b's Deployment should be unchanged: %+v", p)
			}
		default:
			t.Errorf("unexpected parent %q", p.parent.Name)
		}
	}
}

// TestPair_KeyIncludesParentPath: two KS parents with identical
// (kind, ns, name) but different spec.path render the SAME-NAMED child.
// Without spec.Path in the key the two children would merge in pair.
func TestPair_KeyIncludesParentPath(t *testing.T) {
	ks := func(path, val string) Doc {
		return Doc{
			Manifest: map[string]any{
				"kind":     "ConfigMap",
				"metadata": map[string]any{"name": "shared", "namespace": "flux-system"},
				"data":     map[string]any{"k": val},
			},
			Parent: Parent{Kind: "Kustomization", Namespace: "flux-system", Name: "apps", Path: path},
		}
	}
	left := []Doc{ks("main/apps", "from-main"), ks("test/apps", "from-test")}
	right := []Doc{ks("main/apps", "from-main-CHANGED"), ks("test/apps", "from-test")}

	pairs := pair(left, right)
	if len(pairs) != 2 {
		t.Fatalf("expected 2 paired resources (one per parent path), got %d: %+v", len(pairs), pairs)
	}
	for _, p := range pairs {
		changed := p.a["data"].(map[string]any)["k"] != p.b["data"].(map[string]any)["k"]
		if changed && p.parent.Path != "main/apps" {
			t.Errorf("change attributed to wrong parent path %q", p.parent.Path)
		}
	}
}
