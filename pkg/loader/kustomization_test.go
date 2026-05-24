package loader

import "testing"

// TestResolveDataPath_RelativeUnderBase covers the happy path: a
// generator file path that stays under the kustomization directory
// resolves to its absolute equivalent.
func TestResolveDataPath_RelativeUnderBase(t *testing.T) {
	abs, ok := resolveDataPath("/tmp/cluster/apps/foo", "data/values.yaml")
	if !ok {
		t.Fatalf("expected resolve to succeed for in-tree relative path")
	}
	if abs != "/tmp/cluster/apps/foo/data/values.yaml" {
		t.Errorf("unexpected resolution: %s", abs)
	}
}

// TestResolveDataPath_RejectsTraversal locks the defense-in-depth
// guard added after the round-4 audit: a kustomization.yaml that
// declares `files: ["../../../etc/passwd"]` must NOT escape the
// kustomization directory. The resolved key is consulted against
// tree-walk paths today (no escape can match a walked path rooted
// at --path), but a future caller that opens the path would hit a
// path-traversal surface for free without this guard.
func TestResolveDataPath_RejectsTraversal(t *testing.T) {
	cases := []string{
		"../escape.yaml",
		"../../etc/passwd",
		"sub/../../escape",
	}
	for _, rel := range cases {
		if _, ok := resolveDataPath("/tmp/cluster/apps/foo", rel); ok {
			t.Errorf("rel %q should have been rejected as escaping base", rel)
		}
	}
}

// TestResolveDataPath_AbsolutePathPassesThrough confirms the
// documented exception: kustomize accepts absolute paths for
// generator files, so we pass them verbatim (after Clean). The
// loader's downstream "is this under --path?" check still applies
// at walk time; resolveDataPath isn't responsible for absolute-path
// containment.
func TestResolveDataPath_AbsolutePathPassesThrough(t *testing.T) {
	abs, ok := resolveDataPath("/tmp/cluster/apps", "/etc/values.yaml")
	if !ok {
		t.Fatalf("expected absolute path to resolve")
	}
	if abs != "/etc/values.yaml" {
		t.Errorf("absolute path should pass through verbatim; got %s", abs)
	}
}

// TestResolveDataPath_EmptyRejected guards the explicit empty-rel
// check — kustomize would reject an empty entry too.
func TestResolveDataPath_EmptyRejected(t *testing.T) {
	if _, ok := resolveDataPath("/tmp/cluster/apps", ""); ok {
		t.Errorf("empty rel should be rejected")
	}
}
