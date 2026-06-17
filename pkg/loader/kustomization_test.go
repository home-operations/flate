package loader

import (
	"testing"

	"github.com/home-operations/flate/internal/testutil"
)

// TestResolveDataPath covers generator-file path resolution. Rows:
//   - in-tree relative path resolves to its absolute equivalent;
//   - traversal-escaping rels are rejected — defense-in-depth from the
//     round-4 audit: a kustomization.yaml declaring `files:
//     ["../../../etc/passwd"]` must NOT escape the kustomization dir;
//   - absolute paths pass through verbatim (after Clean) — kustomize
//     accepts them and downstream "under --path?" checks still apply;
//   - empty rel is rejected, matching kustomize.
func TestResolveDataPath(t *testing.T) {
	cases := []struct {
		name    string
		base    string
		rel     string
		wantAbs string
		wantOK  bool
	}{
		{"relative under base", "/tmp/cluster/apps/foo", "data/values.yaml", "/tmp/cluster/apps/foo/data/values.yaml", true},
		{"traversal parent", "/tmp/cluster/apps/foo", "../escape.yaml", "", false},
		{"traversal deep", "/tmp/cluster/apps/foo", "../../etc/passwd", "", false},
		{"traversal mixed", "/tmp/cluster/apps/foo", "sub/../../escape", "", false},
		{"absolute passes through", "/tmp/cluster/apps", "/etc/values.yaml", "/etc/values.yaml", true},
		{"empty rejected", "/tmp/cluster/apps", "", "", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			abs, ok := resolveDataPath(tc.base, tc.rel)
			if ok != tc.wantOK {
				t.Fatalf("resolveDataPath(%q, %q) ok = %v, want %v", tc.base, tc.rel, ok, tc.wantOK)
			}
			if ok && abs != tc.wantAbs {
				t.Errorf("resolveDataPath(%q, %q) = %q, want %q", tc.base, tc.rel, abs, tc.wantAbs)
			}
		})
	}
}

// TestResolveResourcePath covers directory `resources:` path
// resolution. Unlike resolveDataPath, a parent-escaping relative dir
// RESOLVES — overlays reference ../base and the dir is only descended
// into, never opened. Absolute paths and URLs are still rejected.
func TestResolveResourcePath(t *testing.T) {
	cases := []struct {
		name    string
		base    string
		rel     string
		wantAbs string
		wantOK  bool
	}{
		{"relative under base", "/tmp/cluster/apps/foo", "sub/dir", "/tmp/cluster/apps/foo/sub/dir", true},
		{"traversal parent resolves", "/tmp/cluster/apps/foo", "../bar", "/tmp/cluster/apps/bar", true},
		{"traversal deep resolves", "/tmp/cluster/apps/foo", "../../../deploy/x", "/tmp/deploy/x", true},
		{"absolute rejected", "/tmp/cluster/apps", "/etc/x", "", false},
		{"url rejected", "/tmp/cluster/apps", "https://example.com/x", "", false},
		{"empty rejected", "/tmp/cluster/apps", "", "", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			abs, ok := resolveResourcePath(tc.base, tc.rel)
			if ok != tc.wantOK {
				t.Fatalf("resolveResourcePath(%q, %q) ok = %v, want %v", tc.base, tc.rel, ok, tc.wantOK)
			}
			if ok && abs != tc.wantAbs {
				t.Errorf("resolveResourcePath(%q, %q) = %q, want %q", tc.base, tc.rel, abs, tc.wantAbs)
			}
		})
	}
}

// TestIsUnreferencedKustomizeResource pins the #777 orphan signal: a manifest is
// "unreferenced" only when its own directory's kustomization.yaml exists and does
// not list it. A listed file, or a directory with no kustomization at all (a
// loose top-level entry), is not.
func TestIsUnreferencedKustomizeResource(t *testing.T) {
	dir := t.TempDir()
	// excluded/ — kustomization references only cm.yaml; ks.yaml is a stray file.
	testutil.WriteFile(t, dir, "excluded/kustomization.yaml", "resources:\n  - cm.yaml\n")
	// referenced/ — kustomization references ks.yaml.
	testutil.WriteFile(t, dir, "referenced/kustomization.yaml", "resources:\n  - ks.yaml\n  - cm.yaml\n")
	// loose/ — a KS file with no kustomization.yaml beside it.

	cases := []struct {
		name string
		file string
		want bool
	}{
		{"excluded by own kustomization", "excluded/ks.yaml", true},
		{"referenced by own kustomization", "referenced/ks.yaml", false},
		{"no kustomization in dir", "loose/ks.yaml", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := IsUnreferencedKustomizeResource(dir, tc.file); got != tc.want {
				t.Errorf("IsUnreferencedKustomizeResource(%q) = %v, want %v", tc.file, got, tc.want)
			}
		})
	}
}
