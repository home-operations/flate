package loader

import "testing"

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
