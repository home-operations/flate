package cacheroot

import (
	"github.com/opencontainers/go-digest"
	"path/filepath"
	"testing"
)

// TestLayout_Paths locks in the on-disk layout. The test is intentionally
// strict — every relocation goes through this fixture, so the diff that
// renames a subdir is co-located with the diff that updates the GC, the
// writers, and this test.
func TestLayout_Paths(t *testing.T) {
	l := New("/cache")
	cases := []struct {
		name string
		got  string
		want string
	}{
		{"OCI", l.OCI(), "/cache/oci"},
		{"OCILock", l.OCILock(), "/cache/oci.lock"},
		{"OCIBlob", l.OCIBlob(digest.Digest("sha256:ffff")), "/cache/oci/blobs/sha256/ffff"},
		{"Sources", l.Sources(), "/cache/sources"},
		{"SourceSlot", l.SourceSlot("myrepo", "deadbeef0123"), "/cache/sources/myrepo/deadbeef0123"},
		{"Baselines", l.Baselines(), "/cache/baselines"},
		{"Baseline", l.Baseline("abc123"), "/cache/baselines/abc123"},
		{"GitMirrors", l.GitMirrors(), "/cache/git-mirrors"},
		{"GitMirror", l.GitMirror("0123"), "/cache/git-mirrors/0123"},
		{"RenderHelmCache", l.RenderHelmCache(), "/cache/render/helm"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if filepath.Clean(c.got) != filepath.Clean(c.want) {
				t.Errorf("%s = %q, want %q", c.name, c.got, c.want)
			}
		})
	}
}

// TestNew_CleansRoot confirms that New normalises dirty roots so that
// the join hot-path helper can skip a second filepath.Clean pass.
func TestNew_CleansRoot(t *testing.T) {
	cases := []struct{ in, want string }{
		{"", ""},
		{"/cache/", "/cache"},
		{"/cache//sub/..", "/cache"},
		{"/cache/.", "/cache"},
	}
	for _, c := range cases {
		l := New(c.in)
		if l.Root != c.want {
			t.Errorf("New(%q).Root = %q, want %q", c.in, l.Root, c.want)
		}
	}
}
