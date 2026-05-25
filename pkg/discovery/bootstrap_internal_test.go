package discovery

import (
	"testing"

	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

// TestNewBootstrapAlias covers the per-kind branches that aliasing
// supports plus the unsupported-kind branch that lets
// publishBootstrapAlias silently skip non-aliasable sourceRefs.
func TestNewBootstrapAlias(t *testing.T) {
	t.Parallel()
	const repoRoot = "/tmp/repo"
	cases := []struct {
		name    string
		id      manifest.NamedResource
		wantOK  bool
		wantURL string
		wantNS  string
	}{
		{
			name:    "GitRepository → file:// alias",
			id:      manifest.NamedResource{Kind: manifest.KindGitRepository, Namespace: "flux-system", Name: "flux-system"},
			wantOK:  true,
			wantURL: "file:///tmp/repo",
			wantNS:  "flux-system",
		},
		{
			name:    "OCIRepository → synthetic oci:// alias",
			id:      manifest.NamedResource{Kind: manifest.KindOCIRepository, Namespace: "flux-system", Name: "flux-manifests"},
			wantOK:  true,
			wantURL: "oci://flate-bootstrap-alias/flux-system/flux-manifests",
			wantNS:  "flux-system",
		},
		{
			name:   "HelmRepository → unsupported (caller should skip)",
			id:     manifest.NamedResource{Kind: manifest.KindHelmRepository, Namespace: "flux-system", Name: "repo"},
			wantOK: false,
		},
		{
			name:   "Bucket → unsupported (caller should skip)",
			id:     manifest.NamedResource{Kind: manifest.KindBucket, Namespace: "flux-system", Name: "bucket"},
			wantOK: false,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			obj, url, ok := newBootstrapAlias(tc.id, repoRoot)
			if ok != tc.wantOK {
				t.Fatalf("ok = %v, want %v", ok, tc.wantOK)
			}
			if !ok {
				if obj != nil || url != "" {
					t.Errorf("unsupported kind returned obj=%v url=%q; want zero values", obj, url)
				}
				return
			}
			if url != tc.wantURL {
				t.Errorf("url = %q, want %q", url, tc.wantURL)
			}
			named := obj.Named()
			if named != tc.id {
				t.Errorf("obj.Named() = %v, want %v", named, tc.id)
			}
		})
	}
}

// TestKnownSourceIDs covers the helper's multi-kind union behavior.
func TestKnownSourceIDs(t *testing.T) {
	t.Parallel()
	s := store.New()
	gr := &manifest.GitRepository{Name: "g1", Namespace: "ns"}
	oc := &manifest.OCIRepository{Name: "o1", Namespace: "ns"}
	hr := &manifest.HelmRepository{Name: "h1", Namespace: "ns"}
	s.AddObject(gr)
	s.AddObject(oc)
	s.AddObject(hr)

	got := knownSourceIDs(s, manifest.KindGitRepository, manifest.KindOCIRepository)
	if _, ok := got[gr.Named()]; !ok {
		t.Errorf("missing GitRepository entry")
	}
	if _, ok := got[oc.Named()]; !ok {
		t.Errorf("missing OCIRepository entry")
	}
	if _, ok := got[hr.Named()]; ok {
		t.Errorf("HelmRepository should not appear — not in the requested kinds")
	}
	if len(got) != 2 {
		t.Errorf("len = %d, want 2; got = %v", len(got), got)
	}
}
