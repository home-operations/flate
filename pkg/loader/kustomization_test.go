package loader

import (
	"errors"
	"io/fs"
	"path/filepath"
	"testing"
	"testing/fstest"

	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
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
			abs, ok := resolvePath(tc.base, tc.rel)
			if ok != tc.wantOK {
				t.Fatalf("resolvePath(%q, %q) ok = %v, want %v", tc.base, tc.rel, ok, tc.wantOK)
			}
			if ok && abs != tc.wantAbs {
				t.Errorf("resolvePath(%q, %q) = %q, want %q", tc.base, tc.rel, abs, tc.wantAbs)
			}
		})
	}
}

// TestFinalizeIndex_Excluded pins the #777 orphan signal: a manifest is
// "unreferenced" only when its own directory's kustomization.yaml exists and does
// not list it. A listed file, or a directory with no kustomization at all (a
// loose top-level entry), is not.
func TestFinalizeIndex_Excluded(t *testing.T) {
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
			l := loaderFixture(store.New(), nil)
			id := manifest.NamedResource{Kind: manifest.KindKustomization, Name: "stray"}
			l.Index.Records[id] = manifest.DiscoveryRecord{Path: filepath.Join(dir, tc.file), File: tc.file}
			l.FinalizeIndex(dir, nil)
			if got := l.Index.Records[id].Excluded; got != tc.want {
				t.Errorf("Excluded(%q) = %v, want %v", tc.file, got, tc.want)
			}
		})
	}
}

func TestLoader_KustomizationFilenames(t *testing.T) {
	for _, tc := range []struct {
		name   string
		loaded int
	}{
		{"kustomization.yaml", 1},
		{"kustomization.yml", 1},
		{"Kustomization", 1},
		{"kustomization.json", 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root := t.TempDir()
			testutil.WriteFile(t, root, tc.name, "resources: [included.yaml]\nunknownDirective: ignored\n")
			for _, file := range []string{"included.yaml", "unlisted.yaml"} {
				testutil.WriteFile(t, root, file, "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: "+file+"}\n")
			}
			if loaded, err := New(store.New()).Load(t.Context(), root); err != nil || loaded != tc.loaded {
				t.Fatalf("Load = %d, %v; want %d objects", loaded, err, tc.loaded)
			}
		})
	}
}

func TestDocumentReader_SourceViewsAndReuse(t *testing.T) {
	r := documentReader{cache: map[string]document{}}
	working := filepath.Join(t.TempDir(), "apps")
	committed := filepath.Join(t.TempDir(), "apps")
	testutil.WriteFile(t, working, "Kustomization", "namespace: working\n")
	testutil.WriteFile(t, committed, "Kustomization", "namespace: host\n")
	testutil.WriteFile(t, committed, "host.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: host}\n")
	if _, name := r.input(filepath.VolumeName(working) + string(filepath.Separator)); name != "." {
		t.Fatalf("filesystem root name = %q, want dot", name)
	}
	r.views = map[string]fs.FS{committed: fstest.MapFS{
		"Kustomization": {Data: []byte("namespace: committed\n")},
	}}
	first, file := r.read(working)
	other, _ := r.read(committed)
	if first == nil || other == nil || first.Namespace != "working" || other.Namespace != "committed" {
		t.Fatalf("source views were mixed: working=%v committed=%v", first, other)
	}
	if _, err := r.parseFile(filepath.Join(committed, "host.yaml"), manifest.ParseDocOptions{}); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("absent committed file fell through to disk: %v", err)
	}
	testutil.WriteFile(t, working, "Kustomization", "namespace: changed\n")
	if got, gotFile := r.read(working); got != first || gotFile != file {
		t.Fatal("positive result was re-read")
	}
	missing := t.TempDir()
	if got, _ := r.read(missing); got != nil {
		t.Fatal("missing kustomization was discovered")
	}
	testutil.WriteFile(t, missing, "kustomization.yaml", "namespace: late\n")
	if got, _ := r.read(missing); got != nil {
		t.Fatal("negative result was re-read")
	}
}

func TestLoader_ClaimsDocumentLifetime(t *testing.T) {
	root := t.TempDir()
	testutil.WriteFile(t, root, "apps/kustomization.yaml", "components: [old]\n")
	st := store.New()
	ks := &manifest.Kustomization{Name: "apps", Path: "./apps"}
	st.AddObject(ks)
	l := New(st)
	l.Claims(root)
	testutil.WriteFile(t, root, "apps/kustomization.yaml", "components: [new]\n")
	for _, tc := range []struct {
		name, component string
		claims          []manifest.KSClaim
	}{
		{"discovery snapshot", "apps/old/", l.Claims(root)},
		{"late reporting", "apps/new/", Claims(st, root)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for _, claim := range tc.claims {
				if claim.Prefix == tc.component {
					return
				}
			}
			t.Fatalf("missing %s in %v", tc.component, tc.claims)
		})
	}
}
