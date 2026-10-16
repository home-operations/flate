package loader

import (
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"testing/fstest"

	"github.com/home-operations/flate/internal/assert"
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
			id := manifest.NamedResource{Group: "kustomize.toolkit.fluxcd.io", Kind: manifest.KindKustomization, Name: "stray"}
			l.Index.Records[id] = manifest.DiscoveryRecord{Path: filepath.Join(dir, tc.file), File: tc.file}
			rendererFixture(t, l, dir, nil)
			if got := l.Index.Records[id].Excluded; got != tc.want {
				t.Errorf("Excluded(%q) = %v, want %v", tc.file, got, tc.want)
			}
		})
	}
	for _, version := range []string{"", "/", "example.test/", "example.test/v1/extra"} {
		t.Run("unlisted ResourceSet/"+version, func(t *testing.T) {
			root := t.TempDir()
			testutil.WriteFile(t, root, "kustomization.yaml", "resources: [selected.yaml]\n")
			testutil.WriteFile(t, root, "selected.yaml", "apiVersion: source.toolkit.fluxcd.io/v1\nkind: GitRepository\nmetadata: {name: selected}\nspec: {url: https://example.test/repo.git}\n")
			testutil.WriteFile(t, root, "excluded.yaml", "apiVersion: fluxcd.controlplane.io/v1\nkind: ResourceSet\nmetadata: {name: excluded}\nspec:\n  dependsOn: [{apiVersion: '"+version+"', kind: Kustomization, name: missing}]\n")
			l := New(store.New())
			_, err := l.Load(t.Context(), root)
			if err != nil {
				t.Fatal(err)
			}
			l.FinalizeIndex(root, nil)
			if len(l.Index.Errors) != 0 || l.Store.GetObject(manifest.NamedResource{Group: manifest.SourceDomain, Kind: manifest.KindGitRepository, Name: "selected"}) == nil {
				t.Fatalf("unlisted input affected selected source: errors=%v", l.Index.Errors)
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
				testutil.WriteFile(t, root, file, "apiVersion: source.toolkit.fluxcd.io/v1\nkind: GitRepository\nmetadata: {name: "+file+"}\nspec: {url: https://example.test/repo.git}\n")
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
	r.views = map[string]fs.FS{working: os.DirFS(working), committed: fstest.MapFS{
		"Kustomization": {Data: []byte("namespace: committed\n")},
	}}
	first, file := r.read(working)
	other, _ := r.read(committed)
	if first == nil || other == nil || first.Namespace != "working" || other.Namespace != "committed" {
		t.Fatalf("source views were mixed: working=%v committed=%v", first, other)
	}
	if _, err := r.parseFile(filepath.Join(committed, "host.yaml"), manifest.ParseDocOptions{DefaultNamespace: ""}); !errors.Is(err, fs.ErrNotExist) {
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
	t.Run("load missing mounted child", func(t *testing.T) {
		child := filepath.Join(committed, "child")
		testutil.WriteFile(t, child, "ks.yaml", "apiVersion: kustomize.toolkit.fluxcd.io/v1\nkind: Kustomization\nmetadata: {name: decoy}\nspec: {path: ./apps}\n")
		st := store.New()
		input := &store.SourceArtifact{LocalPath: committed, FS: fstest.MapFS{
			"Kustomization": {Data: []byte("namespace: committed\n")},
		}}
		st.SetArtifact(manifest.BootstrapSourceID, input)
		l := New(st)
		if _, err := l.Load(t.Context(), child); !errors.Is(err, fs.ErrNotExist) {
			t.Fatalf("missing mounted child error = %v, want absent source content", err)
		}
		if len(st.ListObjects(manifest.FluxKustomizeDomain, manifest.KindKustomization)) != 0 || len(l.Index.Records) != 0 || len(l.Index.Errors) != 0 {
			t.Fatal("physical decoy supplied an object or indexed evidence")
		}
		if st.GetArtifact(manifest.BootstrapSourceID) != input || l.documents.mountedRoot(child) != committed {
			t.Fatal("missing child replaced the authoritative mounted view")
		}
		if k, _ := l.documents.read(committed); k == nil || k.Namespace != "committed" {
			t.Fatal("virtual view lost authority over physical content")
		}
	})

	t.Run("unavailable source", func(t *testing.T) {
		st := store.New()
		repo := &manifest.GitRepository{Name: "released", Namespace: "n"}
		st.AddObject(repo)
		st.SetArtifact(repo.Named(), &store.SourceArtifact{LocalPath: committed, Key: "pin"})
		r.mountSources(st)
		if _, err := r.parseFile(filepath.Join(committed, "host.yaml"), manifest.ParseDocOptions{DefaultNamespace: ""}); !errors.Is(err, manifest.ErrInput) {
			t.Fatalf("released pin read host content through its coordinate: %v", err)
		}
	})

	t.Run("artifact membership and invalidation", func(t *testing.T) {
		st := store.New()
		reader := documentReader{cache: map[string]document{}}
		root := t.TempDir()
		git := &manifest.GitRepository{Name: "git"}
		oci := &manifest.OCIRepository{Name: "oci"}
		bucket := &manifest.Bucket{Name: "bucket"}
		var mounted []string
		for _, tc := range []struct {
			obj   manifest.BaseManifest
			mount bool
		}{
			{git, true},
			{oci, true},
			{bucket, true},
			{&manifest.ExternalArtifact{Name: "external"}, true},
			{&manifest.HelmRepository{Name: "repository"}, false},
			{&manifest.HelmChartSource{Name: "chart"}, false},
			{&manifest.RawObject{APIVersion: "foreign.example/v1", Kind: manifest.KindGitRepository, Name: "foreign"}, false},
		} {
			id := tc.obj.Named()
			path := filepath.Join(root, id.Name)
			testutil.WriteFile(t, path, "Kustomization", "namespace: host\n")
			st.AddObject(tc.obj)
			st.SetArtifact(id, &store.SourceArtifact{LocalRoot: root, LocalPath: path, FS: fstest.MapFS{
				"Kustomization": {Data: []byte("namespace: " + id.Name + "\n")},
			}})
			if tc.mount {
				mounted = append(mounted, path)
			}
		}
		alias := &manifest.GitRepository{Name: "alias"}
		st.AddObject(alias)
		st.SetArtifact(alias.Named(), st.GetArtifact(git.Named()))
		absentPath := &manifest.GitRepository{Name: "absent-path"}
		st.AddObject(absentPath)
		st.SetArtifact(absentPath.Named(), &store.SourceArtifact{FS: fstest.MapFS{}})
		wrongArtifact := &manifest.Bucket{Name: "wrong-artifact"}
		st.AddObject(wrongArtifact)
		st.SetArtifact(wrongArtifact.Named(), &store.ResourceSetArtifact{})
		bootstrapPath := filepath.Join(root, "bootstrap")
		st.SetArtifact(manifest.BootstrapSourceID, &store.SourceArtifact{LocalPath: bootstrapPath, FS: fstest.MapFS{
			"Kustomization": {Data: []byte("namespace: bootstrap\n")},
		}})
		assert.Equal(t, st.GetObject(manifest.BootstrapSourceID) == nil, true)
		mounted = append(mounted, bootstrapPath)
		slices.Sort(mounted)
		assert.Diff(t, reader.mountSources(st), mounted)
		assert.Equal(t, len(reader.views), len(mounted))
		for _, path := range mounted {
			body, _ := reader.read(path)
			if body == nil || body.Namespace != filepath.Base(path) {
				t.Fatalf("mounted content = %v for %s", body, path)
			}
			if again, _ := reader.read(path); again != body {
				t.Fatal("unchanged mounted document was re-parsed")
			}
		}
		assert.Equal(t, len(reader.mountSources(st)), 0)
		ociPath, bucketPath := filepath.Join(root, "oci"), filepath.Join(root, "bucket")
		late := filepath.Join(ociPath, "late")
		if body, _ := reader.read(late); body != nil {
			t.Fatal("unexpected document before replacement")
		}
		st.SetArtifact(oci.Named(), &store.SourceArtifact{LocalPath: ociPath, FS: fstest.MapFS{
			"Kustomization":      {Data: []byte("namespace: replaced\n")},
			"late/Kustomization": {Data: []byte("namespace: late\n")},
		}})
		st.DeleteObject(bucket.Named())
		assert.Diff(t, reader.mountSources(st), []string{bucketPath, ociPath})
		if body, _ := reader.read(ociPath); body == nil || body.Namespace != "replaced" {
			t.Fatalf("replacement retained stale document: %v", body)
		}
		if body, _ := reader.read(late); body == nil || body.Namespace != "late" {
			t.Fatalf("replacement retained negative cache: %v", body)
		}
		if body, _ := reader.read(bucketPath); body != nil || reader.mountedRoot(bucketPath) != "" {
			t.Fatal("removed source retained a mount or read the physical decoy")
		}
	})
}

func TestLoader_ClaimsDocumentLifetime(t *testing.T) {
	root := t.TempDir()
	testutil.WriteFile(t, root, "apps/kustomization.yaml", "components: [old]\n")
	st := store.New()
	admitForTest(st, root)
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
