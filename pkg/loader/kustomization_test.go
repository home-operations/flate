package loader

import (
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"testing"
	"testing/fstest"

	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source/gitrepo"
	"github.com/home-operations/flate/pkg/source/gittree"
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
	t.Run("native_reader_ownership", func(t *testing.T) {
		root := t.TempDir()
		testutil.WriteFile(t, root, "kustomization.yaml", "namespace: native\n")
		location, err := gitrepo.Locate(t.Context(), root, root)
		if err != nil {
			t.Fatal(err)
		}
		artifact, err := gittree.AdmitDirectory(t.Context(), location)
		if err != nil {
			t.Fatal(err)
		}
		st := store.New()
		st.SetArtifact(manifest.BootstrapSourceID, artifact)
		r := documentReader{}
		r.mountSources(st)
		if r.views[root] != artifact.FS {
			t.Fatal("temporary reader constructed a private operation view")
		}
		l := New(st)
		l.mountSources()
		if l.documents.views[root] == artifact.FS || st.GetArtifact(manifest.BootstrapSourceID) != artifact {
			t.Fatal("loader did not own its private view independently of the artifact")
		}
	})
	t.Run("replaced source", func(t *testing.T) {
		for _, native := range []bool{false, true} {
			name := "map"
			if native {
				name = "native"
			}
			t.Run(name, func(t *testing.T) {
				st := store.New()
				l := New(st)
				l.SourceRoot = committed
				var previous fs.FS
				var replacement *store.SourceArtifact
				for _, name := range []string{"first", "second"} {
					input := fstest.MapFS{
						"kustomization.yaml": {Data: []byte("namespace: " + name + "\nresources: [ks.yaml, child]\n")},
						"ks.yaml":            {Data: []byte("apiVersion: kustomize.toolkit.fluxcd.io/v1\nkind: Kustomization\nmetadata: {name: " + name + "}\nspec: {path: ./apps}\n")},
					}
					if name == "second" {
						input["child/kustomization.yaml"] = &fstest.MapFile{Data: []byte("namespace: replacement\n")}
					}
					artifact := &store.SourceArtifact{LocalPath: committed, FS: input}
					if native {
						backing := t.TempDir()
						for file, contents := range input {
							testutil.WriteFile(t, backing, file, string(contents.Data))
						}
						location, err := gitrepo.Locate(t.Context(), backing, backing)
						if err != nil {
							t.Fatal(err)
						}
						artifact, err = gittree.AdmitDirectory(t.Context(), location)
						if err != nil {
							t.Fatal(err)
						}
						artifact.LocalPath = committed
					}
					st.SetArtifact(manifest.BootstrapSourceID, artifact)
					if n, err := l.Load(t.Context(), committed); err != nil || n != 1 {
						t.Fatalf("%s Load = %d, %v; want one object", name, n, err)
					}
					view := l.documents.views[committed]
					if native {
						if view == artifact.FS || view == previous {
							t.Fatal("native replacement did not create a new private operation view")
						}
					} else if _, ok := view.(fstest.MapFS); !ok {
						t.Fatal("non-native replacement wrapped the supplied filesystem")
					}
					previous = view
					replacement = artifact
					if k, _ := l.documents.read(committed); k == nil || k.Namespace != name {
						t.Fatalf("%s source retained a stale document: %v", name, k)
					}
					child, _ := l.documents.read(filepath.Join(committed, "child"))
					if name == "first" && child != nil || name == "second" && (child == nil || child.Namespace != "replacement") {
						t.Fatalf("%s source retained a stale miss: %v", name, child)
					}
					id := manifest.NamedResource{Group: manifest.FluxKustomizeDomain, Kind: manifest.KindKustomization, Name: name}
					if _, ok := l.Index.Lookup(id); !ok || st.GetObject(id) == nil {
						t.Fatalf("%s replacement object was not stored and indexed", name)
					}
					if name == "second" {
						id.Name = "first"
						if _, ok := l.Index.Lookup(id); ok || st.GetObject(id) != nil {
							t.Fatal("replacement retained the previous source's object or index")
						}
					}
				}
				st.SetArtifact(manifest.BootstrapSourceID, &store.SourceArtifact{LocalPath: committed})
				if _, err := l.Load(t.Context(), committed); !errors.Is(err, manifest.ErrInput) {
					t.Fatalf("nil replacement read host content: %v", err)
				}
				if len(l.Index.Records) != 0 || len(st.ListObjects(manifest.FluxKustomizeDomain, manifest.KindKustomization)) != 0 {
					t.Fatal("nil replacement retained old objects or index records")
				}
				st.SetArtifact(manifest.BootstrapSourceID, replacement)
				if n, err := l.Load(t.Context(), committed); err != nil || n != 1 {
					t.Fatalf("remounted source Load = %d, %v; want one object", n, err)
				}
				l.documents.read(committed)
				l.Index.Errors = map[string]error{"host.yaml": manifest.ErrInput}
				st.SetArtifact(manifest.BootstrapSourceID, nil)
				l.mountSources()
				if l.documents.mountedRoot(committed) != "" || len(l.documents.cache) != 0 || len(l.Index.Errors) != 0 || len(l.Index.Records) != 0 || len(st.ListObjects(manifest.FluxKustomizeDomain, manifest.KindKustomization)) != 0 {
					t.Fatal("removed artifact retained a stale binding, document or index error")
				}
			})
		}
	})
	t.Run("late_nested_source", func(t *testing.T) {
		root, unrelated := t.TempDir(), t.TempDir()
		for _, dir := range []string{root, unrelated} {
			testutil.WriteFile(t, dir, "kustomization.yaml", "resources: [ks.yaml]\n")
			testutil.WriteFile(t, dir, "ks.yaml", "apiVersion: kustomize.toolkit.fluxcd.io/v1\nkind: Kustomization\nmetadata: {name: parent}\nspec: {path: ./apps}\n")
		}
		testutil.WriteFile(t, root, "child/kustomization.yaml", "resources: [ks.yaml]\n")
		testutil.WriteFile(t, root, "child/ks.yaml", "apiVersion: kustomize.toolkit.fluxcd.io/v1\nkind: Kustomization\nmetadata: {name: stale}\nspec: {path: ./apps}\n")
		st := store.New()
		admitForTest(st, root)
		location, err := gitrepo.Locate(t.Context(), unrelated, unrelated)
		if err != nil {
			t.Fatal(err)
		}
		artifact, err := gittree.AdmitDirectory(t.Context(), location)
		if err != nil {
			t.Fatal(err)
		}
		repo := &manifest.GitRepository{Name: "unrelated"}
		st.AddObject(repo)
		st.SetArtifact(repo.Named(), artifact)
		l := New(st)
		for _, dir := range []string{root, filepath.Join(root, "child"), unrelated} {
			if _, err := l.Load(t.Context(), dir); err != nil {
				t.Fatal(err)
			}
		}
		view := l.documents.views[unrelated]
		observed, err := fs.Stat(view, ".")
		if err != nil {
			t.Fatal(err)
		}
		backing := t.TempDir()
		testutil.WriteFile(t, backing, "kustomization.yaml", "resources: [ks.yaml]\n")
		testutil.WriteFile(t, backing, "ks.yaml", "apiVersion: kustomize.toolkit.fluxcd.io/v1\nkind: Kustomization\nmetadata: {name: nested}\nspec: {path: ./apps}\n")
		location, err = gitrepo.Locate(t.Context(), backing, backing)
		if err != nil {
			t.Fatal(err)
		}
		artifact, err = gittree.AdmitDirectory(t.Context(), location)
		if err != nil {
			t.Fatal(err)
		}
		artifact.LocalPath = filepath.Join(root, "child")
		nested := &manifest.GitRepository{Name: "nested"}
		st.AddObject(nested)
		st.SetArtifact(nested.Named(), artifact)
		if n, err := l.Load(t.Context(), filepath.Join(root, "child")); err != nil || n != 1 {
			t.Fatalf("late mount Load = %d, %v; want one object", n, err)
		}
		if l.documents.views[artifact.LocalPath] == artifact.FS || l.documents.mountedRoot(filepath.Join(root, "child/ks.yaml")) != artifact.LocalPath {
			t.Fatal("late native mount did not bind its own private operation view")
		}
		id := manifest.NamedResource{Group: manifest.FluxKustomizeDomain, Kind: manifest.KindKustomization, Name: "nested"}
		record, indexed := l.Index.Lookup(id)
		if !indexed || record.Path != filepath.Join(root, "child/ks.yaml") || st.GetObject(id) == nil {
			t.Fatalf("nested mount did not supply its object and index: %+v", record)
		}
		id.Name = "stale"
		if _, indexed := l.Index.Lookup(id); indexed || st.GetObject(id) != nil {
			t.Fatal("late nested mount retained parent-backed content")
		}
		retained, err := fs.Stat(l.documents.views[unrelated], ".")
		if err != nil || l.documents.views[unrelated] != view || retained != observed {
			t.Fatal("late nested mount replaced unrelated view observations")
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
