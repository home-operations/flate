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
	"github.com/home-operations/flate/internal/kustomizeinput"
	"github.com/home-operations/flate/internal/kustpath"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source/gitrepo"
	"github.com/home-operations/flate/pkg/source/gittree"
	"github.com/home-operations/flate/pkg/store"
)

func TestResolve_File(t *testing.T) {
	t.Parallel()
	tmp := t.TempDir()
	base, absolute := filepath.Join(tmp, "cluster/apps/foo"), filepath.Join(tmp, "etc/values.yaml")
	root := filepath.VolumeName(tmp) + string(filepath.Separator)
	cases := []struct {
		name    string
		base    string
		rel     string
		wantAbs string
		wantOK  bool
	}{
		{"relative under base", base, "data/values.yaml", filepath.Join(base, "data/values.yaml"), true},
		{"traversal parent", base, "../escape.yaml", "", false},
		{"traversal deep", base, "../../etc/passwd", "", false},
		{"traversal mixed", base, "sub/../../escape", "", false},
		{"sibling prefix escape", base, "../foobar/values.yaml", "", false},
		{"absolute passes through", base, absolute, absolute, true},
		{"absolute cleaned", base, filepath.Dir(absolute) + string(filepath.Separator) + "sub/../values.yaml", absolute, true},
		{"empty rejected", base, "", "", false},
		{"dot", base, ".", base, true},
		{"dot prefix", base, "..foo", filepath.Join(base, "..foo"), true},
		{"url lexical", base, "https://example.com/x", filepath.Join(base, "https:/example.com/x"), true},
		{"relative base", "apps", "child/file", filepath.FromSlash("apps/child/file"), true},
		{"dot base child", ".", "child", "", false},
		{"empty base child", "", "child", "", false},
		{"dot base itself", ".", ".", ".", true},
		{"empty base itself", "", ".", ".", true},
		{"root base child", root, "child", "", false},
		{"root base itself", root, ".", root, true},
		{"root base absolute", root, absolute, absolute, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			abs, ok := kustpath.Resolve(tc.base, tc.rel, kustpath.File)
			if ok != tc.wantOK {
				t.Fatalf("kustpath.Resolve(%q, %q) ok = %v, want %v", tc.base, tc.rel, ok, tc.wantOK)
			}
			if ok && abs != tc.wantAbs {
				t.Errorf("kustpath.Resolve(%q, %q) = %q, want %q", tc.base, tc.rel, abs, tc.wantAbs)
			}
		})
	}
}

func TestResolve_Directory(t *testing.T) {
	t.Parallel()
	tmp := t.TempDir()
	base := filepath.Join(tmp, "cluster/apps/foo")
	cases := []struct {
		name    string
		base    string
		rel     string
		wantAbs string
		wantOK  bool
	}{
		{"relative under base", base, "sub/dir", filepath.Join(base, "sub/dir"), true},
		{"traversal parent resolves", base, "../bar", filepath.Join(base, "../bar"), true},
		{"traversal deep resolves", base, "../../../deploy/x", filepath.Join(tmp, "deploy/x"), true},
		{"absolute rejected", base, filepath.Join(tmp, "etc/x"), "", false},
		{"url rejected", base, "https://example.com/x", "", false},
		{"empty rejected", base, "", "", false},
		{"dot", base, ".", base, true},
		{"dot prefix", base, "..foo", filepath.Join(base, "..foo"), true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			abs, ok := kustpath.Resolve(tc.base, tc.rel, kustpath.Directory)
			if ok != tc.wantOK {
				t.Fatalf("kustpath.Resolve(%q, %q) ok = %v, want %v", tc.base, tc.rel, ok, tc.wantOK)
			}
			if ok && abs != tc.wantAbs {
				t.Errorf("kustpath.Resolve(%q, %q) = %q, want %q", tc.base, tc.rel, abs, tc.wantAbs)
			}
		})
	}
}

// TestFinalizeIndex_Excluded pins the #777 orphan signal: a manifest is
// "unreferenced" only when its own directory's kustomization.yaml exists and does
// not list it. A listed file, or a directory with no kustomization at all (a
// loose top-level entry), is not.
func TestFinalizeIndex_Excluded(t *testing.T) {
	t.Parallel()
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
			t.Parallel()
			l := loaderFixture(store.New(), nil)
			id := testutil.KustomizationID("", "stray")
			l.Index.Records[id] = manifest.DiscoveryRecord{Path: filepath.Join(dir, tc.file), File: tc.file}
			rendererFixture(t, l, dir)
			if got := l.Index.Records[id].Excluded; got != tc.want {
				t.Errorf("Excluded(%q) = %v, want %v", tc.file, got, tc.want)
			}
		})
	}
	for _, version := range []string{"", "/", "example.test/", "example.test/v1/extra"} {
		t.Run("unlisted ResourceSet/"+version, func(t *testing.T) {
			t.Parallel()
			root := t.TempDir()
			testutil.WriteFile(t, root, "kustomization.yaml", "resources: [selected.yaml]\n")
			testutil.WriteFile(t, root, "selected.yaml", "apiVersion: source.toolkit.fluxcd.io/v1\nkind: GitRepository\nmetadata: {name: selected}\nspec: {url: https://example.test/repo.git}\n")
			testutil.WriteFile(t, root, "excluded.yaml", "apiVersion: fluxcd.controlplane.io/v1\nkind: ResourceSet\nmetadata: {name: excluded}\nspec:\n  dependsOn: [{apiVersion: '"+version+"', kind: Kustomization, name: missing}]\n")
			l := New(store.New())
			_, err := loadForTest(t.Context(), t, l, root)
			if err != nil {
				t.Fatal(err)
			}
			if err := l.FinalizeIndex(t.Context(), root); err != nil {
				t.Fatal(err)
			}
			if len(l.Index.Errors) != 0 || l.Store.GetObject(manifest.NamedResource{Group: manifest.SourceDomain, Kind: manifest.KindGitRepository, Name: "selected"}) == nil {
				t.Fatalf("unlisted input affected selected source: errors=%v", l.Index.Errors)
			}
		})
	}
}

func TestLoader_KustomizationFilenames(t *testing.T) {
	t.Parallel()
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
			t.Parallel()
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
	t.Parallel()
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
	t.Run("retained objects through skips and errors", func(t *testing.T) {
		t.Parallel()
		root := t.TempDir()
		reader := documentReader{views: map[string]fs.FS{root: os.DirFS(root)}}
		testutil.WriteFile(t, root, "mixed.yaml", `apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata: {name: retained}
spec: {path: ./apps, sourceRef: {kind: GitRepository, name: source}}
---
plain: data
---
apiVersion: v1
kind: ConfigMap
metadata: 7
---
apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata: {name: '${TEMPLATE}'}
---
apiVersion: example.test/v1
kind: Widget
metadata: {name: ignored}
---
apiVersion: v1
kind: ConfigMap
metadata: {name: values}
data: {VALUE: keep}
---
apiVersion: example.test/v1/extra
kind: Widget
metadata: {name: invalid}
---
apiVersion: v1
kind: ConfigMap
metadata: {name: pending}
`)
		var objects []manifest.BaseManifest
		opts := manifest.ParseDocOptions{WipeSecrets: true, DefaultNamespace: "apps"}
		err := reader.decodeFileObjects(filepath.Join(root, "mixed.yaml"), opts, false, func(obj manifest.BaseManifest) {
			objects = append(objects, obj)
		})
		if !errors.Is(err, manifest.ErrInput) || !errors.Is(err, manifest.ErrFlux) || len(objects) != 2 {
			t.Fatalf("objects=%v err=%v", objects, err)
		}
		k := objects[0].(*manifest.Kustomization)
		c := objects[1].(*manifest.ConfigMap)
		before, err := k.RenderDocument()
		assert.NoError(t, err)
		testutil.WriteFile(t, root, "next.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: next}\ndata: {VALUE: replaced}\n")
		_, err = reader.parseFile(filepath.Join(root, "next.yaml"), opts)
		assert.NoError(t, err)
		assert.Equal(t, k.Namespace, "apps")
		after, err := k.RenderDocument()
		assert.NoError(t, err)
		assert.Diff(t, after, before)
		value, found, err := c.Value("VALUE")
		assert.NoError(t, err)
		assert.Equal(t, found, true)
		assert.Equal(t, value, "keep")
	})

	t.Run("load missing mounted child", func(t *testing.T) {
		t.Parallel()
		child := filepath.Join(committed, "child")
		testutil.WriteFile(t, child, "ks.yaml", testutil.KustomizationYAML("decoy", "", "  {path: ./apps}\n"))
		st := store.New()
		input := &store.SourceArtifact{LocalPath: committed, FS: fstest.MapFS{
			"Kustomization": {Data: []byte("namespace: committed\n")},
		}}
		st.SetArtifact(manifest.BootstrapSourceID, input)
		l := New(st)
		if _, err := loadForTest(t.Context(), t, l, child); !errors.Is(err, fs.ErrNotExist) {
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
		t.Parallel()
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
		t.Parallel()
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
		st.SetArtifact(wrongArtifact.Named(), &store.Emission{})
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
	t.Run("native_reader_ownership", func(t *testing.T) {
		t.Parallel()
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
		t.Parallel()
		for _, native := range []bool{false, true} {
			name := "map"
			if native {
				name = "native"
			}
			t.Run(name, func(t *testing.T) {
				t.Parallel()
				st := store.New()
				l := New(st)
				l.SourceRoot = committed
				var previous fs.FS
				var replacement *store.SourceArtifact
				for _, name := range []string{"first", "second"} {
					input := fstest.MapFS{
						"kustomization.yaml": {Data: []byte("namespace: " + name + "\nresources: [ks.yaml, child]\n")},
						"ks.yaml":            {Data: []byte(testutil.KustomizationYAML(name, "", "  {path: ./apps}\n"))},
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
					if n, err := loadForTest(t.Context(), t, l, committed); err != nil || n != 1 {
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
					id := testutil.KustomizationID(name, name)
					if _, ok := l.Index.Lookup(id); !ok || st.GetObject(id) == nil {
						t.Fatalf("%s replacement object was not stored and indexed", name)
					}
					if name == "second" {
						id.Name, id.Namespace = "first", "first"
						if _, ok := l.Index.Lookup(id); ok || st.GetObject(id) != nil {
							t.Fatal("replacement retained the previous source's object or index")
						}
					}
				}
				st.SetArtifact(manifest.BootstrapSourceID, &store.SourceArtifact{LocalPath: committed})
				if _, err := loadForTest(t.Context(), t, l, committed); !errors.Is(err, manifest.ErrInput) {
					t.Fatalf("nil replacement read host content: %v", err)
				}
				if len(l.Index.Records) != 0 || len(st.ListObjects(manifest.FluxKustomizeDomain, manifest.KindKustomization)) != 0 {
					t.Fatal("nil replacement retained old objects or index records")
				}
				st.SetArtifact(manifest.BootstrapSourceID, replacement)
				if n, err := loadForTest(t.Context(), t, l, committed); err != nil || n != 1 {
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
		t.Parallel()
		root, unrelated := t.TempDir(), t.TempDir()
		for _, dir := range []string{root, unrelated} {
			testutil.WriteFile(t, dir, "kustomization.yaml", "resources: [ks.yaml]\n")
			testutil.WriteFile(t, dir, "ks.yaml", testutil.KustomizationYAML("parent", "", "  {path: ./apps}\n"))
		}
		testutil.WriteFile(t, root, "child/kustomization.yaml", "resources: [ks.yaml]\n")
		testutil.WriteFile(t, root, "child/ks.yaml", testutil.KustomizationYAML("stale", "", "  {path: ./apps}\n"))
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
			if _, err := loadForTest(t.Context(), t, l, dir); err != nil {
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
		testutil.WriteFile(t, backing, "ks.yaml", testutil.KustomizationYAML("nested", "", "  {path: ./apps}\n"))
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
		if n, err := loadForTest(t.Context(), t, l, filepath.Join(root, "child")); err != nil || n != 1 {
			t.Fatalf("late mount Load = %d, %v; want one object", n, err)
		}
		if l.documents.views[artifact.LocalPath] == artifact.FS || l.documents.mountedRoot(filepath.Join(root, "child/ks.yaml")) != artifact.LocalPath {
			t.Fatal("late native mount did not bind its own private operation view")
		}
		id := testutil.KustomizationID("", "nested")
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
	t.Parallel()
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
			t.Parallel()
			for _, claim := range tc.claims {
				if claim.Prefix == tc.component {
					return
				}
			}
			t.Fatalf("missing %s in %v", tc.component, tc.claims)
		})
	}
}

func TestDocumentReader_InputSnapshot(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	testutil.WriteFile(t, root, "kustomization.yaml", `defaults: &defaults
  namespace: apps
  bases: [../base]
  configMapGenerator:
    - name: settings
      env: ../settings.env
<<: *defaults
Resources: [ignored.yaml]
resources: &resources [selected.yaml]
components: [component]
`)
	r := readerForTest(root)
	body, _ := r.read(root)
	if body == nil {
		t.Fatal("document was not decoded")
	}
	assert.Equal(t, body.Namespace, "apps")
	assert.Diff(t, body.Resources, []string{"selected.yaml", "../base"})
	assert.Diff(t, body.ConfigMapGenerator[0].EnvSources, []string{"../settings.env"})
	var paths []string
	spec, _ := r.read(root)
	for ref := range kustomizeinput.Kustomization(spec) {
		paths = append(paths, ref.Path)
	}
	slices.Sort(paths)
	assert.Diff(t, paths, []string{"../base", "../settings.env", "component", "selected.yaml"})
	for _, input := range []string{"namespace: {invalid: type}", "resources: [{invalid: type}]"} {
		dir := t.TempDir()
		testutil.WriteFile(t, dir, "kustomization.yaml", input)
		invalid := readerForTest(dir)
		if body, _ := invalid.read(dir); body != nil {
			t.Fatalf("invalid typed document was decoded: %s", input)
		}
	}
	componentDir := t.TempDir()
	testutil.WriteFile(t, componentDir, "kustomization.yaml", "kind: Component\n")
	componentReader := readerForTest(componentDir)
	component, _ := componentReader.read(componentDir)
	if !isKustomizeComponent(component) {
		t.Fatal("component lost its default API version")
	}
	again, _ := r.read(root)
	if again != body {
		t.Fatal("input classification replaced the cached document")
	}
}

func TestDocumentReader_UnreadableBuilder(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	st := store.New()
	st.SetArtifact(manifest.BootstrapSourceID, &store.SourceArtifact{LocalPath: root, FS: fstest.MapFS{
		"plain/unreadable/kustomization.yaml": {Mode: fs.ModeDir | 0o755},
		"plain/unreadable/ks.yaml":            {Data: []byte(testutil.KustomizationYAML("raw", "apps", "  path: ./leaf\n"))},
	}})
	l := New(st)
	l.mountSources()
	dir := filepath.Join(root, "plain/unreadable")
	body, file := l.documents.read(dir)
	if body != nil || file != filepath.Join(dir, "kustomization.yaml") {
		t.Fatalf("unreadable builder classified as absent: %v, %q", body, file)
	}
	if _, absent := l.documents.read(filepath.Join(root, "absent")); absent != "" {
		t.Fatalf("absent builder classified as present: %q", absent)
	}
	_, err := l.Load(t.Context(), filepath.Join(root, "plain"))
	assert.NoError(t, err)
	l.Admit(root)
	if _, ok := l.Index.Lookup(testutil.KustomizationID("apps", "raw")); ok {
		t.Fatal("unreadable builder admitted raw resource")
	}
}

func TestDocumentReader_IndexedSource(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	testutil.WriteFile(t, root, "kustomization.yaml", "namespace: host\n")
	st := store.New()
	id := manifest.NamedResource{Group: manifest.SourceDomain, Kind: manifest.KindGitRepository, Namespace: "apps", Name: "indexed"}
	st.SetArtifact(id, &store.SourceArtifact{LocalPath: root, FS: fstest.MapFS{
		"kustomization.yaml": {Data: []byte("namespace: authoritative\n")},
	}})
	index := &manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
		id: {File: "source.yaml"},
	}}
	reader := documentReader{cache: map[string]document{}}
	reader.mountSources(st, index)
	body, _ := reader.read(root)
	if body == nil || body.Namespace != "authoritative" || st.GetObject(id) != nil {
		t.Fatalf("indexed-only source mount = %v", body)
	}
}

func TestLoader_ReplacedProjectedEvidence(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	l := New(store.New())
	l.SourceRoot = root
	for _, version := range []string{"first", "second"} {
		l.Store.SetArtifact(manifest.BootstrapSourceID, &store.SourceArtifact{LocalPath: root, FS: fstest.MapFS{
			"kustomization.yaml": {Data: []byte("namespace: apps\nresources: [objects.yaml]\n")},
			"objects.yaml":       {Data: []byte(testutil.KustomizationYAML("child", "", "  path: ./"+version+"\n") + "---\napiVersion: source.toolkit.fluxcd.io/v1\nkind: GitRepository\nmetadata: {name: source}\nspec: {url: https://fixture.invalid/" + version + "}\n")},
		}})
		_, err := l.Load(t.Context(), root)
		assert.NoError(t, err)
		l.Admit(root)
		owners := l.Kustomizations()
		if len(owners) != 1 || owners[0].Namespace != "apps" || owners[0].Path != "./"+version {
			t.Fatalf("replacement kept stale owner: %+v", owners)
		}
		sources := 0
		for source := range l.Sources() {
			sources++
			repo := source.(*manifest.GitRepository)
			if repo.Namespace != "apps" || repo.URL != "https://fixture.invalid/"+version {
				t.Fatalf("replacement kept stale source: %+v", repo)
			}
		}
		assert.Equal(t, sources, 1)
		if err := l.FinalizeIndex(t.Context(), root); err != nil {
			t.Fatal(err)
		}
	}
}
