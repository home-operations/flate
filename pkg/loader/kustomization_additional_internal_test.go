package loader

import (
	"io/fs"
	"path/filepath"
	"slices"
	"testing"
	"testing/fstest"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/kustomizeinput"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

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
		{"late reporting", "apps/new/", Claims(st, root, st.ListAs[*manifest.Kustomization](manifest.FluxKustomizeDomain, manifest.KindKustomization))},
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
	body := r.read(root).body
	assert.FatalIf(t, body == nil, "%v", "document was not decoded")
	assert.Equal(t, body.Namespace, "apps")
	assert.Diff(t, body.Resources, []string{"selected.yaml", "../base"})
	assert.Diff(t, body.ConfigMapGenerator[0].EnvSources, []string{"../settings.env"})
	var paths []string
	spec := r.read(root).body
	for ref := range kustomizeinput.Kustomization(spec) {
		paths = append(paths, ref.Path)
	}
	slices.Sort(paths)
	assert.Diff(t, paths, []string{"../base", "../settings.env", "component", "selected.yaml"})
	for _, input := range []string{"namespace: {invalid: type}", "resources: [{invalid: type}]"} {
		dir := t.TempDir()
		testutil.WriteFile(t, dir, "kustomization.yaml", input)
		invalid := readerForTest(dir)
		{
			invalidBody := invalid.read(dir).body
			assert.FatalIf(t, invalidBody != nil, "invalid typed document was decoded: %s", input)
		}
	}
	componentDir := t.TempDir()
	testutil.WriteFile(t, componentDir, "kustomization.yaml", "kind: Component\n")
	componentReader := readerForTest(componentDir)
	component := componentReader.read(componentDir).body
	assert.FatalIf(t, !isKustomizeComponent(component), "%v", "component lost its default API version")
	again := r.read(root).body
	assert.FatalIf(t, again != body, "%v", "input classification replaced the cached document")
}

func TestDocumentReader_UnreadableBuilder(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	st := store.New()
	st.SetArtifact(manifest.BootstrapSourceID, &store.SourceArtifact{LocalPath: root, FS: fstest.MapFS{
		"plain/unreadable/kustomization.yaml": {Mode: fs.ModeDir | 0o755},
		"plain/unreadable/ks.yaml": {
			Data: []byte(testutil.KustomizationYAML("raw", "apps", "  path: ./leaf\n")),
		},
	}})
	l := New(st)
	l.mountSources()
	dir := filepath.Join(root, "plain", "unreadable")
	builder6 := l.documents.read(dir)
	body, file := builder6.body, builder6.file
	assert.FatalIf(
		t,
		body != nil || file != filepath.Join(dir, "kustomization.yaml"),
		"unreadable builder classified as absent: %v, %q",
		body,
		file,
	)
	{
		absent := l.documents.read(filepath.Join(root, "absent")).file
		assert.FatalIf(t, absent != "", "absent builder classified as present: %q", absent)
	}
	_, err := l.Load(t.Context(), filepath.Join(root, "plain"), nil)
	assert.NoError(t, err)
	l.Admit(root)
	{
		_, ok := l.Index.Lookup(testutil.KustomizationID("apps", "raw"))
		assert.FatalIf(t, ok, "%v", "unreadable builder admitted raw resource")
	}
}

func TestDocumentReader_IndexedSource(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	testutil.WriteFile(t, root, "kustomization.yaml", "namespace: host\n")
	st := store.New()
	id := manifest.NamedResource{
		Group:     manifest.SourceDomain,
		Kind:      manifest.KindGitRepository,
		Namespace: "apps",
		Name:      "indexed",
	}
	st.SetArtifact(id, &store.SourceArtifact{LocalPath: root, FS: fstest.MapFS{
		"kustomization.yaml": {Data: []byte("namespace: authoritative\n")},
	}})
	index := &manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
		id: {File: "source.yaml"},
	}}
	reader := documentReader{cache: map[string]document{}}
	reader.mountSources(st, index)
	body := reader.read(root).body
	assert.FatalIf(
		t,
		body == nil || body.Namespace != "authoritative" || st.GetObject(id) != nil,
		"indexed-only source mount = %v",
		body,
	)
}

func TestLoader_ReplacedProjectedEvidence(t *testing.T) {
	t.Parallel()
	t.Run("membership replacement", func(t *testing.T) {
		t.Parallel()
		root := t.TempDir()
		l := New(store.New())
		l.SourceRoot = root
		for _, included := range []bool{true, false, true} {
			resources := "namespace: apps\nresources: []\n"
			if included {
				resources = "namespace: apps\nresources: [owner.yaml]\n"
			}
			l.Store.SetArtifact(manifest.BootstrapSourceID, &store.SourceArtifact{LocalPath: root, FS: fstest.MapFS{
				"app/kustomization.yaml": {Data: []byte(resources)},
				"app/owner.yaml": {
					Data: []byte(testutil.KustomizationYAML("owner", "apps", "  path: ./target\n")),
				},
				"target/kustomization.yaml": {Data: []byte("resources: [cm.yaml]\n")},
				"target/cm.yaml": {
					Data: []byte("apiVersion: v1\nkind: ConfigMap\nmetadata: {name: target}\n"),
				},
			}})
			_, err := l.Load(t.Context(), root, nil)
			assert.NoError(t, err)
			l.Admit(root)
			assert.NoError(t, l.FinalizeIndex(t.Context(), root, true))
			id := testutil.KustomizationID("apps", "owner")
			assert.FatalIf(t, l.AdmitsRoot(
				l.IndexedObject(id, l.Index.Records[id]).Obj,
				filepath.Join(root, "app", "owner.yaml"),
			) != included ||
				(l.Store.GetObject(id) != nil) != included, "replacement membership retained another view: included=%t", included)
		}
	})

	root := t.TempDir()
	l := New(store.New())
	l.SourceRoot = root
	previousCandidates := map[manifest.NamedResource]manifest.BaseManifest{}
	previousPublished := map[manifest.NamedResource]manifest.BaseManifest{}
	for _, version := range []string{"first", "second"} {
		l.Store.SetArtifact(manifest.BootstrapSourceID, &store.SourceArtifact{LocalPath: root, FS: fstest.MapFS{
			"kustomization.yaml": {Data: []byte("namespace: apps\nresources: [objects.yaml]\n")},
			"objects.yaml": {
				Data: []byte(
					testutil.KustomizationYAML(
						"child",
						"",
						"  path: ./"+version+"\n",
					) + "---\napiVersion: source.toolkit.fluxcd.io/v1\nkind: GitRepository\nmetadata: {name: source}\nspec: {url: https://fixture.invalid/" + version + "}\n",
				),
			},
		}})
		_, err := l.Load(t.Context(), root, nil)
		assert.NoError(t, err)
		l.Admit(root)
		owners := l.Kustomizations()
		assert.FatalIf(t, len(owners) != 1 || owners[0].Obj.(*manifest.Kustomization).Namespace != "apps" ||
			owners[0].Obj.(*manifest.Kustomization).Path != "./"+version, "replacement kept stale owner: %+v", owners)
		sources := 0
		for source := range l.Sources(t.Context()) {
			sources++
			repo := source.Obj.(*manifest.GitRepository)
			assert.FatalIf(
				t,
				repo.Namespace != "apps" || repo.URL != "https://fixture.invalid/"+version,
				"replacement kept stale source: %+v",
				repo,
			)
		}
		assert.Equal(t, sources, 1)
		for _, id := range []manifest.NamedResource{
			testutil.KustomizationID("apps", "child"),
			{Group: manifest.SourceDomain, Kind: manifest.KindGitRepository, Namespace: "apps", Name: "source"},
		} {
			candidate, published := l.candidates[id].Obj, l.Store.GetObject(id)
			assert.FatalIf(t, candidate == nil || published != candidate || candidate == previousCandidates[id] ||
				published == previousPublished[id], "source replacement did not refresh %s: candidate=%p, published=%p", id, candidate, published)
			previousCandidates[id], previousPublished[id] = candidate, published
		}
		{
			err := l.FinalizeIndex(t.Context(), root, true)
			assert.FatalIf(t, err != nil, "%v", err)
		}
	}
}

func TestDocumentReader_CopiedSourceViews(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	l := New(store.New())
	reader := l.documents
	input := &store.SourceArtifact{LocalPath: root, FS: fstest.MapFS{"probe": {Data: []byte("shared")}}}
	reader.setSources(map[string]*store.SourceArtifact{root: input})
	inputFile := new(l.documents).input(filepath.Join(root, "probe"))
	view, name := inputFile.fs, inputFile.name
	data, err := fs.ReadFile(view, name)
	assert.FatalIf(
		t,
		err != nil || string(data) != "shared",
		"copied reader lost its mounted source view: %q, %v",
		data,
		err,
	)
}
