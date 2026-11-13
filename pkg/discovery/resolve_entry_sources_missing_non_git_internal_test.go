package discovery

import (
	"archive/tar"
	"context"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"testing/fstest"

	"github.com/go-git/go-git/v6"
	"github.com/go-git/go-git/v6/plumbing"
	"github.com/go-git/go-git/v6/plumbing/object"

	"github.com/home-operations/flate/pkg/selection"

	"github.com/home-operations/flate/internal/assert"

	helmv2 "github.com/fluxcd/helm-controller/api/v2"
	kustomizev1 "github.com/fluxcd/kustomize-controller/api/v1"
	sourcev1 "github.com/fluxcd/source-controller/api/v1"

	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/pkg/change"
	"github.com/home-operations/flate/pkg/loader"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source"
	"github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/store"
)

func TestResolveEntrySources_MissingNonGit(t *testing.T) {
	t.Parallel()
	for _, kind := range []string{manifest.KindGitRepository, manifest.KindOCIRepository, manifest.KindBucket, manifest.KindExternalArtifact, manifest.KindHelmRepository} {
		t.Run(kind, func(t *testing.T) {
			t.Parallel()
			root := t.TempDir()
			st := store.New()
			bootstrap := &store.SourceArtifact{
				LocalPath: root,
				LocalRoot: root,
				FS: fstest.MapFS{
					"apps/file":  {Data: []byte("decoy")},
					"Chart.yaml": {Data: []byte("apiVersion: v2\nname: decoy\nversion: 1.0.0\n")},
				},
			}
			st.SetArtifact(manifest.BootstrapSourceID, bootstrap)
			consumer := &manifest.Kustomization{
				Name:      "consumer",
				Namespace: "apps",
				Path:      "./apps",
				SourceRef: kustomizev1.CrossNamespaceSourceReference{Kind: kind, Name: "missing"},
			}
			st.AddObject(consumer)
			id := consumer.SourceID()
			d := Session{cfg: Config{Path: root, Store: st}}

			count, err := d.resolveTestSources(t.Context(), true)
			assert.NoError(t, err)
			_, artifact, err := d.admission().Resolve(t.Context(), id, nil, nil)
			assert.NoError(t, err)
			if kind == manifest.KindHelmRepository || kind == manifest.KindExternalArtifact {
				assert.Equal(t, count, 0)
				assert.Equal(t, artifact == nil, true)
				assert.Equal(t, st.GetObject(id) == nil, true)
				assert.Equal(t, st.GetArtifact(id, store.ArtifactContent) == nil, true)
				status, ok := st.GetStatus(id)
				assert.Equal(t, ok && status.Status == store.StatusReady, false)
			} else {
				assert.Equal(t, count, 1)
				assert.Equal(t, st.GetObject(id).Named(), id)
				assert.Equal(t, artifact == st.GetArtifact(id, store.ArtifactContent), true)
				assert.Equal(t, artifact.InputAlias, true)
				assert.Equal(t, artifact.Key, "")
				body, readErr := fs.ReadFile(artifact.FS, "apps/file")
				assert.NoError(t, readErr)
				assert.Equal(t, string(body), "decoy")
				status, ok := st.GetStatus(id)
				assert.Equal(t, ok, true)
				assert.Equal(t, status.Status, store.StatusReady)
			}

			assert.FatalIf(
				t,
				st.GetArtifact(manifest.BootstrapSourceID, store.ArtifactContent) != bootstrap,
				"%v",
				"canonical bootstrap changed",
			)
		})
	}
}

func TestResolveEntrySources_Content(t *testing.T) {
	t.Parallel()
	t.Run("shallow unavailable pin", func(t *testing.T) {
		t.Parallel()
		root := t.TempDir()
		repo := testutil.GitInit(t, root)
		testutil.WriteFile(t, root, "apps/file", "shallow input")
		head := testutil.GitCommitAll(
			t,
			repo,
			"head",
			&git.CommitOptions{Author: &object.Signature{Name: "t", Email: "t@e"}},
		)
		assert.NoError(t, repo.Storer.SetShallow([]plumbing.Hash{head}))
		st := store.New()
		declared := &manifest.GitRepository{
			Name: "pinned", Namespace: "sources", URL: "https://fixture.invalid/cluster",
			Reference: &sourcev1.GitRepositoryRef{Commit: strings.Repeat("1", 40)},
		}
		st.AddObject(declared)
		consumer := &manifest.Kustomization{
			Name:      "consumer",
			Namespace: "apps",
			Path:      "./apps",
			SourceRef: kustomizev1.CrossNamespaceSourceReference{
				Kind:      manifest.KindOCIRepository,
				Name:      "arbitrary",
				Namespace: "other",
			},
		}
		st.AddObject(consumer)
		d := Session{cfg: Config{Path: root, RepoRoot: root, Store: st, SelfURLs: []string{declared.URL}}}
		_, err := d.resolveTestSources(t.Context(), true)
		assert.NoError(t, err)
		assert.Equal(t, st.GetArtifact(declared.Named(), store.ArtifactContent) == nil, true)
		assert.Equal(t, st.GetObject(declared.Named()) == declared, true)
		warnings := st.Warnings()
		assert.Equal(t, len(warnings), 1)
		assert.Equal(t, warnings[0].Category, manifest.WarnSourceRefUnavailable)
		assert.Equal(t, warnings[0].Resource, declared.Named())
		artifact, ok := st.GetArtifact(consumer.SourceID(), store.ArtifactContent).(*store.SourceArtifact)
		assert.Equal(t, ok, true)
		assert.Equal(t, artifact.InputAlias, true)
		body, err := fs.ReadFile(artifact.FS, "apps/file")
		assert.NoError(t, err)
		assert.Equal(t, string(body), "shallow input")
	})
	owner := cachetest.New(t, "", cache.New)
	input, _, key, err := owner.Acquire(
		t.Context(),
		cache.ArchiveKey{Policy: "fixed", Content: t.Name()},
		&cache.Policy{MaxAge: cache.Forever},
		func(_ context.Context, target cache.ProducerInput) (cache.ProducerResult, error) {
			writer := tar.NewWriter(target)
			if err := writer.AddFS(
				fstest.MapFS{"apps/file": {Data: []byte("selected")}, "other/file": {Data: []byte("other")}},
			); err != nil {
				return cache.ProducerResult{}, err
			}
			return cache.ProducerResult{}, writer.Close()
		},
	)

	assert.FatalIf(t, err != nil, "%v", err)

	bootstrap := &store.SourceArtifact{
		FS:        input,
		Key:       key,
		Content:   cache.AcquisitionIdentity(input),
		LocalRoot: "/input",
		LocalPath: "/input",
		Revision:  "first",
	}
	obj := &manifest.GitRepository{Name: "alias", Namespace: "apps"}
	candidate := entryResolution{input: bootstrap, object: obj, git: obj}

	for _, phase := range []string{"warm", "metadata", "sparse", "mutable"} {
		t.Run(phase, func(t *testing.T) {
			t.Parallel()
			testResolveEntrySourcesContentCase(
				t,
				testResolveEntrySourcesContentCaseInputs{
					bootstrap: &bootstrap,
					obj:       &obj,
					candidate: &candidate,
					phase:     &phase,
				},
			)
		},
		)
	}
}

func TestResolveEntrySources_IndexedDeclarations(t *testing.T) {
	t.Parallel()
	t.Run("outside selected entry", func(t *testing.T) {
		t.Parallel()
		root := t.TempDir()
		testutil.WriteFile(t, root, "entry/consumer.yaml", testutil.KustomizationYAML("consumer", "apps",
			"  path: ./payload\n  sourceRef: {kind: OCIRepository, name: outside, namespace: sources}\n"))
		testutil.WriteFile(
			t,
			root,
			"outside/source.yaml",
			"apiVersion: source.toolkit.fluxcd.io/v1\nkind: OCIRepository\nmetadata: {name: outside, namespace: sources}\nspec: {url: 'oci://fixture.invalid/actual'}\n",
		)
		st := store.New()
		l := loader.New(st)
		t.Cleanup(l.Close)
		l.SourceRoot = root
		requester := testutil.KustomizationID("apps", "consumer")
		configureSelection(t, l, selection.Request{Roots: []manifest.NamedResource{requester}})
		d := Session{cfg: Config{Path: filepath.Join(root, "entry"), RepoRoot: root, Store: st}, loader: l}
		_, err := d.resolveTestSources(t.Context(), false)
		assert.NoError(t, err)
		_, err = l.Load(t.Context(), root, manifest.NamedResource{}, nil)
		assert.NoError(t, err)
		assert.NoError(t, l.Admit(t.Context(), root))
		id := manifest.NamedResource{
			Group:     manifest.SourceDomain,
			Kind:      manifest.KindOCIRepository,
			Namespace: "sources",
			Name:      "outside",
		}
		assert.Equal(t, l.CatalogObject(id) != nil, true)
		before := l.Options.Session.Snapshot().Output(id)
		assert.Equal(t, before, false)
		count, err := d.resolveTestSources(t.Context(), true)
		assert.NoError(t, err)
		assert.Equal(t, count, 0)
		assert.Equal(t, st.GetArtifact(id, store.ArtifactContent) == nil, true)
		assert.Equal(t, l.Options.Session.Snapshot().Output(id), before)
	})
	for _, stored := range []bool{false, true} {
		t.Run(fmt.Sprintf("missing chart/stored=%t", stored), func(t *testing.T) {
			t.Parallel()
			root := t.TempDir()
			testutil.WriteFile(
				t,
				root,
				"release.yaml",
				"apiVersion: helm.toolkit.fluxcd.io/v2\nkind: HelmRelease\nmetadata: {name: release, namespace: apps}\nspec: {chartRef: {kind: OCIRepository, name: missing}}\n",
			)
			st := store.New()
			l := loader.New(st)
			l.SourceRoot = root
			requester := &manifest.HelmRelease{
				Name: "release", Namespace: "apps",
				ChartRef: &helmv2.CrossNamespaceSourceReference{Kind: manifest.KindOCIRepository, Name: "missing"},
			}
			d := Session{cfg: Config{Path: root, RepoRoot: root, Store: st}, loader: l}
			_, err := d.resolveTestSources(t.Context(), false)
			assert.NoError(t, err)
			_, err = l.Load(t.Context(), root, manifest.NamedResource{}, nil)
			assert.NoError(t, err)
			assert.NoError(t, l.Admit(t.Context(), root))
			assert.Equal(t, st.GetObject(requester.Named()) == nil, true)
			if stored {
				st.AddObject(requester)
			}
			groups := map[manifest.NamedResource]*entrySourceGroup{}
			d.addMissingSources(groups)
			chartSource := requester.ChartSource()
			group, found := groups[chartSource]
			assert.Equal(t, found, true)
			if stored {
				assert.Diff(t, group.refs, []manifest.NamedResource{requester.Named()})
			}
			count, err := d.resolveTestSources(t.Context(), true)
			assert.NoError(t, err)
			assert.Equal(t, count, 0)
			assert.Equal(t, st.GetObject(chartSource) == nil, true)
			assert.Equal(t, st.GetArtifact(chartSource, store.ArtifactContent) == nil, true)
		})
	}

	t.Run("git changed only", func(t *testing.T) {
		t.Parallel()
		kind, independent := manifest.KindOCIRepository, false
		testResolveEntrySourcesIndexedDeclarationsCase(t, testResolveEntrySourcesIndexedDeclarationsCaseInputs{
			kind: &kind, independent: &independent, changed: true,
		})
	})
	for _, kind := range []string{manifest.KindGitRepository, manifest.KindOCIRepository, manifest.KindHelmRepository, manifest.KindBucket, manifest.KindHelmChart, manifest.KindExternalArtifact} {
		for _, independent := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/independent=%t", kind, independent), func(t *testing.T) {
				t.Parallel()
				testResolveEntrySourcesIndexedDeclarationsCase(
					t,
					testResolveEntrySourcesIndexedDeclarationsCaseInputs{kind: &kind, independent: &independent},
				)
			},
			)
		}
	}
}

type testResolveEntrySourcesContentCaseInputs struct {
	bootstrap **store.SourceArtifact
	obj       **manifest.GitRepository
	candidate *entryResolution
	phase     *string
}

func testResolveEntrySourcesContentCase(t *testing.T, testCaseInput testResolveEntrySourcesContentCaseInputs) {
	t.Helper()

	d := Admission{aliasViews: map[string]*store.SourceArtifact{}}
	first, key, err := d.entrySourceView(t.Context(), testCaseInput.candidate)

	assert.FatalIf(t, err != nil, "%v", err)

	d.aliasViews[key] = first
	selected := (*testCaseInput.candidate)
	cloned := *(*testCaseInput.bootstrap)
	selected.input = &cloned
	switch *testCaseInput.phase {
	case "metadata":
		cloned.Revision = "next"
		cloned.Metadata = map[string]string{"org.opencontainers.image.revision": "next"}
	case "sparse":
		shaped := *(*testCaseInput.obj)
		shaped.SparseCheckout = []string{"other"}
		selected.git = &shaped
	case "mutable":
		cloned.Content = ""
		cloned.Key = ""
		cloned.FS = fstest.MapFS{"apps/file": {Data: []byte("selected")}}
	}
	got, _, err := d.entrySourceView(t.Context(), &selected)

	assert.FatalIf(t, err != nil, "%v", err)

	_, identity, err := source.OperationView(t.Context(), got.FS)
	if err != nil || got.Content != identity {
		t.Fatalf("view identity = %q, want %q: %v", got.Content, identity, err)
	}
	if (*testCaseInput.phase) == "mutable" {
		assert.FatalIf(t, got.Content != "", "%v", "mutable alias acquired a generation")
	} else if got.Content == "" || (got.Content == first.Content) != ((*testCaseInput.phase) != "sparse") {
		t.Fatalf("shaped identity = %q, first %q", got.Content, first.Content)
	}
}

type testResolveEntrySourcesIndexedDeclarationsCaseInputs struct {
	kind        *string
	independent *bool
	changed     bool
}

func testResolveEntrySourcesIndexedDeclarationsCase(
	t *testing.T,
	testCaseInput testResolveEntrySourcesIndexedDeclarationsCaseInputs,
) {
	t.Helper()

	root := t.TempDir()
	repo := testutil.GitInit(t, root)
	id := manifest.NamedResource{
		Group:     manifest.SourceDomain,
		Kind:      (*testCaseInput.kind),
		Namespace: "sources",
		Name:      "declared",
	}
	spec := "{url: 'oci://fixture.invalid/artifact', interval: 5m}"
	if (*testCaseInput.kind) == manifest.KindGitRepository {
		spec = "{url: 'https://fixture.invalid/actual', interval: 5m}"
	}
	if (*testCaseInput.kind) == manifest.KindExternalArtifact {
		spec = "{}"
	}
	if (*testCaseInput.kind) == manifest.KindHelmRepository {
		spec = "{url: 'https://fixture.invalid/charts', interval: 5m}"
	}
	if (*testCaseInput.kind) == manifest.KindHelmChart {
		spec = "{chart: app, sourceRef: {kind: HelmRepository, name: repository}, interval: 5m}"
	}
	if (*testCaseInput.kind) == manifest.KindBucket {
		spec = "{endpoint: fixture.invalid, bucketName: published, interval: 5m}"
	}
	testutil.WriteFile(
		t,
		root,
		"entry/source.yaml",
		fmt.Sprintf(
			"apiVersion: source.toolkit.fluxcd.io/v1\nkind: %s\nmetadata: {name: declared, namespace: sources, uid: authored-source}\nspec: %s\n",
			(*testCaseInput.kind),
			spec,
		),
	)
	testutil.GitCommitAll(t, repo, "indexed declaration", &git.CommitOptions{
		Author: &object.Signature{Name: "t", Email: "t@e"},
	})
	testutil.WriteFile(t, root, ".gitignore", "*.yaml\n")
	testutil.WriteFile(t, root, "entry/ignored.yaml", testutil.KustomizationYAML("ignored", "apps",
		"  path: ./payload\n  sourceRef: {kind: GitRepository, name: ignored-source}\n"))
	st := store.New()
	l := loader.New(st)
	l.SourceRoot = root
	var requester manifest.BaseManifest = &manifest.Kustomization{Name: "requester", Namespace: "apps", Path: "./payload", SourceRef: kustomizev1.CrossNamespaceSourceReference{Kind: (*testCaseInput.kind), Name: id.Name, Namespace: id.Namespace}}
	if (*testCaseInput.kind) == manifest.KindHelmRepository {
		var err error
		parseDocResult, parseDocResultErr := manifest.ParseDoc(
			map[string]any{
				"apiVersion": "helm.toolkit.fluxcd.io/v2",
				"kind":       manifest.KindHelmRelease,
				"metadata":   map[string]any{"name": "requester", "namespace": "apps"},
				"spec": map[string]any{
					"chart": map[string]any{
						"spec": map[string]any{
							"chart": "chart",
							"sourceRef": map[string]any{
								"kind":      (*testCaseInput.kind),
								"name":      id.Name,
								"namespace": id.Namespace,
							},
						},
					},
				},
			},
			manifest.ParseDocOptions{},
		)
		requester, err = parseDocResult.Object, parseDocResultErr

		assert.FatalIf(t, err != nil, "%v", err)
	}
	if (*testCaseInput.kind) == manifest.KindHelmChart {
		requester = &manifest.HelmRelease{
			Name: "requester", Namespace: "apps",
			ChartRef: &helmv2.CrossNamespaceSourceReference{Kind: id.Kind, Name: id.Name, Namespace: id.Namespace},
		}
	}
	cfg := Config{
		Path:     filepath.Join(root, "entry"),
		RepoRoot: root,
		Store:    st,
		Selection: selection.Request{
			Roots:   []manifest.NamedResource{requester.Named()},
			Changed: testCaseInput.changed,
		},
	}
	if *testCaseInput.independent {
		cfg.Selection.Roots = []manifest.NamedResource{id}
	}
	configureSelection(t, l, cfg.Selection)
	if testCaseInput.changed {
		cfg.Changes = change.NewSet([]string{"payload/input.yaml"})
		l.Options.Changes = cfg.Changes
	}
	d := Session{cfg: cfg, loader: l}

	{
		_, err := d.resolveTestSources(t.Context(), false)
		assert.FatalIf(t, err != nil, "%v", err)
	}

	{
		_, err := l.Load(t.Context(), cfg.Path, manifest.NamedResource{}, nil)
		assert.FatalIf(t, err != nil, "%v", err)
	}

	assert.NoError(t, l.Admit(t.Context(), root))
	ignored := testutil.KustomizationID("apps", "ignored")
	_, ignoredIndexed := l.Index.Lookup(ignored)
	assert.Equal(t, ignoredIndexed, false)
	record, indexed := l.Index.Lookup(id)

	assert.FatalIf(
		t,
		!indexed || st.GetObject(id) != nil,
		"%v",
		"source was not indexed without publication",
	)

	body, err := os.ReadFile(record.Path)

	assert.FatalIf(t, err != nil, "%v", err)

	documents, err := manifest.DecodeDocs(strings.NewReader(string(body)), manifest.IncidentalDocuments)

	assert.FatalIf(t, err != nil, "%v", err)

	parseDocResult2, err := manifest.ParseDoc(documents[0].Document, manifest.ParseDocOptions{})
	expected := parseDocResult2.Object

	assert.FatalIf(t, err != nil, "%v", err)

	before := manifest.Fingerprint(expected)
	if !(*testCaseInput.independent) {
		st.AddObject(requester)
	}

	{
		count, err := d.resolveTestSources(t.Context(), true)
		assert.FatalIf(t, err != nil || count != 0, "declared source resolution = %d, %v", count, err)
	}

	obj, doc, emission := st.GetEmission(id)
	emission.Finish()

	assert.FatalIf(
		t,
		obj == nil || manifest.Fingerprint(obj) != before || doc == nil,
		"authored declaration missing: object=%v document=%v",
		obj,
		doc,
	)

	assert.FatalIf(
		t,
		doc["metadata"].(map[string]any)["uid"] != "authored-source",
		"authored document changed: %v",
		doc,
	)

	assert.FatalIf(
		t,
		st.GetArtifact(id, store.ArtifactContent) != nil,
		"%v",
		"explicit non-Git declaration acquired a local artifact",
	)
	assert.Equal(t, st.GetObject(ignored) == nil, true)
	ignoredSource := manifest.NamedResource{
		Group: manifest.SourceDomain, Kind: manifest.KindGitRepository,
		Namespace: "apps", Name: "ignored-source",
	}
	assert.Equal(t, st.GetObject(ignoredSource) == nil, true)
	assert.Equal(t, st.GetArtifact(ignoredSource, store.ArtifactContent) == nil, true)
}

func TestRun_ProducedBootstrapOCI(t *testing.T) {
	t.Parallel()
	for _, route := range []string{"indexed application", "emitted source"} {
		t.Run(route, func(t *testing.T) {
			t.Parallel()
			root := t.TempDir()
			st := store.New()
			sourceID := manifest.NamedResource{
				Group:     manifest.SourceDomain,
				Kind:      manifest.KindOCIRepository,
				Namespace: "flux-system",
				Name:      "flux-system",
			}
			testutil.WriteFile(
				t,
				root,
				"entry/consumer.yaml",
				testutil.KustomizationYAML(
					"consumer",
					"flux-system",
					"  path: ./payload\n  sourceRef: {kind: OCIRepository, name: flux-system}\n",
				),
			)
			testutil.WriteFile(t, root, "payload/kustomization.yaml", "resources: []\n")
			if route == "indexed application" {
				testutil.WriteFile(
					t,
					root,
					"entry/operator.yaml",
					"apiVersion: fluxcd.controlplane.io/v1\nkind: FluxInstance\nmetadata: {name: flux, namespace: flux-system}\nspec:\n  sync: {kind: OCIRepository, name: flux-system, url: 'oci://fixture.invalid/cluster', ref: latest, path: ./payload}\n",
				)
			} else {
				owner := &manifest.Kustomization{Name: "producer", Namespace: "flux-system"}
				st.AddObject(owner)
				declared := &manifest.OCIRepository{
					Name:      sourceID.Name,
					Namespace: sourceID.Namespace,
					URL:       "oci://fixture.invalid/cluster",
				}
				st.RecordEmitted(
					owner.Named(),
					&store.Emission{Children: []store.Child{{Obj: declared}}, Complete: true},
					nil,
				)
			}
			result, err := Run(
				t.Context(),
				Config{Path: filepath.Join(root, "entry"), RepoRoot: root, Store: st, Trees: testTrees(t)},
			)
			assert.NoError(t, err)
			t.Cleanup(result.Close)
			assert.Equal(t, st.GetArtifact(sourceID, store.ArtifactContent) == nil, true)
			sourceObject := st.GetDeclaration(sourceID)
			if route == "indexed application" {
				assert.Equal(t, sourceObject == nil, true)
				sourceObject = result.Admission.resolvedSources.snapshot()[sourceID].object
			}
			declaration, ok := sourceObject.(*manifest.OCIRepository)
			assert.Equal(t, ok, true)
			if ok {
				assert.Equal(t, declaration.URL, "oci://fixture.invalid/cluster")
			}
			for _, warning := range result.Admission.Warnings() {
				assert.Equal(t, warning.Category == manifest.WarnBootstrapAliases, false)
			}
		})
	}
}
