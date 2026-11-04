package change_test

import (
	"io/fs"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"testing"

	kustomizev1 "github.com/fluxcd/kustomize-controller/api/v1"
	"github.com/google/go-cmp/cmp"

	"github.com/home-operations/flate/internal/admissiontest"
	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/change"
	"github.com/home-operations/flate/pkg/loader"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

func TestFinalizeIndex_ArtifactGenerators(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name, resources, scan string
		component, alias      bool
	}{
		{"generator_first", "generator.yaml, consumer.yaml", ".", false, false},
		{"consumer_first", "consumer.yaml, generator.yaml", ".", false, false},
		{"component_full", "consumer.yaml", ".", true, false},
		{"component_changed_only", "consumer.yaml", "apps", true, false},
		{"component_input_alias", "consumer.yaml", "apps", true, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			testFinalizeIndexArtifactGeneratorsCase(t, &tc)
		},
		)
	}
}

func admitForFilter(s *store.Store, root string) {
	s.SetArtifact(
		manifest.BootstrapSourceID,
		&store.SourceArtifact{Kind: manifest.KindGitRepository, LocalPath: root, LocalRoot: root, FS: os.DirFS(root)},
	)
}

func ownersForFilter(index *manifest.DiscoveryIndex, file string) []manifest.NamedResource {
	ownersOfFileResult := index.OwnersOfFile(file)
	owners := ownersOfFileResult.Owners
	return owners
}

func rendererForFilter(t *testing.T, l *loader.Loader, root string) *manifest.DiscoveryIndex {
	t.Helper()
	admitForFilter(l.Store, root)
	assert.NoError(t, l.FinalizeIndex(t.Context(), root, true))
	return l.Index
}

type readerForFilter struct {
	fs.FS

	missing string
	opens   int
}

func (f *readerForFilter) Open(name string) (fs.File, error) {
	if name == f.missing {
		f.opens++
	}
	return f.FS.Open(name)
}

func (f *readerForFilter) Stat(name string) (fs.FileInfo, error) {
	return fs.Stat(f.FS, name)
}

func snapshotIndexForFilter(index *manifest.DiscoveryIndex) manifest.DiscoveryIndex {
	snapshot := *index
	snapshot.Records = maps.Clone(index.Records)
	for id, r := range snapshot.Records { //nolint:gocritic // Map iteration preserves the value snapshot without an extra lookup.
		r.Refs = slices.Clone(r.Refs)
		r.Productions = slices.Clone(r.Productions)
		snapshot.Records[id] = r
	}
	snapshot.Claims = maps.Clone(index.Claims)
	for file, ids := range snapshot.Claims {
		snapshot.Claims[file] = slices.Clone(ids)
	}
	snapshot.Readers = maps.Clone(index.Readers)
	for file, ids := range snapshot.Readers {
		snapshot.Readers[file] = slices.Clone(ids)
	}

	return snapshot
}

func assertFilterIndexForTest(t *testing.T, got, want manifest.DiscoveryIndex) {
	t.Helper()

	{
		diff := cmp.Diff(want, got, cmp.Comparer(manifest.Equal))
		assert.ErrorIf(t, diff != "", "discovery index differs (-want +got):\n%s", diff)
	}
}

func TestFinalizeIndex_StaticArtifactGeneratorConsumer(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	testutil.WriteFile(
		t,
		root,
		"entry/generator.yaml",
		"apiVersion: source.extensions.fluxcd.io/v1beta1\nkind: ArtifactGenerator\nmetadata: {name: generator, namespace: apps, uid: authored}\nspec:\n  sources: [{alias: foreign, kind: OCIRepository, name: foreign, namespace: external}, {alias: repo, kind: GitRepository, name: input}]\n  pathPattern: '@repo/{app}'\n  artifacts: [{name: 'app-{app}'}]\n",
	)
	testutil.WriteFile(
		t,
		root,
		"entry/consumer.yaml",
		testutil.KustomizationYAML(
			"consumer",
			"apps",
			"  path: ./leaf\n  sourceRef: {kind: ExternalArtifact, name: app-api}\n",
		),
	)
	testutil.WriteFile(
		t,
		root,
		"entry/other.yaml",
		testutil.KustomizationYAML(
			"other",
			"other",
			"  path: ./leaf\n  sourceRef: {kind: ExternalArtifact, name: app-other}\n",
		),
	)
	testutil.WriteFile(
		t,
		root,
		"entry/foreign-owner.yaml",
		testutil.KustomizationYAML(
			"foreign-owner",
			"flux-system",
			"  path: ./apps\n  sourceRef: {kind: GitRepository, name: foreign}\n",
		),
	)
	view := filepath.Join(root, "views", "foreign")
	testutil.WriteFile(t, view, "apps/kustomization.yaml", "resources: [consumer.yaml]\n")
	testutil.WriteFile(
		t,
		view,
		"apps/consumer.yaml",
		testutil.KustomizationYAML(
			"foreign",
			"apps",
			"  path: ./leaf\n  sourceRef: {kind: ExternalArtifact, name: app-foreign}\n",
		),
	)
	s := store.New()
	admitForFilter(s, root)
	source := &manifest.GitRepository{Name: "foreign", Namespace: "flux-system"}
	s.AddObject(source)
	s.SetArtifact(
		source.Named(),
		&store.SourceArtifact{
			LocalRoot: root,
			LocalPath: view,
			Key:       "foreign-pin",
			Content:   "foreign-pin",
			FS:        os.DirFS(view),
		},
	)
	l := loader.New(s)
	l.SourceRoot = root
	for _, scan := range []string{filepath.Join(root, "entry"), view} {
		{
			_, err := l.Load(t.Context(), scan, nil)
			assert.FatalIf(t, err != nil, "%v", err)
		}
	}

	{
		err := l.FinalizeIndex(t.Context(), root, true)
		assert.FatalIf(t, err != nil, "%v", err)
	}

	generator := manifest.NamedResource{
		Group:     "source.extensions.fluxcd.io",
		Kind:      "ArtifactGenerator",
		Namespace: "apps",
		Name:      "generator",
	}
	output := manifest.NamedResource{
		Group:     manifest.SourceDomain,
		Kind:      manifest.KindExternalArtifact,
		Namespace: "apps",
		Name:      "app-api",
	}
	record, _ := l.Index.Lookup(generator)
	assert.Diff(t, record.Refs, []manifest.NamedResource{
		{Group: manifest.SourceDomain, Kind: manifest.KindOCIRepository, Namespace: "external", Name: "foreign"},
		{Group: manifest.SourceDomain, Kind: manifest.KindGitRepository, Namespace: "apps", Name: "input"},
	})
	assert.Diff(
		t,
		s.Productions(output, l.Index),
		[]manifest.Production{
			{
				Owner:       generator,
				Declaration: generator,
				Authored:    generator,
				Source:      manifest.BootstrapSourceID,
				File:        "entry/generator.yaml",
			},
		},
	)
	for _, unrelated := range []manifest.NamedResource{
		{Group: manifest.SourceDomain, Kind: manifest.KindExternalArtifact, Namespace: "other", Name: "app-other"},
		{Group: manifest.SourceDomain, Kind: manifest.KindExternalArtifact, Namespace: "apps", Name: "app-foreign"},
	} {
		assert.FatalIf(t, slices.ContainsFunc(
			s.Productions(unrelated, l.Index),
			func(p manifest.Production) bool { return p.Declaration == generator },
		), "static declaration escaped its namespace or source view: %s", unrelated)
	}
	filter := admissiontest.Filter(
		t,
		admissiontest.Config{
			Changes:   change.NewSet([]string{"entry/consumer.yaml"}),
			Index:     l.Index,
			Inventory: s,
			Root:      "",
		},
	)
	assert.Equal(t, filter.ShouldReconcile(generator), true)
	assert.Equal(t, filter.ShouldReconcile(testutil.KustomizationID("apps", "consumer")), true)
	assert.Equal(t, filter.ShouldReconcile(testutil.KustomizationID("other", "other")), false)
	assert.Equal(t, filter.ShouldReconcile(testutil.KustomizationID("apps", "foreign")), false)
}

func testFinalizeIndexArtifactGeneratorsCase(
	t *testing.T,
	testCaseInput *struct {
		name string

		resources string
		scan      string
		component bool
		alias     bool
	},
) {
	t.Helper()

	root := t.TempDir()
	testutil.WriteFile(t, root, "apps/kustomization.yaml", "resources: ["+testCaseInput.resources+"]\n")
	generatorFile := "apps/generator.yaml"
	if testCaseInput.component {
		generatorFile = "shared/generator.yaml"
		testutil.WriteFile(
			t,
			root,
			"shared/kustomization.yaml",
			"apiVersion: kustomize.config.k8s.io/v1alpha1\nkind: Component\nresources: [generator.yaml]\n",
		)
	}
	testutil.WriteFile(
		t,
		root,
		generatorFile,
		"apiVersion: source.extensions.fluxcd.io/v1beta1\nkind: ArtifactGenerator\nmetadata: {name: generator, namespace: authored, uid: authored}\nspec:\n  sources: [{alias: foreign, kind: OCIRepository, name: foreign, namespace: external}, {alias: repo, kind: GitRepository, name: input}, {alias: bucket, kind: Bucket, name: bucket, namespace: storage}]\n  pathPattern: '@repo/apps/{app}'\n  artifacts: [{name: 'app-{app}'}]\n",
	)
	testutil.WriteFile(
		t,
		root,
		"apps/consumer.yaml",
		testutil.KustomizationYAML(
			"consumer",
			"authored",
			"  path: ./leaf\n  sourceRef: {kind: ExternalArtifact, name: app-api}\n",
		),
	)
	s := store.New()
	source := manifest.BootstrapSourceID
	if testCaseInput.alias {
		repository := &manifest.GitRepository{Name: "alias", Namespace: "flux-system"}
		source = repository.Named()
		s.AddObject(repository)
		s.SetArtifact(
			source,
			&store.SourceArtifact{
				LocalRoot:  root,
				LocalPath:  filepath.Join(t.TempDir(), "alias"),
				InputAlias: true,
				FS:         os.DirFS(root),
			},
		)
	}
	for _, ns := range []string{"apps", "staging"} {
		owner := &manifest.Kustomization{
			Name:            ns,
			Namespace:       "flux-system",
			Path:            "./apps",
			TargetNamespace: ns,
		}
		if testCaseInput.alias {
			owner.SourceRef = kustomizev1.CrossNamespaceSourceReference{
				Kind:      source.Kind,
				Name:      source.Name,
				Namespace: source.Namespace,
			}
		}
		if testCaseInput.component {
			owner.Components = []string{"../shared"}
		}
		s.AddObject(owner)
	}
	l := loader.New(s)
	l.SourceRoot = root
	admitForFilter(s, root)

	{
		_, err := l.Load(t.Context(), filepath.Join(root, testCaseInput.scan), nil)
		assert.FatalIf(t, err != nil, "%v", err)
	}

	{
		err := l.FinalizeIndex(t.Context(), root, true)
		assert.FatalIf(t, err != nil, "%v", err)
	}

	idx := l.Index
	for _, ns := range []string{"apps", "staging"} {
		generator := manifest.NamedResource{
			Group:     "source.extensions.fluxcd.io",
			Kind:      "ArtifactGenerator",
			Namespace: ns,
			Name:      "generator",
		}
		record, ok := idx.Lookup(generator)

		assert.FatalIf(
			t,
			!ok || record.File != generatorFile,
			"generator coordinates before filtering = %+v",
			record,
		)

		assert.Diff(t, record.Refs, []manifest.NamedResource{
			{
				Group:     manifest.SourceDomain,
				Kind:      manifest.KindOCIRepository,
				Namespace: "external",
				Name:      "foreign",
			},
			{Group: manifest.SourceDomain, Kind: manifest.KindGitRepository, Namespace: ns, Name: "input"},
			{Group: manifest.SourceDomain, Kind: manifest.KindBucket, Namespace: "storage", Name: "bucket"},
		})
		target := manifest.NamedResource{
			Group:     manifest.SourceDomain,
			Kind:      manifest.KindExternalArtifact,
			Namespace: ns,
			Name:      "app-api",
		}
		r, ok := idx.Lookup(target)

		assert.FatalIf(t, !ok || r.Path != "" || s.GetObject(target) != nil ||
			s.GetArtifact(target, store.ArtifactContent) != nil, "prediction supplied content: %+v", r)

		want := manifest.Production{
			Owner:       testutil.KustomizationID("flux-system", ns),
			Declaration: generator,
			Authored: manifest.NamedResource{
				Group:     generator.Group,
				Kind:      generator.Kind,
				Namespace: "authored",
				Name:      generator.Name,
			},
			Source: source,
			File:   generatorFile,
		}
		assert.Diff(t, r.Productions, []manifest.Production{want})
	}
	filter := admissiontest.Filter(
		t,
		admissiontest.Config{
			Changes:   change.NewSet([]string{generatorFile}),
			Index:     idx,
			Inventory: s,
			Root:      "",
		},
	)
	assert.Diff(
		t,
		idx.Readers[generatorFile],
		[]manifest.FileReader{
			{Reader: testutil.KustomizationID("flux-system", "apps"), Source: source},
			{Reader: testutil.KustomizationID("flux-system", "staging"), Source: source},
		},
	)
	for _, ns := range []string{"apps", "staging"} {
		owner := testutil.KustomizationID("flux-system", ns)

		assert.FatalIf(
			t,
			!filter.ShouldReconcile(owner),
			"%v",
			"changed declaration did not select its contributing owner",
		)

		generator := manifest.NamedResource{
			Group:     "source.extensions.fluxcd.io",
			Kind:      "ArtifactGenerator",
			Namespace: ns,
			Name:      "generator",
		}
		obj := s.GetObject(generator)
		assert.Equal(t, obj != nil, !testCaseInput.component)

		assert.FatalIf(
			t,
			obj != nil && (!manifest.KindOf(obj).Discovery || !manifest.KindOf(obj).Runnable),
			"generator admission = %v",
			obj,
		)

		assert.FatalIf(
			t,
			!filter.ShouldReconcile(testutil.KustomizationID(ns, "consumer")),
			"%v",
			"changed generator declaration did not select its source consumer",
		)

		{
			r, ok := idx.Lookup(
				manifest.NamedResource{
					Group:     manifest.SourceDomain,
					Kind:      manifest.KindExternalArtifact,
					Namespace: ns,
					Name:      "generator",
				},
			)
			assert.FatalIf(t, ok &&
				len(r.Productions) != 0, "%v", "generator name became an output")
		}
	}
}
