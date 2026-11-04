package loader

import (
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"testing"

	kustomizev1 "github.com/fluxcd/kustomize-controller/api/v1"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

func TestFinalizeIndex_ArtifactSourceViews(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	s := store.New()
	l := loaderFixture(s, nil)
	l.SourceRoot = root
	admitForTest(s, root)
	for _, name := range []string{"first", "second"} {
		view := filepath.Join(root, "views", name)
		testutil.WriteFile(
			t,
			view,
			"apps/kustomization.yaml",
			"namespace: apps\nresources: [generator.yaml, consumer.yaml]\n",
		)
		testutil.WriteFile(
			t,
			view,
			"apps/generator.yaml",
			"apiVersion: source.extensions.fluxcd.io/v1beta1\nkind: ArtifactGenerator\nmetadata: {name: generator, uid: authored}\nspec:\n  sources: [{alias: foreign, kind: OCIRepository, name: foreign, namespace: external}, {alias: repo, kind: GitRepository, name: input}, {alias: bucket, kind: Bucket, name: bucket, namespace: storage}]\n  pathPattern: '@repo/{app}'\n  artifacts: [{name: 'app-{app}'}, {name: common}]\n",
		)
		testutil.WriteFile(
			t,
			view,
			"apps/consumer.yaml",
			testutil.KustomizationYAML(
				"consumer",
				"",
				"  path: ./leaf\n  sourceRef: {kind: ExternalArtifact, name: app-"+name+"}\n",
			),
		)
		source := &manifest.GitRepository{Name: name, Namespace: "flux-system"}
		s.AddObject(source)
		s.SetArtifact(
			source.Named(),
			&store.SourceArtifact{
				LocalRoot: root,
				LocalPath: view,
				Key:       "pin-" + name,
				Content:   "pin-" + name,
				FS:        os.DirFS(view),
			},
		)
		s.AddObject(
			&manifest.Kustomization{
				Name:      name,
				Namespace: "flux-system",
				Path:      "./apps",
				SourceRef: kustomizev1.CrossNamespaceSourceReference{Kind: manifest.KindGitRepository, Name: name},
			},
		)
		{
			_, err := l.Load(t.Context(), view)
			assert.NoError(t, err)
		}
		authored := manifest.NamedResource{
			Group: "source.extensions.fluxcd.io",
			Kind:  "ArtifactGenerator",
			Name:  "generator",
		}
		record, ok := l.Index.Lookup(authored)
		assert.FatalIf(t, !ok, "%s", "authored generator missing before index finalization")
		assert.Diff(t, record.Refs, []manifest.NamedResource{
			{Group: manifest.SourceDomain, Kind: manifest.KindOCIRepository, Namespace: "external", Name: "foreign"},
			{Group: manifest.SourceDomain, Kind: manifest.KindGitRepository, Name: "input"},
			{Group: manifest.SourceDomain, Kind: manifest.KindBucket, Namespace: "storage", Name: "bucket"},
		})
	}
	for _, ks := range s.ListAs[*manifest.Kustomization](manifest.FluxKustomizeDomain, manifest.KindKustomization) {
		state := l.sourceState(ks.SourceID(), root)
		scope := l.scopeSource(new(state))
		l.scopes.sources[scope].ready = true
	}
	l.Admit(root)
	projected := manifest.NamedResource{
		Group:     "source.extensions.fluxcd.io",
		Kind:      "ArtifactGenerator",
		Namespace: "apps",
		Name:      "generator",
	}
	record, ok := l.Index.Lookup(projected)
	assert.FatalIf(t, !ok, "%s", "projected generator missing before index finalization")
	assert.Diff(t, record.Refs, []manifest.NamedResource{
		{Group: manifest.SourceDomain, Kind: manifest.KindOCIRepository, Namespace: "external", Name: "foreign"},
		{Group: manifest.SourceDomain, Kind: manifest.KindGitRepository, Namespace: "apps", Name: "input"},
		{Group: manifest.SourceDomain, Kind: manifest.KindBucket, Namespace: "storage", Name: "bucket"},
	})
	obj, doc, attempt := s.GetEmission(projected)
	attempt.Finish()
	assert.FatalIf(
		t,
		obj == nil || doc == nil || doc["metadata"].(map[string]any)["uid"] != "authored",
		"projected authoritative document = %#v",
		doc,
	)
	{
		_, authoredNamespace := doc["metadata"].(map[string]any)["namespace"]
		assert.FatalIf(t, authoredNamespace, "%s", "projection changed authored document namespace")
	}
	{
		err := l.FinalizeIndex(t.Context(), root, true)
		assert.NoError(t, err)
	}
	idx := l.Index
	generator := manifest.NamedResource{
		Group:     "source.extensions.fluxcd.io",
		Kind:      "ArtifactGenerator",
		Namespace: "apps",
		Name:      "generator",
	}
	for _, name := range []string{"first", "second"} {
		target := manifest.NamedResource{
			Group:     manifest.SourceDomain,
			Kind:      manifest.KindExternalArtifact,
			Namespace: "apps",
			Name:      "app-" + name,
		}
		r, _ := idx.Lookup(target)
		var scoped []manifest.Production
		for _, p := range r.Productions {
			if p.Owner != (manifest.NamedResource{}) {
				scoped = append(scoped, p)
			}
		}
		want := manifest.Production{
			Owner:       testutil.KustomizationID("flux-system", name),
			Declaration: generator,
			Authored:    manifest.NamedResource{Group: generator.Group, Kind: generator.Kind, Name: generator.Name},
			Source: manifest.NamedResource{
				Group:     manifest.SourceDomain,
				Kind:      manifest.KindGitRepository,
				Namespace: "flux-system",
				Name:      name,
			},
			File: "views/" + name + "/apps/generator.yaml",
		}
		assert.Diff(t, scoped, []manifest.Production{want})
	}
	common, _ := idx.Lookup(
		manifest.NamedResource{
			Group:     manifest.SourceDomain,
			Kind:      manifest.KindExternalArtifact,
			Namespace: "apps",
			Name:      "common",
		},
	)
	var files []string
	for _, p := range common.Productions {
		if p.Owner != (manifest.NamedResource{}) {
			files = append(files, p.File)
		}
	}
	slices.Sort(files)
	assert.Diff(t, files, []string{"views/first/apps/generator.yaml", "views/second/apps/generator.yaml"})
}

func TestFinalizeIndex_ArtifactScopeIsolation(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name, template string
		split, deleted bool
		ignored, want  bool
	}{
		{name: "before", template: "old-{app}"},
		{name: "after", template: "app-{app}", want: true},
		{name: "disjoint_coverage", template: "app-{app}", split: true},
		{name: "deleted_declaration", template: "app-{app}", deleted: true},
		{name: "explicit_ignored_declaration", template: "app-{app}", ignored: true, want: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			testFinalizeIndexArtifactScopeIsolationCase(t, &tc)
		},
		)
	}
}

func TestSelfProduceBuilder_ArtifactDocumentVisit(t *testing.T) {
	t.Parallel()
	for _, mode := range []string{"explicit", "deferred", "unadmitted", "projected", "self_owner", "transformed"} {
		t.Run(mode, func(t *testing.T) {
			t.Parallel()
			testSelfProduceBuilderArtifactDocumentVisitCase(
				t,
				&mode,
			)
		},
		)
	}
}

func TestFinalizeIndex_UntraversedBuilder(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	testutil.WriteFile(
		t,
		root,
		"evidence/kustomization.yaml",
		"configMapGenerator:\n- name: untraversed\n  literals: [value=hidden]\n",
	)
	testutil.WriteFile(
		t,
		root,
		"selected/kustomization.yaml",
		"configMapGenerator:\n- name: selected\n  literals: [value=visible]\n",
	)
	l := New(store.New())
	l.SourceRoot = root
	admitForTest(l.Store, root)
	l.mountSources()
	l.documents.read(filepath.Join(root, "evidence"))
	_, err := l.Load(t.Context(), filepath.Join(root, "selected"))
	assert.NoError(t, err)
	{
		err := l.FinalizeIndex(t.Context(), root, true)
		assert.NoError(t, err)
	}
	for _, name := range []string{"untraversed", "selected"} {
		id := manifest.NamedResource{Kind: manifest.KindConfigMap, Name: name}
		_, indexed := l.Index.Lookup(id)
		assert.FatalIf(t, indexed != (name == "selected"), "generator %s indexed = %v", name, indexed)
	}
}

func testFinalizeIndexArtifactScopeIsolationCase(
	t *testing.T,
	testCaseInput *struct {
		name     string
		template string
		split    bool
		deleted  bool
		ignored  bool
		want     bool
	},
) {
	t.Helper()

	root := t.TempDir()
	consumerDir := "producer"
	resources := "generator.yaml, consumer.yaml"
	if testCaseInput.split {
		consumerDir, resources = "consumer", "generator.yaml"
	}
	testutil.WriteFile(t, root, "producer/kustomization.yaml", "namespace: apps\nresources: ["+resources+"]\n")
	if !testCaseInput.deleted {
		testutil.WriteFile(
			t,
			root,
			"producer/generator.yaml",
			"apiVersion: source.extensions.fluxcd.io/v1beta1\nkind: ArtifactGenerator\nmetadata: {name: generator}\nspec:\n  sources: [{alias: repo, kind: GitRepository, name: input}]\n  pathPattern: '@repo/{app}'\n  artifacts: [{name: '"+testCaseInput.template+"'}]\n",
		)
	}
	if testCaseInput.ignored {
		testutil.WriteFile(t, root, "producer/.krmignore", "generator.yaml\n")
	}
	if testCaseInput.split {
		testutil.WriteFile(
			t,
			root,
			"consumer/kustomization.yaml",
			"namespace: apps\nresources: [consumer.yaml]\n",
		)
	}
	testutil.WriteFile(
		t,
		root,
		consumerDir+"/consumer.yaml",
		testutil.KustomizationYAML(
			"consumer",
			"",
			"  path: ./leaf\n  sourceRef: {kind: ExternalArtifact, name: app-api}\n",
		),
	)
	s := store.New()
	owner := &manifest.Kustomization{Name: "producer", Namespace: "flux-system", Path: "./producer"}
	s.AddObject(owner)
	if testCaseInput.split {
		s.AddObject(&manifest.Kustomization{Name: "other", Namespace: "flux-system", Path: "./consumer"})
	}
	l := loaderFixture(s, nil)
	l.SourceRoot = root
	admitForTest(s, root)
	{
		_, err := l.Load(t.Context(), root)
		assert.NoError(t, err)
	}
	{
		err := l.FinalizeIndex(t.Context(), root, true)
		assert.NoError(t, err)
	}
	target := manifest.NamedResource{
		Group:     manifest.SourceDomain,
		Kind:      manifest.KindExternalArtifact,
		Namespace: "apps",
		Name:      "app-api",
	}
	got := slices.ContainsFunc(
		s.Productions(target, l.Index),
		func(p manifest.Production) bool { return p.Owner == owner.Named() },
	)
	if got != testCaseInput.want {
		t.Fatalf("prediction = %t, want %t", got, testCaseInput.want)
	}
}

func testSelfProduceBuilderArtifactDocumentVisitCase(
	t *testing.T,
	testCaseInput *string,
) {
	t.Helper()

	root := t.TempDir()
	generatorNS, artifactName, namespace := "apps", "app-{app}", ""
	if (*testCaseInput) == "projected" || (*testCaseInput) == "transformed" {
		generatorNS, namespace = "", "namespace: apps\n"
	}
	if (*testCaseInput) == "transformed" {
		namespace += "namePrefix: pre-\nnameSuffix: -post\n"
	}
	if (*testCaseInput) == "self_owner" {
		generatorNS, artifactName = "authored", "app-api"
	}
	testutil.WriteFile(
		t,
		root,
		"kustomization.yaml",
		namespace+"apiVersion: kustomize.toolkit.fluxcd.io/v1\nkind: Kustomization\nmetadata: {name: document, namespace: apps}\nspec:\n  sourceRef: {kind: ExternalArtifact, name: app-document}\nresources: [generator.yaml, consumer.yaml, hidden.yaml]\n",
	)
	testutil.WriteFile(
		t,
		root,
		"generator.yaml",
		fmt.Sprintf(
			"apiVersion: source.extensions.fluxcd.io/v1beta1\nkind: ArtifactGenerator\nmetadata: {name: generator, namespace: %q}\nspec:\n  pathPattern: '@repo/{app}'\n  artifacts: [{name: %q}]\n",
			generatorNS,
			artifactName,
		),
	)
	testutil.WriteFile(
		t,
		root,
		"consumer.yaml",
		testutil.KustomizationYAML(
			"consumer",
			"apps",
			"  sourceRef: {kind: ExternalArtifact, name: app-api}\n",
		),
	)
	testutil.WriteFile(
		t,
		root,
		"hidden.yaml",
		testutil.KustomizationYAML(
			"hidden",
			"apps",
			"  sourceRef: {kind: ExternalArtifact, name: app-hidden}\n",
		),
	)
	reader := readerForTest(root)
	st := store.New()
	seedArtifactDocuments(t, artifactDocumentInput{mode: testCaseInput, root: root, store: st}, &reader)
	b := selfProduceBuilder{
		idx:          &manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{}},
		store:        st,
		reader:       reader,
		emittedFiles: map[string][]manifest.NamedResource{},
	}
	generator := manifest.NamedResource{
		Group:     "source.extensions.fluxcd.io",
		Kind:      "ArtifactGenerator",
		Namespace: "apps",
		Name:      "generator",
	}
	authored := generator
	authored.Namespace = generatorNS
	if (*testCaseInput) == "transformed" {
		generator.Name = "pre-generator-post"
	}
	if (*testCaseInput) == "projected" || (*testCaseInput) == "self_owner" {
		b.idx.Records[generator] = manifest.DiscoveryRecord{
			Authored: authored,
			Path:     filepath.Join(root, "generator.yaml"),
			File:     "generator.yaml",
		}
	}
	l := New(st)
	w := traversal{documents: reader, visited: map[graphContext]struct{}{}}
	var scope []graphVisit
	var declarations []producerDeclaration
	assert.FatalIf(t, !w.walk(
		t.Context(),
		graphVisit{root: root, dir: ".", source: manifest.BootstrapSourceID},
		func(v graphVisit) bool {
			v.deferMaterialization = (*testCaseInput) != "explicit" && (*testCaseInput) != "transformed"
			if (*testCaseInput) == "self_owner" {
				v.owner = generator
			}
			scope = append(scope, v)
			if v.document == nil && v.readable {
				var err error
				declarations, err = b.recordProduced(t.Context(), new(v), declarations, l)
				assert.NoError(t, err)
			}
			return true
		},
	), "%s", "walk stopped before completing")
	b.joinProductions(declarations, scope)
	want := map[string]int{"app-api": 1, "app-document": 0, "app-hidden": 0}
	if (*testCaseInput) == "explicit" || (*testCaseInput) == "transformed" {
		want["app-hidden"] = 1
	}
	if (*testCaseInput) == "unadmitted" {
		want["app-api"] = 0
	}
	for name, count := range want {
		id := manifest.NamedResource{
			Group:     manifest.SourceDomain,
			Kind:      manifest.KindExternalArtifact,
			Namespace: "apps",
			Name:      name,
		}
		record, _ := b.idx.Lookup(id)
		assert.Equal(t, len(record.Productions), count)
		if count != 0 {
			assert.Diff(
				t,
				record.Productions,
				[]manifest.Production{
					{
						Owner:       generator,
						Declaration: generator,
						Authored:    authored,
						Source:      manifest.BootstrapSourceID,
						File:        "generator.yaml",
					},
				},
			)
		}
	}
}

type artifactDocumentInput struct {
	mode  *string
	root  string
	store *store.Store
}

func seedArtifactDocuments(t *testing.T, fixture artifactDocumentInput, reader *documentReader) {
	t.Helper()
	if (*fixture.mode) != "explicit" {
		for _, name := range []string{"generator", "consumer"} {
			if (*fixture.mode) == "unadmitted" && name == "generator" {
				continue
			}
			children, err := reader.parseFile(
				t.Context(),
				filepath.Join(fixture.root, name+".yaml"),
				manifest.ParseDocOptions{},
			)
			assert.NoError(t, err)
			obj := children[0].Obj
			if (*fixture.mode) == "projected" || (*fixture.mode) == "self_owner" {
				obj = obj.WithNamespace("apps")
			}
			fixture.store.AddObject(obj, children[0].Doc)
		}
	}
}
