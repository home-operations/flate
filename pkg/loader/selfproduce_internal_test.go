package loader

import (
	"io/fs"
	"maps"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"testing/fstest"

	kustomizev1 "github.com/fluxcd/kustomize-controller/api/v1"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/kustomizeinput"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

func cmID(ns string) manifest.NamedResource {
	return manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: ns, Name: "cluster-settings"}
}

func TestPackageRoots_EdgeFactLifetime(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	st := store.New()
	bootstrap := &store.SourceArtifact{LocalPath: root, FS: fstest.MapFS{
		"a/kustomization.yaml": {
			Data: []byte("resources: [../target, ../missing]\ntransformers: [../configuration]\n"),
		},
		"b/kustomization.yaml":             {Data: []byte("resources: [../target]\n")},
		"target/kustomization.yaml":        {Data: []byte("resources: []\n")},
		"configuration/kustomization.yaml": {Data: []byte("resources: []\n")},
	}}
	st.SetArtifact(manifest.BootstrapSourceID, bootstrap)
	l := New(st)
	l.SourceRoot = root
	_, err := l.Load(t.Context(), root, manifest.NamedResource{}, nil)
	assert.NoError(t, err)
	assert.NoError(t, l.Admit(t.Context(), root))
	for _, name := range []string{"target", "missing", "configuration"} {
		assert.FatalIf(
			t,
			!l.documents.cache[filepath.Join(root, name)].incoming,
			"direct edge to %s was not recorded",
			name,
		)
	}
	target := filepath.Join(root, "target")
	doc := l.documents.cache[target]
	doc.selected, doc.built = true, []store.Child{}
	l.documents.cache[target] = doc
	mounted := map[string]*store.SourceArtifact{root: bootstrap}
	for _, name := range []string{"a", "a", "b"} {
		mount := filepath.Join(root, name)
		mounted[mount] = &store.SourceArtifact{LocalPath: mount, FS: fstest.MapFS{}}
		l.documents.setSources(maps.Clone(mounted))
		assert.FatalIf(
			t,
			l.documents.cache[target].incoming,
			"%s",
			"mount change retained an incoming fact outside its subtree",
		)
		roots := l.packageRoots(root, nil)
		want := name == "a"
		assert.FatalIf(t, l.documents.cache[target].incoming != want,
			"target after masking %s: incoming=%t, want %t", name, l.documents.cache[target].incoming, want)

		assert.FatalIf(
			t,
			slices.Contains(roots, "target") == want,
			"root selection after masking %s: %v",
			name,
			roots,
		)
		assert.FatalIf(t, l.documents.cache[filepath.Join(root, "missing")].incoming ||
			l.documents.cache[filepath.Join(root, "configuration")].incoming, "%s", "masked parent's missing or configuration edge survived")
		l.packageRoots(root, nil)
	}
	{
		doc := l.documents.cache[target]
		assert.FatalIf(t, !doc.selected || doc.built == nil, "%s", "cold replay discarded selected/built state")
	}
	l.documents.setSources(map[string]*store.SourceArtifact{root: bootstrap})
	l.packageRoots(root, nil)
	assert.FatalIf(
		t,
		l.documents.cache[target].incoming,
		"%s",
		"withdrawal restored an edge from a discarded parent document",
	)
	for _, name := range []string{"a", "b"} {
		parent := filepath.Join(root, name)
		body := l.documents.read(parent).body
		doc := l.documents.cache[parent]
		doc.traversed = true
		l.documents.cache[parent] = doc
		l.documents.references(parent, body, false, func(kustomizeinput.Reference, string) bool { return false })
		assert.FatalIf(
			t,
			l.documents.cache[parent].edgesKnown,
			"%s",
			"partial traversal marked all direct edges known",
		)
		l.documents.references(parent, body, false, nil)
	}
	assert.FatalIf(
		t,
		!l.documents.cache[target].incoming,
		"%s",
		"new traversal did not restore incoming facts after withdrawal",
	)
}

//nolint:paralleltest // The test calls a helper that changes process-global state.
func TestSelfProduceBuilder_RecordProducedReuse(t *testing.T) {
	for _, prefix := range []string{"", "pre-"} {
		t.Run("prefix="+prefix, func(t *testing.T) {
			testSelfProduceBuilderRecordProducedReuseCase(
				t,
				&prefix,
			)
		},
		)
	}
}

func TestFinalizeIndex_EmptyPathRequiresSource(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name, path, source string
		produces           bool
	}{
		{name: "stub"},
		{name: "source_root", source: "flux-system", produces: true},
		{name: "explicit_root", path: "./", produces: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			root := t.TempDir()
			testutil.WriteFile(t, root, "apps/kustomization.yaml", "namespace: apps\nresources: [cm.yaml]\n")
			testutil.WriteFile(
				t,
				root,
				"apps/cm.yaml",
				"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: cluster-settings}\n",
			)
			testutil.WriteFile(
				t,
				root,
				"flux/nested/raw.yaml",
				"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: loose, namespace: apps}\n",
			)
			st := store.New()
			ks := &manifest.Kustomization{
				Name:      "root",
				Namespace: "flux-system",
				Path:      tc.path,
				SourceRef: kustomizev1.CrossNamespaceSourceReference{
					Kind:      manifest.KindGitRepository,
					Name:      tc.source,
					Namespace: "flux-system",
				},
			}
			st.AddObject(ks)
			l := loaderFixture(st, nil)

			idx := rendererFixture(t, l, root)
			var want []manifest.NamedResource
			if tc.produces {
				want = []manifest.NamedResource{ks.Named()}
			}
			{
				got := renderers(idx, cmID("apps"))
				assert.FatalIf(t, !slices.Equal(got, want), "ProducedBy = %v, want %v", got, want)
			}
			{
				got := renderers(
					idx,
					manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "apps", Name: "loose"},
				)
				assert.FatalIf(t, !slices.Equal(
					got,
					want,
				), "loose manifest producers = %v, want %v", got, want)
			}
			{
				got := ownersForTest(idx, "apps/cm.yaml")
				assert.FatalIf(t, !slices.Equal(got, want), "OwnersOfFile = %v, want %v", got, want)
			}
		})
	}
}

// The bjw-s/onedr0p topology: a root KS with a BARE spec.path (no
// kustomization.yaml) whose per-namespace subdir bases each stamp their own
// `namespace:` and pull in a shared substitutions component defining a
// namespace-LESS ConfigMap. The index must attribute that ConfigMap — in
// EACH group's resolved namespace — back to the root KS whose own render
// emits it, resolving the bare-dir → subdir-base → component → namespace
// chain that no path-prefix index can see.
func TestFinalizeIndex_BareDirComponentNamespace(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct{ name, target, local, namespace string }{
		{"bare", "", "", ""},
		{"target", "target", "", "target"},
		{"local", "", "namespace: local\n", "local"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			testFinalizeIndexBareDirComponentNamespaceCase(
				t,
				&tc,
			)
		},

		// Produced in EVERY group's namespace (the namespace transformer stamps
		// the component's namespace-less ConfigMap per base) — not just the first.

		// A namespace no base stamps is not attributed.

		)
	}
}

// A substituteFrom ConfigMap defined OUTSIDE the KS's own render subtree —
// i.e. produced by a different KS — must NOT be attributed to this KS, so its
// dependency edge survives and a real failure stays loud. Here the root KS's
// spec.path holds only its own kustomization with no component, so it produces
// nothing.
//
//nolint:paralleltest // The test calls a helper that changes process-global state.
//nolint:paralleltest // This test changes process-global state.
func TestFinalizeIndex_NonProducerNotAttributed(t *testing.T) {
	dir := t.TempDir()
	testutil.WriteFile(t, dir, "kubernetes/apps/kustomization.yaml",
		"apiVersion: kustomize.config.k8s.io/v1beta1\nkind: Kustomization\nnamespace: flux-system\nresources: []\n")

	s := store.New()
	ks := &manifest.Kustomization{
		Name:      "cluster-apps",
		Namespace: "flux-system",
		Path:      "./kubernetes/apps",
	}
	s.AddObject(ks)

	idx := rendererFixture(t, loaderFixture(s, nil), dir)
	{
		got := renderers(idx, cmID("flux-system"))
		assert.ErrorIf(t, len(got) != 0, "ProducedBy = %v, want empty (KS produces no cluster-settings)", got)
	}
	t.Run("nonproducer_allocations", func(t *testing.T) {
		b := &selfProduceBuilder{}
		{
			got := testing.AllocsPerRun(10, func() {
				b.recordProducer(new(producerDeclaration{obj: ks, identity: ks.Named()}), nil)
			})
			assert.FatalIf(t, got != 0, "nonproducer preparation allocations=%v, want 0", got)
		}
	})
	t.Run("unused_references", func(t *testing.T) {
		id := ks.Named()
		record := manifest.DiscoveryRecord{Path: filepath.Join(dir, "ks.yaml"), File: "ks.yaml"}
		index := &manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{id: record}}
		l := &Loader{Store: s, Index: index, scopes: &scopeConstruction{}}
		ctx := t.Context()
		measure := func() float64 {
			t.Helper()
			return testing.AllocsPerRun(10, func() {
				{
					err := l.buildProductions(ctx, dir, nil)
					assert.NoError(t, err)
				}
			})
		}
		withoutReferences := measure()
		record.Refs = []manifest.NamedResource{
			{Group: manifest.SourceDomain, Kind: manifest.KindGitRepository, Name: "unused"},
		}
		index.Records[id] = record
		{
			got := measure()
			assert.FatalIf(
				t,
				got != withoutReferences,
				"nonproducer reference allocations = %v, want %v",
				got,
				withoutReferences,
			)
		}
	})
}

// Nil index is safe — collectDeps falls back to always-add (the pre-index
// behavior) when no repoRoot produced an index.
func TestDiscoveryIndex_NilSafe(t *testing.T) {
	t.Parallel()
	var idx *manifest.DiscoveryIndex
	{
		got := declarationOwnersForTest(idx, manifest.NamedResource{})
		assert.ErrorIf(t, got != nil, "nil index OwnersOfProducer = %v, want nil", got)
	}
	{
		got := renderers(idx, cmID("flux-system"))
		assert.ErrorIf(t, got != nil, "nil index ProducedBy = %v, want nil", got)
	}
	{
		_, ok := emissionParentForTest(idx, "apps/base/app-a/ks.yaml")
		assert.ErrorIf(t, ok, "nil index EmissionParentByFile = ok, want absent")
	}
	{
		got := ownersForTest(idx, "apps/base/postgres/cluster.yaml")
		assert.ErrorIf(t, got != nil, "nil index OwnersOfFile = %v, want nil", got)
	}
}

// TestFinalizeIndex_OwnersOfFileDirectFileEscape: an overlay lists an
// existing resource file directly, by a path escaping its own directory. Even
// though the escape-rejecting policy keeps it from being opened, it must still
// be attributed to the owning KS — the direct-file twin of the dir escape in
// #833. Otherwise changed-only mode omits the KS when that file is the only edit.
func TestFinalizeIndex_OwnersOfFileDirectFileEscape(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	testutil.WriteFile(
		t,
		dir,
		"apps/homelab/kustomization.yaml",
		"apiVersion: kustomize.config.k8s.io/v1beta1\nkind: Kustomization\nresources:\n  - ../base/postgres/cluster.yaml\n",
	)
	testutil.WriteFile(t, dir, "apps/base/postgres/cluster.yaml",
		"apiVersion: postgresql.cnpg.io/v1\nkind: Cluster\nmetadata:\n  name: postgres\n")

	s := store.New()
	ks := &manifest.Kustomization{Name: "apps", Namespace: "flux-system", Path: "./apps/homelab"}
	s.AddObject(ks)

	idx := rendererFixture(t, loaderFixture(s, nil), dir)

	{
		got := ownersForTest(idx, "apps/base/postgres/cluster.yaml")
		assert.ErrorIf(
			t,
			!slices.Contains(got, ks.Named()),
			"OwnersOfFile(escaped file cluster.yaml) = %v, want to contain apps",
			got,
		)
	}
}

type missingResourceFS struct {
	fs.FS

	missing              string
	statError            error
	opens, stats, probes int
}

func (f *missingResourceFS) Open(name string) (fs.File, error) {
	if name == f.missing {
		f.opens++
	} else if strings.HasPrefix(name, f.missing+"/") {
		f.probes++
	}
	return f.FS.Open(name)
}

func (f *missingResourceFS) Stat(name string) (fs.FileInfo, error) {
	if name == f.missing {
		f.stats++
		if f.statError != nil {
			return nil, f.statError
		}
	}
	return fs.Stat(f.FS, name)
}

func TestFinalizeIndex_OwnersOfFileDanglingResource(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name      string
		statError error
		present   bool
		preload   bool
		wantOpens int
	}{
		{name: "cached_file", present: true, preload: true},
		{name: "unscanned_file", present: true, wantOpens: 1},
		{name: "absent"},
		{name: "stat_permission", statError: fs.ErrPermission, wantOpens: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			testFinalizeIndexOwnersOfFileDanglingResourceCase(
				t,
				&tc,
			)
		},
		)
	}
}

// TestFinalizeIndex_OwnersOfFileDanglingDirectory: the directory
// form of the dangling case. A resources: entry pointing at a directory
// that no longer exists records only the directory path, but change
// detection reports the deleted files individually — OwnersOfFile must
// resolve them through the recorded parent directory.
func TestFinalizeIndex_OwnersOfFileDanglingDirectory(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	testutil.WriteFile(t, dir, "apps/homelab/kustomization.yaml",
		"apiVersion: kustomize.config.k8s.io/v1beta1\nkind: Kustomization\nresources:\n  - ../base/postgres\n")
	// A peer overlay directly references a deleted file beneath the deleted
	// directory: its exact-key owner must not mask the directory owner.
	testutil.WriteFile(
		t,
		dir,
		"apps/other/kustomization.yaml",
		"apiVersion: kustomize.config.k8s.io/v1beta1\nkind: Kustomization\nresources:\n  - ../base/postgres/cluster.yaml\n",
	)

	s := store.New()
	ks := &manifest.Kustomization{Name: "apps", Namespace: "flux-system", Path: "./apps/homelab"}
	other := &manifest.Kustomization{Name: "other", Namespace: "flux-system", Path: "./apps/other"}
	s.AddObject(ks)
	s.AddObject(other)

	idx := rendererFixture(t, loaderFixture(s, nil), dir)

	for _, file := range []string{
		"apps/base/postgres/kustomization.yaml",
		"apps/base/postgres/nested/cluster.yaml",
	} {
		{
			got := ownersForTest(idx, file)
			assert.ErrorIf(
				t,
				!slices.Contains(got, ks.Named()),
				"OwnersOfFile(%s under deleted dir) = %v, want to contain apps",
				file,
				got,
			)
		}
	}
	// Owned by both: other by its exact entry, apps through the deleted dir.
	{
		got := ownersForTest(
			idx,
			"apps/base/postgres/cluster.yaml",
		)
		assert.ErrorIf(t, !slices.Contains(got, ks.Named()) ||
			!slices.Contains(
				got,
				other.Named(),
			), "OwnersOfFile(deleted cluster.yaml) = %v, want to contain both apps and other", got)
	}
	{
		got := ownersForTest(idx, "apps/base/redis/cm.yaml")
		assert.ErrorIf(t, got != nil, "OwnersOfFile(unrelated file) = %v, want nil", got)
	}
}

func testSelfProduceBuilderRecordProducedReuseCase(
	t *testing.T,
	testCaseInput *string,
) {
	t.Helper()
	s := store.New()
	index := &manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{}}
	builder := selfProduceBuilder{idx: index, store: s, emittedFiles: map[string][]manifest.NamedResource{}}
	l := New(s)
	obj := &manifest.Kustomization{
		Name:      "child",
		SourceRef: kustomizev1.CrossNamespaceSourceReference{Kind: manifest.KindGitRepository, Name: "input"},
	}
	children := []parsedObject{{Obj: obj, version: manifest.FluxKustomizeDomain + "/v1"}}
	builder.reader.cache = map[string]document{"child.yaml": {objects: children}}
	visits := make([]graphVisit, 0, 2)
	for _, name := range []string{"a", "z"} {
		owner := &manifest.Kustomization{Name: name, Namespace: "apps"}
		s.AddObject(owner)
		visit := graphVisit{
			namePrefix: (*testCaseInput),
			owner:      owner.Named(),
			namespace:  "apps",
			file:       "child.yaml",
			coordinate: "child.yaml",
		}
		visits = append(visits, visit)
		_, err := builder.recordProduced(t.Context(), new(visit), nil, l)
		assert.NoError(t, err)
	}
	{
		got := testing.AllocsPerRun(100, func() {
			for i := range visits {
				visit := visits[i]
				_, err := builder.recordProduced(t.Context(), new(visit), nil, l)
				if err != nil {
					t.Fatal(err)
				}
			}
		})
		assert.FatalIf(t, got != 0, "repeated production capture allocated %v times, want zero", got)
	}
	id := manifest.Resolve(obj.Named(), "apps", "")
	id.Name = (*testCaseInput) + id.Name
	if (*testCaseInput) != "" {
		assert.Equal(t, len(l.scopes.projections[0])+len(l.scopes.projections[1]), 1)
	}
	assert.Diff(
		t,
		index.Records[id].Refs,
		[]manifest.NamedResource{
			{Group: manifest.SourceDomain, Kind: manifest.KindGitRepository, Namespace: "apps", Name: "input"},
		},
	)
	assert.Equal(t, len(index.Records[id].Productions), 2)
}

func testFinalizeIndexBareDirComponentNamespaceCase(
	t *testing.T,
	testCaseInput *struct {
		name      string
		target    string
		local     string
		namespace string
	},
) {
	t.Helper()

	dir := t.TempDir()
	testutil.WriteFile(
		t,
		dir,
		"kubernetes/apps/flux-system/kustomization.yaml",
		"apiVersion: kustomize.config.k8s.io/v1beta1\nkind: Kustomization\nnamespace: flux-system\ncomponents:\n  - ../../components/substitutions\n",
	)
	testutil.WriteFile(
		t,
		dir,
		"kubernetes/apps/default/kustomization.yaml",
		"apiVersion: kustomize.config.k8s.io/v1beta1\nkind: Kustomization\nnamespace: default\ncomponents:\n  - ../../components/substitutions\n",
	)
	testutil.WriteFile(
		t,
		dir,
		"kubernetes/components/substitutions/kustomization.yaml",
		"apiVersion: kustomize.config.k8s.io/v1alpha1\nkind: Component\nresources:\n  - ./cluster-settings.yaml\n",
	)
	testutil.WriteFile(t, dir, "kubernetes/components/substitutions/cluster-settings.yaml",
		"apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: cluster-settings\ndata:\n  CLUSTER_NAME: home\n"+
			"---\napiVersion: foreign.example/v1\nkind: ConfigMap\nmetadata: {name: foreign-config}\ndata: {VALUE: foreign}\n"+
			"---\napiVersion: foreign.example/v1\nkind: Secret\nmetadata: {name: foreign-secret}\nstringData: {VALUE: foreign}\n")

	testutil.WriteFile(
		t,
		dir,
		"kubernetes/apps/bare/kustomization.yaml",
		testCaseInput.local+"resources: [child.yaml]\nconfigMapGenerator:\n  - name: generated\n    literals: [CLUSTER_NAME=home]\nsecretGenerator:\n  - name: generated-secret\n    literals: [KEY=value]\n",
	)
	testutil.WriteFile(
		t,
		dir,
		"kubernetes/apps/bare/child.yaml",
		testutil.KustomizationYAML(
			"consumer",
			"",
			"  path: ./leaf\n  postBuild:\n    substituteFrom: [{kind: ConfigMap, name: generated}, {kind: Secret, name: generated-secret}]\n",
		),
	)
	testutil.WriteFile(t, dir, "leaf/kustomization.yaml", "resources: []\n")
	s := store.New()
	clusterApps := &manifest.Kustomization{
		Name:      "cluster-apps",
		Namespace: "flux-system",
		Path:      "./kubernetes/apps",
	}
	clusterApps.TargetNamespace = testCaseInput.target
	s.AddObject(clusterApps)

	admitForTest(s, dir)
	l := loaderFixture(s, nil)
	l.SourceRoot = dir
	{
		_, err := loadForTest(t.Context(), t, l, filepath.Join(dir, "kubernetes", "apps", "bare"))
		assert.NoError(t, err)
	}
	{
		err := l.FinalizeIndex(t.Context(), dir, true)
		assert.NoError(t, err)
	}
	idx := l.Index
	for _, id := range []manifest.NamedResource{
		{Kind: manifest.KindConfigMap, Name: "foreign-config"},
		{Kind: manifest.KindSecret, Name: "foreign-secret"},
	} {
		for _, ns := range []string{"", "flux-system", "default", "target"} {
			id.Namespace = ns
			{
				_, ok := idx.Lookup(id)
				assert.FatalIf(t, ok, "foreign data lookalike indexed: %s", id)
			}
		}
	}

	namespaces := []string{"flux-system", "default"}
	if testCaseInput.target != "" {
		namespaces = []string{testCaseInput.target}
	}
	for _, ns := range namespaces {
		{
			got := renderers(idx, cmID(ns))
			assert.ErrorIf(
				t,
				!slices.Contains(got, clusterApps.Named()),
				"ProducedBy(ConfigMap/%s/cluster-settings) = %v, want to contain cluster-apps",
				ns,
				got,
			)
		}
	}
	secret := manifest.NamedResource{
		Kind:      manifest.KindSecret,
		Namespace: testCaseInput.namespace,
		Name:      "generated-secret",
	}
	{
		got := renderers(idx, secret)
		assert.ErrorIf(
			t,
			!slices.Contains(got, clusterApps.Named()),
			"generated secret %s owners=%v",
			secret,
			got,
		)
	}
	generated := manifest.NamedResource{
		Kind:      manifest.KindConfigMap,
		Namespace: testCaseInput.namespace,
		Name:      "generated",
	}
	{
		got := renderers(idx, generated)
		assert.ErrorIf(
			t,
			!slices.Contains(got, clusterApps.Named()),
			"generated ConfigMap %s owners=%v",
			generated,
			got,
		)
	}
	consumer := testutil.KustomizationID(testCaseInput.namespace, "consumer")
	record, ok := idx.Lookup(consumer)
	assert.FatalIf(t, !ok,
		"consumer %s not indexed: records=%+v", consumer, idx.Records)

	assert.Diff(t, record.Refs, []manifest.NamedResource{generated})
	obj, ok := candidateForTest(t, l, consumer).(*manifest.Kustomization)
	assert.FatalIf(t, !ok, "consumer %s not stored", consumer)
	refs := 0
	for ref := range manifest.References(obj, manifest.ReferenceRelations) {
		if ref.Use == "substituteFrom" {
			refs++
			assert.Equal(t, ref.Namespace, testCaseInput.namespace)
			{
				_, ok := idx.Lookup(ref.NamedResource)
				assert.ErrorIf(t, !ok, "consumer input %s not indexed", ref.NamedResource)
			}
		}
	}
	assert.Equal(t, refs, 2)
	for _, id := range []manifest.NamedResource{generated, secret, consumer} {
		wrong := id
		wrong.Namespace = "flux-system"
		{
			_, ok := idx.Lookup(wrong)
			assert.ErrorIf(t, ok, "parent namespace leaked into %s", wrong)
		}
	}

	{
		got := renderers(idx, cmID("kube-system"))
		assert.ErrorIf(
			t,
			len(got) != 0,
			"ProducedBy(ConfigMap/kube-system/cluster-settings) = %v, want empty",
			got,
		)
	}
}
