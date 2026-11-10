package discovery

import (
	"encoding/json"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"testing/fstest"

	kustomizev1 "github.com/fluxcd/kustomize-controller/api/v1"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/pkg/kustomize"
	"github.com/home-operations/flate/pkg/loader"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/store"
	"github.com/home-operations/flate/pkg/task"
)

func TestRun_EntryOwnerEligibility(t *testing.T) {
	t.Parallel()
	t.Run("independent", func(t *testing.T) { t.Parallel(); testRunEntryOwnerEligibilityIndependent(t) })
	for _, scenario := range []string{"transitive_index_only", "transitive_admitted", "effective_batch", "generated_target", "production_declaration", "owner_production", "foreign_production", "cycle", "self_cycle", "deferred_required", "deferred_optional", "stable_then_changed"} {
		t.Run(scenario, func(t *testing.T) {
			t.Parallel()
			testRunEntryOwnerEligibilityCase(t, &scenario)
		},
		)
	}
}

func TestRun_BuiltEntryDoesNotRebindTransformedOwner(t *testing.T) {
	t.Parallel()
	const root = "/virtual/transformed-entry-owner"
	input := fstest.MapFS{
		"app/kustomization.yaml": {
			Data: []byte(
				"resources: [owner.yaml]\npatches:\n- target: {kind: Kustomization, name: owner}\n  patch: '[{\"op\":\"replace\",\"path\":\"/spec/path\",\"value\":\"./app\"}]'\n",
			),
		},
		"app/owner.yaml": {Data: []byte(testutil.KustomizationYAML("owner", "apps", "  path: ./other\n"))},
	}
	st := store.New()
	cacheDir := t.TempDir()
	testReceiptTreesResult1 := testReceiptTrees(t, cacheDir)
	trees, closeCache := testReceiptTreesResult1.trees, testReceiptTreesResult1.close
	source := &store.SourceArtifact{LocalPath: root, FS: input}
	cfg := Config{Input: source, Path: root + "/app", Store: st, Trees: trees}
	result, err := Run(t.Context(), cfg)
	assert.NoError(t, err)
	if !result.Binding.Producer.ReportHidden() {
		t.Fatalf("transformed child replaced the literal entry: %+v",
			result.Binding,
		)
	}
	owner := testutil.KustomizationID("apps", "owner")
	record, ok := result.Index.Lookup(owner)
	declaration, typed := record.Declaration.(*manifest.Kustomization)
	assert.FatalIf(t, !ok || !typed || declaration.Path != "./app" || record.File != "app/kustomization.yaml" ||
		record.Builder != record.File ||
		record.Parent != result.Binding.Producer, "transformed declaration lost its prepared ownership: %+v", record)
	assert.FatalIf(t, st.GetObject(owner) != nil, "%v", "prepared child was published before ordinary runtime emission")
	producer := st.GetObject(result.Binding.Producer).(*manifest.Kustomization)
	req := preparedRequest(t, &cfg, producer, source)
	want := map[manifest.NamedResource]manifest.DiscoveryRecord{owner: {
		Application:  `{"apiVersion":"kustomize.toolkit.fluxcd.io/v1","kind":"Kustomization","metadata":{"name":"owner","namespace":"apps"},"spec":{"path":"./app"}}`,
		Productions:  []manifest.Production{{Owner: producer.Named(), Source: result.Binding.Source}},
		ContentRoots: map[manifest.NamedResource][]string{manifest.BootstrapSourceID: {"app"}},
	}}
	assert.Diff(t, packageHistory(t, trees, &req, producer.Named()), want)
	closeCache()
	testReceiptTreesResult2 := testReceiptTrees(t, cacheDir)
	trees, closeCache = testReceiptTreesResult2.trees, testReceiptTreesResult2.close
	assert.Diff(t, packageHistory(t, trees, &req, producer.Named()), want)
	assert.FatalIf(
		t,
		previousPackageHistory(t, trees, &req).Owner == owner,
		"%v",
		"transformed child replaced the accepted producer",
	)
	closeCache()
}

func TestPrepareEntry_BuildFailurePreservesRawErrors(t *testing.T) {
	t.Parallel()
	for _, failure := range []string{"build", "document"} {
		t.Run(failure, func(t *testing.T) {
			t.Parallel()
			root := t.TempDir()
			input := fstest.MapFS{
				"app/kustomization.yaml": {
					Data: []byte("resources: [selected.yaml]\npatches: [{path: missing.yaml}]\n"),
				},
				"app/selected.yaml": {Data: []byte(`apiVersion: fluxcd.controlplane.io/v1
kind: ResourceSet
metadata: {name: selected}
spec:
  dependsOn: [{apiVersion: bad/group/version, kind: ConfigMap, name: dependency}]
`)},
			}
			if failure == "document" {
				input["app/kustomization.yaml"].Data = []byte("resources: [good.yaml, selected.yaml]\n")
				input["app/good.yaml"] = &fstest.MapFile{
					Data: []byte("apiVersion: v1\nkind: ConfigMap\nmetadata: {name: first}\n"),
				}
			}
			for name, file := range input {
				testutil.WriteFile(t, root, name, string(file.Data))
			}
			cacheDir := t.TempDir()
			cacheOwner := cachetest.New(t, cacheDir, cache.New)
			trees := kustomize.NewTreeCache(cacheOwner, nil, task.NewBounded(2))
			closeCache := trees.Close
			t.Cleanup(closeCache)
			source := &store.SourceArtifact{
				Kind:      manifest.KindGitRepository,
				LocalPath: root,
				LocalRoot: root,
				FS:        os.DirFS(root),
			}
			st := store.New()
			st.SetArtifact(manifest.BootstrapSourceID, source)
			d := Session{cfg: Config{Path: filepath.Join(root, "app"), Store: st, Trees: trees}, loader: loader.New(st)}
			d.loader.SourceRoot = root
			{
				_, err := d.loader.Load(t.Context(), d.cfg.Path, manifest.NamedResource{}, nil)
				assert.FatalIf(t, err != nil, "%v", err)
			}
			rawError := d.loader.EntryFailure(t.Context(), root)
			assert.FatalIf(
				t,
				!errors.Is(rawError, manifest.ErrInput),
				"raw selected error = %v, want ErrInput",
				rawError,
			)
			owner := &manifest.Kustomization{Name: "entry", Path: "./app"}
			d.binding = Binding{Producer: owner.Named(), Source: manifest.BootstrapSourceID}
			{
				err := d.prepareProducer(
					t.Context(),
					owner,
					st.GetArtifact(manifest.BootstrapSourceID, store.ArtifactContent).(*store.SourceArtifact),
					nil,
				)
				assert.FatalIf(
					t,
					!errors.Is(err, manifest.ErrFlux),
					"invalid package build error = %v, want ErrFlux",
					err,
				)
			}
			{
				got := d.loader.EntryFailure(t.Context(), root)
				assert.FatalIf(
					t,
					got == nil || got.Error() != rawError.Error(),
					"%v",
					"failed entry build replaced its raw error",
				)
			}
			_, _, emission := st.GetEmission(owner.Named())
			defer emission.Finish()
			assert.FatalIf(
				t,
				emission.Complete || len(emission.Manifests) != 0,
				"failed package build published documents: %v",
				emission.Manifests,
			)
			first := manifest.NamedResource{Kind: manifest.KindConfigMap, Name: "first"}
			assert.FatalIf(
				t,
				d.loader.PromotionDocuments(first) != nil || st.GetObject(first) != nil,
				"%v",
				"failed package published a partial batch",
			)
			closeCache()
			assert.NoError(t, cacheOwner.Close())
			testReceiptTreesResult3 := testReceiptTrees(t, cacheDir)
			trees, closeCache = testReceiptTreesResult3.trees, testReceiptTreesResult3.close
			req := preparedRequest(t, &d.cfg, owner, source)
			assert.FatalIf(
				t,
				previousPackageHistory(t, trees, &req).Records != nil,
				"%v",
				"failed package persisted a partial accepted batch",
			)
			testutil.WriteFile(t, root, "app/kustomization.yaml", "resources: [selected.yaml]\n")
			const selectedFile = "app/selected.yaml"
			testutil.WriteFile(
				t,
				root,
				selectedFile,
				strings.ReplaceAll(string(input[selectedFile].Data), "bad/group/version", "v1"),
			)
			st = store.New()
			st.SetArtifact(manifest.BootstrapSourceID, source)
			d = Session{cfg: Config{Path: filepath.Join(root, "app"), Store: st, Trees: trees}, loader: loader.New(st)}
			d.loader.SourceRoot = root
			_, err := d.loader.Load(t.Context(), d.cfg.Path, manifest.NamedResource{}, nil)
			assert.NoError(t, err)
			d.binding = Binding{Producer: owner.Named(), Source: manifest.BootstrapSourceID}
			assert.NoError(t, d.prepareProducer(t.Context(), owner, source, nil))
			assert.NoError(t, d.acceptPrepared(t.Context()))
			assert.Equal(t, len(packageHistory(t, trees, &req, owner.Named())), 1)
			closeCache()
			testReceiptTreesResult4 := testReceiptTrees(t, cacheDir)
			trees, closeCache = testReceiptTreesResult4.trees, testReceiptTreesResult4.close
			assert.Equal(t, len(packageHistory(t, trees, &req, owner.Named())), 1)
			closeCache()
		})
	}
}

func TestPrepareEntry_PublicationFailure(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	testutil.WriteFile(t, root, "app/kustomization.yaml", "resources: [cm.yaml]\n")
	testutil.WriteFile(t, root, "app/cm.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: accepted}\n")
	cacheDir := t.TempDir()
	testReceiptTreesResult5 := testReceiptTrees(t, cacheDir)
	trees, closeCache := testReceiptTreesResult5.trees, testReceiptTreesResult5.close
	source := &store.SourceArtifact{LocalPath: root, FS: os.DirFS(root)}
	st := store.New()
	st.SetArtifact(manifest.BootstrapSourceID, source)
	d := Session{cfg: Config{Path: filepath.Join(root, "app"), Store: st, Trees: trees}, loader: loader.New(st)}
	d.loader.SourceRoot = root
	_, err := d.loader.Load(t.Context(), d.cfg.Path, manifest.NamedResource{}, nil)
	assert.NoError(t, err)
	owner, err := d.bindEntry(t.Context(), root)
	assert.NoError(t, err)
	req := preparedRequest(t, &d.cfg, owner, source)
	assert.NoError(t, os.RemoveAll(cacheDir))
	assert.NoError(t, d.prepareProducer(t.Context(), owner, source, nil))
	err = d.acceptPrepared(t.Context())
	assert.FatalIf(t, !errors.Is(err, manifest.ErrFlux) || !errors.Is(err, fs.ErrNotExist) ||
		!strings.Contains(
			err.Error(),
			`discovery entry "app" publish`,
		), "publication failure lost classification, cause or entry context: %v", err)
	assert.FatalIf(
		t,
		len(d.loader.PromotionDocuments(manifest.NamedResource{Kind: manifest.KindConfigMap, Name: "accepted"})) != 1,
		"%v",
		"publication fault preceded successful rendering and registration",
	)
	closeCache()
	testReceiptTreesResult6 := testReceiptTrees(t, cacheDir)
	trees, closeCache = testReceiptTreesResult6.trees, testReceiptTreesResult6.close
	assert.FatalIf(
		t,
		previousPackageHistory(t, trees, &req).Records != nil,
		"%v",
		"failed publication left durable accepted history",
	)
	closeCache()
}

func TestAdmitEntry_BuildFailurePreservesRawErrors(t *testing.T) {
	t.Parallel()
	const root = "/virtual/failed-entry"
	input := fstest.MapFS{
		"kustomization.yaml": {Data: []byte("resources: [cm.yaml]\npatches: [{path: missing.yaml}]\n")},
		"cm.yaml":            {Data: []byte("apiVersion: v1\nkind: ConfigMap\nmetadata: {name: first}\n")},
	}
	st := store.New()
	result, err := Run(
		t.Context(),
		Config{Input: &store.SourceArtifact{LocalPath: root, FS: input}, Path: root, Store: st, Trees: testTrees(t)},
	)
	assert.NoError(t, err)
	assert.FatalIf(
		t,
		st.GetObject(result.Binding.Producer) == nil,
		"%v",
		"failed preparation lost the ordinary producer",
	)
	_, _, emission := st.GetEmission(result.Binding.Producer)
	defer emission.Finish()
	assert.FatalIf(
		t,
		emission.Complete || len(emission.Manifests) != 0,
		"%v",
		"failed preparation published a successful or partial emission",
	)
	record, ok := result.Index.Lookup(manifest.NamedResource{Kind: manifest.KindConfigMap, Name: "first"})
	assert.FatalIf(t, !ok ||
		!slices.ContainsFunc(
			record.Productions,
			func(p manifest.Production) bool { return p.Owner == result.Binding.Producer },
		), "failed root lost structural ancestry: %+v", record)
}

func TestRun_UnappliedPackage(t *testing.T) {
	t.Parallel()
	const root = "/virtual/unapplied-package"
	input := fstest.MapFS{
		"entry/source.yaml": {
			Data: []byte(
				"apiVersion: source.toolkit.fluxcd.io/v1\nkind: GitRepository\nmetadata: {name: self, namespace: apps}\nspec: {url: https://fixture.invalid/self}\n",
			),
		},
		"entry/generator.yaml": {
			Data: []byte(
				"apiVersion: helm.toolkit.fluxcd.io/v2\nkind: HelmRelease\nmetadata: {name: generator, namespace: apps}\nspec:\n  chart:\n    spec:\n      chart: charts/bootstrap\n      sourceRef: {kind: GitRepository, name: self}\n",
			),
		},
		"entry/unapplied/kustomization.yaml": {Data: []byte("resources: [cm.yaml]\npatches: [{path: missing.yaml}]\n")},
		"entry/unapplied/cm.yaml": {
			Data: []byte("apiVersion: v1\nkind: ConfigMap\nmetadata: {name: unused}\n"),
		},
	}
	st := store.New()
	result, err := Run(t.Context(), Config{
		Input: &store.SourceArtifact{LocalPath: root, FS: input}, Path: filepath.Join(root, "entry"),
		Store: st, Trees: testTrees(t), SelfURLs: []string{"https://fixture.invalid/self"},
	})
	assert.NoError(t, err)
	assert.FatalIf(t, result.Binding != (Binding{}), "%v", "unrendered application qualified the entry")
	assert.FatalIf(
		t,
		st.GetObject(testutil.HelmReleaseID("apps", "generator")) == nil,
		"%v",
		"unapplied package hid the application producer",
	)
}

func TestRun_BuiltPackages(t *testing.T) {
	t.Parallel()
	for _, selected := range []string{".", "a"} {
		t.Run(selected, func(t *testing.T) {
			t.Parallel()
			testRunBuiltPackagesCase(t, &selected)
		},
		)
	}
}

func TestRun_EmptyEntryHistory(t *testing.T) {
	t.Parallel()
	const root = "/virtual/empty-entry"
	input := fstest.MapFS{"kustomization.yaml": {Data: []byte("resources: []\n")}}
	cacheDir := t.TempDir()
	testReceiptTreesResult7 := testReceiptTrees(t, cacheDir)
	trees, closeCache := testReceiptTreesResult7.trees, testReceiptTreesResult7.close
	st := store.New()
	source := &store.SourceArtifact{LocalPath: root, FS: input}
	result, err := Run(t.Context(), Config{Input: source, Path: root, Store: st, Trees: trees})
	assert.NoError(t, err)
	owner := st.GetObject(result.Binding.Producer).(*manifest.Kustomization)
	req := preparedRequest(t, &Config{}, owner, source)
	assert.Equal(t, len(packageHistory(t, trees, &req, owner.Named())), 0)
	closeCache()
	testReceiptTreesResult8 := testReceiptTrees(t, cacheDir)
	trees, closeCache = testReceiptTreesResult8.trees, testReceiptTreesResult8.close
	assert.Equal(t, len(packageHistory(t, trees, &req, owner.Named())), 0)
	closeCache()
}

func TestRun_DiscoveryOnlyEntry(t *testing.T) {
	t.Parallel()
	const root = "/virtual/discovery-only"
	input := fstest.MapFS{
		"Taskfile.yaml":        {Data: []byte("version: '3'\ntasks: {build: {cmds: [true]}}\n")},
		"clusters/a/sync.yaml": {Data: []byte(testutil.KustomizationYAML("a", "apps", "  path: ./clusters/a\n"))},
		"clusters/b/sync.yaml": {Data: []byte(testutil.KustomizationYAML("b", "apps", "  path: ./clusters/b\n"))},
	}
	st := store.New()
	observed := &entryGeneratorFS{FS: input}
	result, err := Run(
		t.Context(),
		Config{Input: &store.SourceArtifact{LocalPath: root, FS: observed}, Path: root, Store: st, Trees: testTrees(t)},
	)
	assert.NoError(t, err)
	assert.Equal(t, result.Binding, Binding{})
	assert.Equal(t, observed.calls.Load(), int64(0))
	for _, name := range []string{"a", "b"} {
		assert.FatalIf(
			t,
			st.GetObject(testutil.KustomizationID("apps", name)) == nil,
			"cluster %s was not discovered",
			name,
		)
	}
	for _, obj := range st.ListObjects(manifest.FluxKustomizeDomain, manifest.KindKustomization) {
		if obj.Named().ReportHidden() {
			t.Fatalf("discovery-only entry published %s", obj.Named())
		}
	}
}

func bindEntryForTest(t *testing.T, d *Session, root string) *manifest.Kustomization {
	t.Helper()
	owner, err := d.bindEntry(t.Context(), root)
	assert.NoError(t, err)
	return owner
}

func testRunEntryOwnerEligibilityIndependent(t *testing.T) {
	t.Helper()

	const root = "/virtual/independent-entry-owner"
	input := fstest.MapFS{
		"entry/kustomization.yaml": {Data: []byte("resources: [owner.yaml, cm.yaml]\n")},
		"entry/owner.yaml": {
			Data: []byte(
				testutil.KustomizationYAML("owner", "apps", "  path: ./entry\n  dependsOn: [{name: ready}]\n"),
			),
		},
		"entry/cm.yaml": {
			Data: []byte("apiVersion: v1\nkind: ConfigMap\nmetadata: {name: output, namespace: apps}\n"),
		},
		"ready/kustomization.yaml": {Data: []byte("resources: []\n")},
	}
	st := store.New()
	st.AddObject(&manifest.Kustomization{Name: "ready", Namespace: "apps", Path: "./ready"})
	result, err := Run(
		t.Context(),
		Config{
			Store: st,
			Trees: testTrees(t),
			Input: &store.SourceArtifact{LocalPath: root, FS: input},
			Path:  root + "/entry",
		},
	)
	assert.NoError(t, err)
	assert.Equal(t, result.Binding.Producer, testutil.KustomizationID("apps", "owner"))
}

func testRunEntryOwnerEligibilityCase(t *testing.T, testCaseInput *string,
) {
	t.Helper()

	const root = "/virtual/entry-eligibility"
	input := fstest.MapFS{
		"entry/kustomization.yaml": {Data: []byte("resources: [owner.yaml, sibling.yaml]\n")},
		"entry/owner.yaml": {
			Data: []byte(
				testutil.KustomizationYAML("owner", "apps", "  path: ./entry\n  dependsOn: [{name: ready}]\n"),
			),
		},
		"entry/sibling.yaml": {
			Data: []byte(testutil.KustomizationYAML("sibling", "apps", "  path: ./leaf\n")),
		},
		"leaf/kustomization.yaml": {Data: []byte("resources: []\n")},
	}
	owner := &manifest.Kustomization{Name: "owner", Namespace: "apps", Path: "./entry"}
	owner.DependsOn = []kustomizev1.DependencyReference{{Name: "ready"}}
	ready := &manifest.Kustomization{Name: "ready", Namespace: "apps", Path: "./ready"}
	ready.DependsOn = []kustomizev1.DependencyReference{{Name: "sibling"}}
	if (*testCaseInput) == "effective_batch" {
		input["entry/sibling.yaml"].Data = []byte(
			testutil.KustomizationYAML("before", "other", "  path: ./leaf\n"),
		)
		input["entry/kustomization.yaml"].Data = []byte(
			"namespace: apps\nresources: [sibling.yaml]\npatches:\n- target: {kind: Kustomization, name: before}\n  patch: '[{\"op\":\"replace\",\"path\":\"/metadata/name\",\"value\":\"sibling\"}]'\n",
		)
	}
	if (*testCaseInput) == "generated_target" {
		ready.DependsOn = nil
		ready.SourceRef = kustomizev1.CrossNamespaceSourceReference{
			Kind: manifest.KindExternalArtifact,
			Name: "generated",
		}
		input["entry/kustomization.yaml"].Data = []byte("resources: [generator.yaml]\n")
		input["entry/generator.yaml"] = &fstest.MapFile{
			Data: []byte(
				"apiVersion: source.extensions.fluxcd.io/v1beta1\nkind: ArtifactGenerator\nmetadata: {name: generator, namespace: apps}\nspec:\n  artifacts: [{name: generated}]\n",
			),
		}
	}
	if (*testCaseInput) == "cycle" {
		ready.DependsOn[0].Name = "loop"
	}
	if (*testCaseInput) == "self_cycle" {
		owner.DependsOn[0].Name = owner.Name
	}
	if (*testCaseInput) == "deferred_required" || (*testCaseInput) == "deferred_optional" {
		owner.PostBuild = &kustomizev1.PostBuild{
			SubstituteFrom: []kustomizev1.SubstituteReference{
				{
					Kind:     manifest.KindConfigMap,
					Name:     "values",
					Optional: (*testCaseInput) == "deferred_optional",
				},
			},
		}
	}
	st := store.New()
	encoded, err := json.Marshal(testutil.ObjectDocument(owner))
	assert.NoError(t, err)
	input["entry/owner.yaml"].Data = encoded
	st.AddObject(owner)
	if (*testCaseInput) == "transitive_admitted" {
		st.AddObject(ready)
	}
	d := newDiscoverer(
		&Config{
			Store: st,
			Trees: testTrees(t),
			Input: &store.SourceArtifact{LocalPath: root, FS: input},
			Path:  root + "/entry",
		},
	)
	_, err = d.resolveEntrySources(t.Context(), false)
	assert.NoError(t, err)
	d.loader.SourceRoot = root
	_, err = d.loader.Load(t.Context(), root+"/entry", manifest.NamedResource{}, nil)
	assert.NoError(t, err)
	d.loader.Index.Records[ready.Named()] = manifest.DiscoveryRecord{Declaration: ready}
	if (*testCaseInput) == "cycle" {
		loop := &manifest.Kustomization{Name: "loop", Namespace: "apps"}
		loop.DependsOn = []kustomizev1.DependencyReference{{Name: ready.Name}}
		d.loader.Index.Records[loop.Named()] = manifest.DiscoveryRecord{Declaration: loop}
	}
	testRunEntryOwnerEligibilityCaseTail(
		t,
		&testRunEntryOwnerEligibilityCaseTailInputs{
			testCaseInput: &testCaseInput,
			ready:         &ready,
			d:             &d,
			owner:         &owner,
			st:            &st,
			root:          root,
		},
	)
}

func testRunEntryOwnerEligibilityCaseTail(t *testing.T, tailCaseInput *testRunEntryOwnerEligibilityCaseTailInputs) {
	t.Helper()
	if (*(*tailCaseInput.testCaseInput)) == "production_declaration" ||
		(*(*tailCaseInput.testCaseInput)) == "owner_production" ||
		(*(*tailCaseInput.testCaseInput)) ==
			"foreign_production" {
		(*tailCaseInput.ready).
			DependsOn = nil
		source := manifest.BootstrapSourceID
		if (*(*tailCaseInput.testCaseInput)) == "foreign_production" {
			source.Name = "foreign"
		}
		(*tailCaseInput.d).
			loader.Index.Records[(*tailCaseInput.ready).Named()] = manifest.DiscoveryRecord{
			Productions: []manifest.Production{
				{
					Owner:       testutil.KustomizationID("apps", "producer"),
					Declaration: testutil.KustomizationID("apps", "sibling"),
					Source:      source,
				},
			},
		}
		if (*(*tailCaseInput.testCaseInput)) == "owner_production" {
			(*tailCaseInput.d).
				loader.Index.Records[(*tailCaseInput.ready).Named()] = manifest.DiscoveryRecord{
				Productions: []manifest.Production{{Owner: (*tailCaseInput.owner).Named(), Source: source}},
			}
		}
	}
	assert.FatalIf(
		t,
		(*(*tailCaseInput.testCaseInput)) != "transitive_admitted" && (*tailCaseInput.st).GetObject((*tailCaseInput.ready).Named()) != nil ||
			len((*tailCaseInput.d).
				loader.Index.Records[(*tailCaseInput.ready).Named()].Refs,
			) != 0,
		"%v",
		"index-only prerequisite gained store or ordering-ref authority",
	)
	selected, err := (*tailCaseInput.d).bindEntry(t.Context(), tailCaseInput.root)
	assert.NoError(t, err)
	wantOwner := (*(*tailCaseInput.testCaseInput)) == "cycle" || (*(*tailCaseInput.testCaseInput)) == "self_cycle" ||
		(*(*tailCaseInput.testCaseInput)) == "foreign_production"
	assert.Equal(t, selected == (*tailCaseInput.owner), wantOwner)
	assert.FatalIf(
		t,
		!wantOwner && !selected.Named().ReportHidden(),
		"%v",
		"captured prerequisite did not select the literal entry",
	)
	if (*(*tailCaseInput.testCaseInput)) == "deferred_required" ||
		(*(*tailCaseInput.testCaseInput)) == "deferred_optional" {
		{
			_, prepared := (*tailCaseInput.d).prepared[(*tailCaseInput.owner).Named()]
			assert.FatalIf(t, prepared, "%v", "referenced substitution admitted an incomplete preparation")
		}
	}
	if (*(*tailCaseInput.testCaseInput)) == "effective_batch" {
		assert.FatalIf(t, !slices.ContainsFunc((*tailCaseInput.d).
			prepared[(*tailCaseInput.owner).Named()].children, func(c store.Child) bool { return c.Obj.Named() == testutil.KustomizationID("apps", "sibling") },
		), "%v", "eligibility did not inspect the transformed batch")
	}
	if (*(*tailCaseInput.testCaseInput)) == "stable_then_changed" {
		previous := (*tailCaseInput.d).prepared[(*tailCaseInput.owner).Named()].receipt
		input, _, err := kustomize.SourceInput((*tailCaseInput.st), selected)
		assert.NoError(t, err)
		assert.NoError(t, (*tailCaseInput.d).prepareProducer(t.Context(), selected, input, nil))
		declaration := (*tailCaseInput.d).loader.Index.Records[(*tailCaseInput.owner).Named()].Declaration
		assert.FatalIf(
			t,
			declaration == nil || !manifest.Equal(declaration, (*tailCaseInput.owner)),
			"%v",
			"literal registration erased the original authored declaration",
		)
		assert.Equal(t, bindEntryForTest(t, (*tailCaseInput.d), tailCaseInput.root), selected)
		assert.Equal(t, (*tailCaseInput.d).prepared[(*tailCaseInput.owner).Named()].receipt, previous)
		independent := *(*tailCaseInput.ready)
		independent.DependsOn = nil
		(*tailCaseInput.d).
			loader.Index.Records[(*tailCaseInput.ready).Named()] = manifest.DiscoveryRecord{Declaration: &independent}
		assert.Equal(t, bindEntryForTest(t, (*tailCaseInput.d), tailCaseInput.root), (*tailCaseInput.owner))
		assert.Equal(t, (*tailCaseInput.d).prepared[(*tailCaseInput.owner).Named()].receipt, previous)
		assert.FatalIf(
			t,
			len((*tailCaseInput.ready).DependsOn) != 1,
			"%v",
			"eligibility mutated the borrowed prerequisite",
		)
		projected := (*tailCaseInput.owner).Clone()
		projected.DependsOn = []kustomizev1.DependencyReference{{Name: "${NEXT}", Namespace: "${NS}"}}
		parent := &manifest.ResourceSet{Name: "updates", Namespace: "apps"}
		(*tailCaseInput.st).AddObject(parent)
		(*tailCaseInput.st).RecordEmitted(parent.Named(), &store.Emission{
			Children: []store.Child{{Obj: projected, Doc: testutil.ObjectDocument(projected)}}, Complete: true,
		}, nil)
		projected = projected.Clone()
		projected.DependsOn = []kustomizev1.DependencyReference{{Name: "sibling", Namespace: "apps"}}
		(*tailCaseInput.st).RecordEmitted(parent.Named(), &store.Emission{
			Children: []store.Child{{Obj: projected, Doc: testutil.ObjectDocument(projected)}}, Complete: true,
		}, nil)
		assert.FatalIf(
			t,
			!bindEntryForTest(t, (*tailCaseInput.d), tailCaseInput.root).Named().ReportHidden(),
			"%v",
			"new owner/snapshot retained the independent decision",
		)
		projected = projected.Clone()
		projected.DependsOn[0].Name = "ready"
		(*tailCaseInput.st).RecordEmitted(parent.Named(), &store.Emission{
			Children: []store.Child{{Obj: projected, Doc: testutil.ObjectDocument(projected)}}, Complete: true,
		}, nil)
		assert.Equal(t, bindEntryForTest(t, (*tailCaseInput.d), tailCaseInput.root), projected)
		before := (*tailCaseInput.d).prepared[projected.Named()]
		replacement := *before.source
		replacement.Key = "new-source"
		replacement.Content = "new-content"
		(*tailCaseInput.st).
			SetArtifact(manifest.BootstrapSourceID, &replacement)
		assert.Equal(t, bindEntryForTest(t, (*tailCaseInput.d), tailCaseInput.root), projected)
		assert.Equal(t, (*tailCaseInput.d).prepared[projected.Named()].source, &replacement)
	}
}
