package loader

import (
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"testing"
	"testing/fstest"

	helmv2 "github.com/fluxcd/helm-controller/api/v2"
	kustomizev1 "github.com/fluxcd/kustomize-controller/api/v1"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/inputprojection"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/change"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

func optionalSupplierPair(
	tb testing.TB,
	namespace string,
) (*change.Set, [2]*store.Store, [2]string, *manifest.GitRepository) {
	tb.Helper()
	var stores [2]*store.Store
	var roots [2]string
	var indexes [2]*manifest.DiscoveryIndex
	source := &manifest.GitRepository{Name: "supplier", Namespace: "infra"}
	for side := range stores {
		root := tb.TempDir()
		roots[side] = root
		st := store.New()
		stores[side] = st
		st.AddObject(source)
		st.SetArtifact(
			manifest.BootstrapSourceID,
			&store.SourceArtifact{LocalPath: root, LocalRoot: root, FS: fstest.MapFS{
				"flux/consumer.yaml":      {Data: []byte("unchanged consumer")},
				"apps/kustomization.yaml": {Data: []byte("resources: [output.yaml]\n")},
				"apps/output.yaml":        {Data: []byte("unchanged output")},
			}},
		)
		files := fstest.MapFS{"inputs/kustomization.yaml": {Data: []byte("resources: []\n")}}
		if side == 0 {
			files["inputs/kustomization.yaml"].Data = []byte("resources: [optional.yaml]\n")
			files["inputs/optional.yaml"] = &fstest.MapFile{
				Data: fmt.Appendf(
					nil,
					"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: optional, namespace: %q}\ndata: {VALUE: available}\n",
					namespace,
				),
			}
		}
		st.SetArtifact(
			source.Named(),
			&store.SourceArtifact{
				LocalPath: filepath.Join(root, ".supplier"),
				LocalRoot: root,
				Key:       strconv.Itoa(side),
				Content:   strconv.Itoa(side),
				FS:        files,
			},
		)
		observation := manifest.InputVisibility{Source: source.Named(), Root: "inputs", Complete: true}
		if side == 0 {
			observation.Inputs = map[manifest.NamedResource][]string{
				{Kind: manifest.KindConfigMap, Namespace: namespace, Name: "optional"}: {"inputs/optional.yaml"},
			}
		}
		indexes[side] = &manifest.DiscoveryIndex{Visibility: []manifest.InputVisibility{observation}}
	}
	changes, err := change.Qualify(tb.Context(), nil, stores[0], stores[1])
	if err != nil {
		tb.Fatal(err)
	}
	changes, err = changes.WithReaders(
		tb.Context(),
		indexes[0],
		indexes[1],
		[2]map[manifest.NamedResource]inputprojection.Projection{},
	)
	if err != nil {
		tb.Fatal(err)
	}
	return changes, stores, roots, source
}

func TestFilter_CrossSourceOptionalInputPairedSides(t *testing.T) {
	t.Parallel()
	for _, mode := range []string{"exact_removed", "deferred_consumer", "wrong_namespace", "unrelated_blank_namespace", "artifact_replaced", "declaration_replaced", "stale_bootstrap_alternative", "foreign_duplicate"} {
		t.Run(mode, func(t *testing.T) {
			t.Parallel()
			testFilterCrossSourceOptionalInputPairedSidesCase(
				t,
				&mode,
			)
		},
		)
	}
}

func TestFilter_ChangedInputUsesDeclarationSource(t *testing.T) {
	t.Parallel()
	for _, foreign := range []bool{false, true} {
		t.Run(strconv.FormatBool(foreign), func(t *testing.T) {
			t.Parallel()
			testFilterChangedInputUsesDeclarationSourceCase(
				t,
				&foreign,
			)
		},
		)
	}
}

func TestFilter_DeferredSourceValuesPairedSides(t *testing.T) {
	t.Parallel()
	for _, state := range []string{"deferred", "present", "unchanged_collision"} {
		t.Run(state, func(t *testing.T) {
			t.Parallel()
			testFilterDeferredSourceValuesPairedSidesCase(
				t,
				&state,
			)
		},
		)
	}
}

func TestFilter_ReadSourceAlternatives(t *testing.T) {
	t.Parallel()
	for _, mode := range []string{"changed_first", "changed_last", "read_only_collision", "structural_collision"} {
		t.Run(mode, func(t *testing.T) {
			t.Parallel()
			testFilterReadSourceAlternativesCase(t, &mode)
		},
		)
	}
}

func TestSet_WithReadersSelectedSource(t *testing.T) {
	t.Parallel()
	changes, stores, roots, source := optionalSupplierPair(t, "apps")
	reader := testutil.HelmReleaseID("apps", "reader")
	foreign := testutil.HelmReleaseID("apps", "foreign")
	opposite := testutil.HelmReleaseID("apps", "opposite")
	var indexes [2]*manifest.DiscoveryIndex
	for side := range indexes {
		indexes[side] = &manifest.DiscoveryIndex{
			Records: map[manifest.NamedResource]manifest.DiscoveryRecord{},
			Readers: map[string][]manifest.FileReader{},
		}
		for _, id := range []manifest.NamedResource{reader, foreign, opposite} {
			indexes[side].Records[id] = manifest.DiscoveryRecord{
				File: "flux/" + id.Name + ".yaml",
				Refs: []manifest.NamedResource{source.Named()},
			}
		}
	}
	file := ".supplier/inputs/optional.yaml"
	indexes[0].Readers[file] = []manifest.FileReader{
		{Reader: reader, Source: source.Named(), Exact: true},
		{Reader: foreign, Source: manifest.BootstrapSourceID, Exact: true},
	}
	indexes[1].Readers[file] = []manifest.FileReader{{Reader: opposite, Source: source.Named(), Exact: true}}
	original := maps.Clone(inputMap(sourceChanges(changes, source.Named())))
	resolved, err := changes.WithReaders(
		t.Context(),
		indexes[0],
		indexes[1],
		[2]map[manifest.NamedResource]inputprojection.Projection{},
	)
	assert.NoError(t, err)
	assert.Equal(t, len(inputMap(resolved)), 0)
	assert.Equal(t, inputMap(sourceChanges(resolved, source.Named()))[reader], "inputs/optional.yaml")
	assert.Equal(t, inputMap(sourceChanges(resolved, source.Named()))[foreign], "")
	assert.Equal(t, inputMap(sourceChanges(resolved, source.Named()))[opposite], "")
	assert.Diff(t, inputMap(sourceChanges(changes, source.Named())), original)
	for side, st := range stores {
		filter := newAdmission(
			t,
			admissionConfig{changes: resolved, index: indexes[side], inventory: st, root: roots[side]},
		)
		assert.Equal(t, filter.ShouldReconcile(reader), true)
		assert.Equal(t, filter.ShouldReconcile(foreign), false)
		assert.Equal(t, filter.ShouldReconcile(opposite), side == 1)
	}
}

func testFilterCrossSourceOptionalInputPairedSidesCase(
	t *testing.T,
	testCaseInput *string,
) {
	t.Helper()

	namespace := "apps"
	if (*testCaseInput) == "wrong_namespace" {
		namespace = "other"
	}
	if (*testCaseInput) == "unrelated_blank_namespace" {
		namespace = ""
	}
	changes, stores, roots, source := optionalSupplierPair(t, namespace)
	input := manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: namespace, Name: "optional"}
	if (*testCaseInput) == "foreign_duplicate" {
		foreign := &manifest.GitRepository{Name: "foreign", Namespace: "infra"}
		var indexes [2]*manifest.DiscoveryIndex
		for side, st := range stores {
			st.AddObject(foreign)
			st.SetArtifact(
				foreign.Named(),
				&store.SourceArtifact{
					LocalPath: filepath.Join(roots[side], ".foreign"),
					Content:   "same",
					Key:       "same",
					FS:        fstest.MapFS{"inputs/optional.yaml": {Data: []byte("same")}},
				},
			)
			observation := manifest.InputVisibility{Source: source.Named(), Root: "inputs", Complete: true}
			if side == 0 {
				observation.Inputs = map[manifest.NamedResource][]string{input: {"inputs/optional.yaml"}}
			}
			indexes[side] = &manifest.DiscoveryIndex{
				Visibility: []manifest.InputVisibility{
					observation,
					{
						Source:   foreign.Named(),
						Root:     "inputs",
						Complete: true,
						Inputs:   map[manifest.NamedResource][]string{input: {"inputs/optional.yaml"}},
					},
				},
			}
		}
		var err error
		changes, err = change.Qualify(t.Context(), changes, stores[0], stores[1])
		assert.NoError(t, err)
		changes, err = changes.WithReaders(
			t.Context(),
			indexes[0],
			indexes[1],
			[2]map[manifest.NamedResource]inputprojection.Projection{},
		)
		assert.NoError(t, err)
	}
	assert.Equal(t, inputMap(sourceChanges(changes, source.Named()))[input], "inputs/optional.yaml")
	assert.Equal(t, len(inputMap(changes)), 0)
	assert.Equal(t, changes.Len(), 0)
	assert.Diff(
		t,
		sourceChanges(changes, source.Named()).Paths(),
		[]string{"inputs/kustomization.yaml", "inputs/optional.yaml"},
	)
	consumer := &manifest.Kustomization{
		Name:      "consumer",
		Namespace: "apps",
		Path:      "apps",
		PostBuild: &kustomizev1.PostBuild{
			SubstituteFrom: []manifest.SubstituteReference{
				{Kind: manifest.KindConfigMap, Name: "optional", Optional: true},
			},
		},
	}
	sibling := consumer.Clone()
	sibling.Name = "sibling"
	sibling.PostBuild = &kustomizev1.PostBuild{
		SubstituteFrom: []manifest.SubstituteReference{
			{Kind: manifest.KindConfigMap, Name: "unrelated", Optional: true},
		},
	}
	if (*testCaseInput) == "stale_bootstrap_alternative" {
		var original [2]store.Artifact
		for side, st := range stores {
			original[side] = st.GetArtifact(manifest.BootstrapSourceID, store.ArtifactContent)
			st.SetArtifact(
				manifest.BootstrapSourceID,
				st.GetArtifact(source.Named(), store.ArtifactContent).(*store.SourceArtifact),
			)
		}
		var err error
		changes, err = change.Qualify(t.Context(), nil, stores[0], stores[1])
		assert.NoError(t, err)
		before := manifest.InputVisibility{
			Source:   source.Named(),
			Root:     "inputs",
			Complete: true,
			Inputs:   map[manifest.NamedResource][]string{input: {"inputs/optional.yaml"}},
		}
		after := before
		after.Inputs = nil
		changes, err = changes.WithReaders(
			t.Context(),
			&manifest.DiscoveryIndex{Visibility: []manifest.InputVisibility{before}},
			&manifest.DiscoveryIndex{Visibility: []manifest.InputVisibility{after}},
			[2]map[manifest.NamedResource]inputprojection.Projection{},
		)
		assert.NoError(t, err)
		for side, st := range stores {
			st.SetArtifact(manifest.BootstrapSourceID, original[side].(*store.SourceArtifact))
		}
	}
	testFilterCrossSourceOptionalInputPairedSidesCaseCompletion(
		t,
		&testFilterCrossSourceOptionalInputPairedSidesCaseCompletionState{
			testCaseInput: testCaseInput,
			changes:       changes,
			stores:        &stores,
			roots:         &roots,
			source:        source,
			consumer:      consumer,
			sibling:       sibling,
		},
	)
}

func testFilterChangedInputUsesDeclarationSourceCase(
	t *testing.T,
	testCaseInput *bool,
) {
	t.Helper()

	var stores [2]*store.Store
	var roots [2]string
	consumer := &manifest.HelmRelease{
		Name:      "consumer",
		Namespace: "apps",
		Chart: manifest.HelmChart{
			Source: manifest.NamedResource{
				Group:     manifest.SourceDomain,
				Kind:      manifest.KindOCIRepository,
				Name:      "chart",
				Namespace: "apps",
			},
		},
		ChartRef: &helmv2.CrossNamespaceSourceReference{
			Kind:      manifest.KindOCIRepository,
			Name:      "chart",
			Namespace: "apps",
		},
		ValuesFrom: []helmv2.ValuesReference{{Kind: manifest.KindConfigMap, Name: "settings"}},
	}
	var indexes [2]*manifest.DiscoveryIndex
	for side := range stores {
		root := t.TempDir()
		roots[side] = root
		st := store.New()
		stores[side] = st
		st.AddObject(consumer)
		files := fstest.MapFS{"consumer.yaml": {Data: []byte("unchanged consumer")}}
		if side == 1 {
			files["input.yaml"] = &fstest.MapFile{
				Data: []byte(
					"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: settings}\ndata: {value: changed}\n",
				),
			}
		}
		st.SetArtifact(
			manifest.BootstrapSourceID,
			&store.SourceArtifact{LocalPath: root, LocalRoot: root, FS: files},
		)
		origin := manifest.BootstrapSourceID
		if *testCaseInput {
			source := &manifest.GitRepository{
				Name:      "foreign",
				Namespace: "infra",
				URL:       "https://fixture.invalid/foreign",
			}
			st.AddObject(source)
			origin = source.Named()
			st.SetArtifact(
				origin,
				&store.SourceArtifact{
					LocalPath: t.TempDir(),
					Content:   "foreign",
					Key:       "foreign",
					FS:        fstest.MapFS{},
				},
			)
		}
		indexes[side] = &manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
			consumer.Named(): {
				File: "consumer.yaml",
				Productions: []manifest.Production{
					{Authored: consumer.Named(), Source: origin, File: "consumer.yaml"},
				},
			},
		}}
		observation := manifest.InputVisibility{Source: manifest.BootstrapSourceID, Root: ".", Complete: true}
		if side == 1 {
			observation.Inputs = map[manifest.NamedResource][]string{
				{Kind: manifest.KindConfigMap, Name: "settings"}: {"input.yaml"},
			}
		}
		indexes[side].Visibility = []manifest.InputVisibility{observation}
	}
	changes, err := change.Qualify(t.Context(), nil, stores[0], stores[1])
	assert.NoError(t, err)
	changes, err = changes.WithReaders(
		t.Context(),
		indexes[0],
		indexes[1],
		[2]map[manifest.NamedResource]inputprojection.Projection{},
	)
	assert.NoError(t, err)
	for side, st := range stores {
		filter := newAdmission(
			t,
			admissionConfig{changes: changes, index: indexes[side], inventory: st, root: roots[side]},
		)
		assert.Equal(t, filter.ShouldReconcile(consumer.Named()), !(*testCaseInput))
	}
}

func testFilterDeferredSourceValuesPairedSidesCase(
	t *testing.T,
	testCaseInput *string,
) {
	t.Helper()

	var stores [2]*store.Store
	var indexes [2]*manifest.DiscoveryIndex
	var roots [2]string
	source := &manifest.GitRepository{Name: "values", Namespace: "apps"}
	parent := testutil.KustomizationID("flux-system", "parent")
	release := testutil.HelmReleaseID("apps", "demo")
	sibling := testutil.HelmReleaseID("apps", "sibling")
	for side := range stores {
		root := t.TempDir()
		roots[side] = root
		testutil.WriteFile(
			t,
			root,
			"flux/parent.yaml",
			testutil.KustomizationYAML("parent", "flux-system", "  path: ./apps\n  targetNamespace: apps\n"),
		)
		testutil.WriteFile(t, root, "apps/kustomization.yaml", "resources: [hr.yaml, sibling.yaml]\n")
		testutil.WriteFile(t, root, "apps/hr.yaml", `apiVersion: helm.toolkit.fluxcd.io/v2
kind: HelmRelease
metadata: {name: demo, namespace: apps}
spec:
  chart:
    spec:
      chart: charts/demo
      sourceRef: {kind: GitRepository, name: values}
      valuesFiles: [config/prod.yaml]
`)
		testutil.WriteFile(
			t,
			root,
			"apps/sibling.yaml",
			"apiVersion: helm.toolkit.fluxcd.io/v2\nkind: HelmRelease\nmetadata: {name: sibling, namespace: apps}\nspec: {chartRef: {kind: OCIRepository, name: unrelated}}\n",
		)
		st := store.New()
		stores[side] = st
		st.AddObject(source)
		st.SetArtifact(
			manifest.BootstrapSourceID,
			&store.SourceArtifact{LocalPath: root, LocalRoot: root, FS: os.DirFS(root)},
		)
		value := side
		if (*testCaseInput) == "unchanged_collision" {
			value = 0
		}
		st.SetArtifact(
			source.Named(),
			&store.SourceArtifact{
				LocalPath: filepath.Join(root, ".selected"),
				LocalRoot: root,
				Key:       strconv.Itoa(side),
				Content:   strconv.Itoa(side),
				FS:        fstest.MapFS{"config/prod.yaml": {Data: fmt.Appendf(nil, "value: %d\n", value)}},
			},
		)
		l := New(st)
		l.SourceRoot = root
		_, err := l.Load(t.Context(), root)
		assert.NoError(t, err)
		assert.NoError(t, l.FinalizeIndex(t.Context(), root, true))
		indexes[side] = l.Index
		r, ok := l.Index.Lookup(release)
		assert.FatalIf(t, !ok || r.Parent != parent ||
			!slices.ContainsFunc(
				r.Productions,
				func(p manifest.Production) bool { return p.Owner == parent },
			) ||
			st.GetObject(
				release,
			) != nil, "SETUP side %d: release must be indexed with its contributing parent and absent from Store: %+v", side, r)
		if (*testCaseInput) == "present" {
			st.AddObject(
				&manifest.HelmRelease{
					Name:             release.Name,
					Namespace:        release.Namespace,
					Chart:            manifest.HelmChart{Source: source.Named()},
					ChartValuesFiles: []string{"config/prod.yaml"},
					HelmReleaseSpec: helmv2.HelmReleaseSpec{
						Chart: &helmv2.HelmChartTemplate{
							Spec: helmv2.HelmChartTemplateSpec{
								Chart:   "chart",
								Version: "",
								SourceRef: &helmv2.CrossNamespaceObjectReference{
									Kind:       source.Named().Kind,
									Name:       source.Named().Name,
									Namespace:  source.Named().Namespace,
									APIVersion: "",
								},
								ValuesFiles: []string{"config/prod.yaml"},
							},
						},
					},
				},
			)
		}
	}
	bootstrap := change.NewSet(nil)
	if (*testCaseInput) == "unchanged_collision" {
		bootstrap = change.NewSet([]string{".selected/config/prod.yaml"})
	}
	changes, err := change.Qualify(t.Context(), bootstrap, stores[0], stores[1])
	assert.NoError(t, err)
	want := (*testCaseInput) != "unchanged_collision"
	assert.Equal(t, sourceChanges(changes, source.Named()).Contains("config/prod.yaml"), want)
	assert.Equal(t, changes.Len(), bootstrap.Len())
	for side, st := range stores {
		filter := newAdmission(
			t,
			admissionConfig{changes: changes, index: indexes[side], inventory: st, root: roots[side]},
		)
		for _, id := range []manifest.NamedResource{release, parent} {
			{
				got := filter.ShouldReconcile(id)
				assert.ErrorIf(
					t,
					got != want,
					"SELECTION side %d: %s selected=%t, want %t",
					side,
					id,
					got,
					want,
				)
			}
		}
		assert.Equal(t, filter.ShouldReconcile(sibling), false)
	}
}

func testFilterReadSourceAlternativesCase(t *testing.T, testCaseInput *string,
) {
	t.Helper()

	reader := &manifest.Kustomization{Name: "reader", Namespace: "apps", Path: ".selected"}
	var stores [2]*store.Store
	var indexes [2]*manifest.DiscoveryIndex
	var roots [2]string
	for side := range stores {
		root := t.TempDir()
		roots[side] = root
		st := store.New()
		stores[side] = st
		st.SetArtifact(manifest.BootstrapSourceID, &store.SourceArtifact{LocalPath: root, FS: os.DirFS(root)})
		reads := make([]manifest.FileReader, 0, 2)
		for _, name := range []string{"unchanged", "changed"} {
			source := &manifest.GitRepository{Name: name, Namespace: "apps"}
			value := 0
			if name == "changed" &&
				((*testCaseInput) == "changed_first" || (*testCaseInput) == "changed_last") {
				value = side
			}
			st.AddObject(source)
			st.SetArtifact(
				source.Named(),
				&store.SourceArtifact{
					LocalPath: filepath.Join(root, ".selected"),
					LocalRoot: root,
					Key:       fmt.Sprintf("%s-%d", name, side),
					Content:   fmt.Sprintf("%s-%d", name, side),
					FS:        fstest.MapFS{"input.yaml": {Data: fmt.Appendf(nil, "value: %d\n", value)}},
				},
			)
			reads = append(reads, manifest.FileReader{Reader: reader.Named(), Source: source.Named()})
		}
		if (*testCaseInput) == "changed_first" {
			slices.Reverse(reads)
		}
		index := &manifest.DiscoveryIndex{
			Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
				reader.Named(): {File: "flux/reader.yaml"},
			},
			Readers: map[string][]manifest.FileReader{".selected/input.yaml": reads},
		}
		indexes[side] = index
		owners := index.OwnersOfFile(".selected/input.yaml").Owners
		assert.Diff(t, owners, []manifest.NamedResource{reader.Named()})
		if (*testCaseInput) == "structural_collision" {
			st.AddObject(reader)
			index.Claims = map[string][]manifest.NamedResource{".selected/": {reader.Named()}}
		}
	}
	bootstrap := change.NewSet(nil)
	if (*testCaseInput) == "read_only_collision" || (*testCaseInput) == "structural_collision" {
		bootstrap = change.NewSet([]string{".selected/input.yaml"})
	}
	changes, err := change.Qualify(t.Context(), bootstrap, stores[0], stores[1])
	assert.NoError(t, err)
	for side, st := range stores {
		filter := newAdmission(
			t,
			admissionConfig{changes: changes, index: indexes[side], inventory: st, root: roots[side]},
		)
		assert.Equal(t, filter.ShouldReconcile(reader.Named()), (*testCaseInput) != "read_only_collision")
	}
}

type testFilterCrossSourceOptionalInputPairedSidesCaseCompletionState struct {
	testCaseInput *string
	changes       *change.Set
	stores        *[2]*store.Store
	roots         *[2]string
	source        *manifest.GitRepository
	consumer      *manifest.Kustomization
	sibling       *manifest.Kustomization
}

func testFilterCrossSourceOptionalInputPairedSidesCaseCompletion(
	t *testing.T,
	scope *testFilterCrossSourceOptionalInputPairedSidesCaseCompletionState,
) {
	t.Helper()
	for side, st := range *scope.stores {
		index := &manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
			scope.consumer.Named(): {
				File: "flux/consumer.yaml",
				Refs: []manifest.NamedResource{
					{Kind: manifest.KindConfigMap, Namespace: "apps", Name: "optional"},
				},
			},
			scope.sibling.Named(): {File: "flux/sibling.yaml"},
		}}
		if (*scope.testCaseInput) != "deferred_consumer" {
			st.AddObject(scope.consumer)
		}
		st.AddObject(scope.sibling)
		if (*scope.testCaseInput) == "artifact_replaced" {
			art := *st.GetArtifact(scope.source.Named(), store.ArtifactContent).(*store.SourceArtifact)
			art.Content, art.Revision = "replacement", "replacement"
			st.SetArtifact(scope.source.Named(), &art)
		}
		if (*scope.testCaseInput) == "declaration_replaced" {
			replacement := *scope.source
			replacement.URL = "https://example.test/replaced"
			st.AddObject(&replacement)
		}
		filter := newAdmission(
			t,
			admissionConfig{changes: scope.changes, index: index, inventory: st, root: scope.roots[side]},
		)
		want := (*scope.testCaseInput) == "exact_removed" || (*scope.testCaseInput) == "deferred_consumer" ||
			(*scope.testCaseInput) ==
				"stale_bootstrap_alternative" ||
			(*scope.testCaseInput) ==
				"foreign_duplicate"
		if got := filter.ShouldReconcile(scope.consumer.Named()); got != want {
			t.Errorf("side %d consumer selected=%t, want %t: %v", side, got, want, filter.KeepNames())
		}
		assert.Equal(t, filter.ShouldReconcile(scope.sibling.Named()), false)
	}
}
