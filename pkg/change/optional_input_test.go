package change_test

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing/object"

	"github.com/home-operations/flate/internal/admissiontest"
	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/inputprojection"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/pkg/baseline"
	"github.com/home-operations/flate/pkg/change"
	"github.com/home-operations/flate/pkg/discovery"
	"github.com/home-operations/flate/pkg/loader"
	"github.com/home-operations/flate/pkg/manifest"
	sourcecache "github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/store"
)

func TestOptionalInput_CrossSourcePreparedStores(t *testing.T) {
	t.Parallel()
	roots := testutil.CrossSourceOptionalInputs(t)
	var stores [2]*store.Store
	var indexes [2]*manifest.DiscoveryIndex
	var selected [2]*store.SourceArtifact
	supplier := (&manifest.GitRepository{Name: "supplier", Namespace: "flux-system"}).Named()
	for side, root := range roots {
		stores[side] = store.New()
		result, err := discovery.Run(
			t.Context(),
			discovery.Config{
				Path:      filepath.Join(root, "flux"),
				Store:     stores[side],
				Selection: manifest.Selection{Changed: true},
			},
		)
		assert.NoError(t, err)
		indexes[side] = result.Index
		selected[side], _ = stores[side].GetArtifact(supplier, store.ArtifactContent).(*store.SourceArtifact)

		{
			input := selected[side]
			assert.FatalIf(
				t,
				input == nil || input.FS == nil || input.LocalPath == root || input.InputAlias,
				"side %d did not prepare a distinct committed supplier: %+v",
				side,
				input,
			)
		}
	}
	bootstrap, err := change.Detect(
		t.Context(),
		stores[0].GetArtifact(manifest.BootstrapSourceID, store.ArtifactContent).(*store.SourceArtifact),
		stores[1].GetArtifact(manifest.BootstrapSourceID, store.ArtifactContent).(*store.SourceArtifact),
		manifest.WorkScope{},
	)
	assert.NoError(t, err)
	assert.Equal(t, bootstrap.Len(), 0)
	delta, err := change.Detect(t.Context(), selected[0], selected[1], manifest.WorkScope{})
	assert.NoError(t, err)
	assert.Diff(t, delta.Paths(), []string{"inputs/kustomization.yaml", "inputs/optional.yaml"})
	input := manifest.NamedResource{Kind: manifest.KindConfigMap, Name: "optional", Namespace: "flux-system"}
	probe := store.New()
	probe.SetArtifact(manifest.BootstrapSourceID, selected[0])
	observations := [2]*manifest.DiscoveryIndex{}
	for side, index := range indexes {
		observations[side] = &manifest.DiscoveryIndex{}
		for _, observation := range index.Visibility {
			if observation.Source == supplier {
				observation.Source = manifest.BootstrapSourceID
				observations[side].Visibility = append(observations[side].Visibility, observation)
			}
		}
	}
	delta, err = delta.WithReaders(
		t.Context(), [2]*manifest.DiscoveryIndex{
			observations[0],
			observations[1],
		},
		[2]map[manifest.NamedResource]inputprojection.Projection{}, manifest.WorkScope{},
	)
	assert.NoError(t, err)
	assert.Equal(
		t,
		admissiontest.Filter(t, admissiontest.Config{Changes: delta, Index: nil, Inventory: probe, Root: selected[0].LocalPath}).
			ShouldReconcile(input),
		true,
	)
	assert.Equal(
		t,
		admissiontest.Filter(t, admissiontest.Config{Changes: bootstrap, Index: nil, Inventory: probe, Root: selected[0].LocalPath}).
			ShouldReconcile(input),
		false,
	)
	changes, err := change.Qualify(t.Context(), bootstrap, [2]*store.Store{stores[0], stores[1]}, manifest.WorkScope{})
	assert.NoError(t, err)
	changes, err = changes.WithReaders(
		t.Context(), [2]*manifest.DiscoveryIndex{
			indexes[0],
			indexes[1],
		},
		[2]map[manifest.NamedResource]inputprojection.Projection{}, manifest.WorkScope{},
	)
	assert.NoError(t, err)
	for side, st := range stores {
		filter := admissiontest.Filter(
			t,
			admissiontest.Config{Changes: changes, Index: indexes[side], Inventory: st, Root: roots[side]},
		)
		assert.Equal(t, filter.ShouldReconcile(testutil.KustomizationID("flux-system", "app")), true)
	}
}

func TestOptionalInput_AppearanceRoutes(t *testing.T) {
	t.Parallel()
	for _, route := range []string{"directory", "committed"} {
		t.Run(route, func(t *testing.T) {
			t.Parallel()
			testOptionalInputAppearanceRoutesCase(t, &route)
		},
		)
	}
}

func writeOptionalInputCluster(t *testing.T, root, kind string) {
	t.Helper()
	testutil.WriteFile(t, root, "flux/cluster.yaml", `apiVersion: source.toolkit.fluxcd.io/v1
kind: GitRepository
metadata: {name: cluster, namespace: flux-system}
spec: {url: https://example.test/cluster.git}
---
apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata: {name: app, namespace: flux-system}
spec:
  path: ./apps/app
  sourceRef: {kind: GitRepository, name: cluster}
  postBuild:
    substituteFrom: [{kind: `+kind+`, name: optional, optional: true}]
`)
	testutil.WriteFile(
		t,
		root,
		"flux/stable.yaml",
		testutil.KustomizationYAML(
			"stable",
			"flux-system",
			"  path: ./apps/stable\n  sourceRef: {kind: GitRepository, name: cluster}\n  dependsOn: [{name: app}]\n",
		),
	)
	testutil.WriteFile(t, root, "apps/app/kustomization.yaml", "resources: [output.yaml]\n")
	testutil.WriteFile(t, root, "apps/app/output.yaml", `apiVersion: v1
kind: ConfigMap
metadata: {name: rendered, namespace: flux-system}
data: {value: "${VALUE:-fallback}"}
`)
}

func TestFilter_OptionalInputChanges(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name, kind, beforeName, namespace, operation string
		want                                         bool
	}{
		{"configmap appears", "ConfigMap", "optional", "flux-system", "appear", true},
		{"secret appears", "Secret", "optional", "flux-system", "appear", true},
		{"configmap edits", "ConfigMap", "optional", "flux-system", "edit", true},
		{"secret edits", "Secret", "optional", "flux-system", "edit", true},
		{"configmap removes", "ConfigMap", "optional", "flux-system", "remove", true},
		{"secret removes", "Secret", "optional", "flux-system", "remove", true},
		{"unrelated removal", "ConfigMap", "unrelated", "flux-system", "remove", false},
		{"wrong namespace removal", "ConfigMap", "optional", "other", "remove", false},
		{"empty namespace removal", "ConfigMap", "optional", "", "remove", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			testFilterOptionalInputChangesCase(t, &tc)
		},
		)
	}
}

func TestOptionalInput_PairedSides(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name, inputName      string
		projected, committed bool
	}{
		{"optional", "optional", false, false},
		{"unrelated", "unrelated", false, false},
		{"projected namespace", "optional", true, false},
		{"committed optional", "optional", false, true},
		{"committed unrelated", "unrelated", false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			testOptionalInputPairedSidesCase(t, &tc)
		},
		)
	}
}

func TestFilter_OrderingOnlySiblingExcluded(t *testing.T) {
	t.Parallel()
	before, after := t.TempDir(), t.TempDir()
	for _, root := range []string{before, after} {
		writeOptionalInputCluster(t, root, manifest.KindConfigMap)
		body, err := fs.ReadFile(os.DirFS(root), "flux/cluster.yaml")
		assert.NoError(t, err)
		testutil.WriteFile(t, root, "flux/cluster.yaml", string(body)+"  dependsOn: [{name: sibling}]\n")
		testutil.WriteFile(
			t,
			root,
			"flux/sibling.yaml",
			testutil.KustomizationYAML(
				"sibling",
				"flux-system",
				"  path: ./apps/sibling\n  sourceRef: {kind: GitRepository, name: cluster}\n",
			),
		)
		testutil.WriteFile(t, root, "apps/sibling/kustomization.yaml", "resources: []\n")
	}
	testutil.WriteFile(
		t,
		after,
		"flux/sibling.yaml",
		testutil.KustomizationYAML(
			"sibling",
			"flux-system",
			"  path: ./apps/sibling\n  prune: true\n  sourceRef: {kind: GitRepository, name: cluster}\n",
		),
	)
	inputs, err := baseline.ResolvePaths(t.Context(), after, before)
	assert.NoError(t, err)
	changes, err := change.Detect(t.Context(), inputs.BaseInput, inputs.Source, manifest.WorkScope{})
	assert.NoError(t, err)
	filters, _ := optionalInputFilters(t, [2]*store.SourceArtifact{inputs.BaseInput, inputs.Source}, changes)
	f := filters[1]
	assert.Equal(t, f.ShouldReconcile(testutil.KustomizationID("flux-system", "sibling")), true)
	assert.Equal(
		t,
		f.ShouldReconcile(
			manifest.NamedResource{
				Group:     manifest.SourceDomain,
				Kind:      manifest.KindGitRepository,
				Namespace: "flux-system",
				Name:      "cluster",
			},
		),
		true,
	)
	assert.Equal(t, f.ShouldReconcile(testutil.KustomizationID("flux-system", "app")), false)
}

func optionalInputFilters(
	t *testing.T,
	inputs [2]*store.SourceArtifact,
	changes *change.Set,
) ([2]*change.Filter, [2]*loader.Loader) {
	t.Helper()
	var stores [2]*store.Store
	var indexes [2]*manifest.DiscoveryIndex
	var filters [2]*change.Filter
	var loaders [2]*loader.Loader
	for side, input := range inputs {
		st := store.New()
		stores[side] = st
		result, err := discovery.Run(
			t.Context(),
			discovery.Config{
				Input:     input,
				Store:     st,
				SelfURLs:  []string{"https://example.test/cluster.git"},
				Selection: manifest.Selection{Changed: true},
			},
		)
		assert.NoError(t, err)
		indexes[side] = result.Index
	}
	var err error
	changes, err = changes.WithReaders(
		t.Context(), [2]*manifest.DiscoveryIndex{
			indexes[0],
			indexes[1],
		},
		[2]map[manifest.NamedResource]inputprojection.Projection{}, manifest.WorkScope{},
	)
	assert.NoError(t, err)
	for side, input := range inputs {
		l := admissiontest.New(
			t,
			admissiontest.Config{
				Changes:   changes,
				Index:     indexes[side],
				Inventory: stores[side],
				Root:      input.LocalPath,
			},
		)
		admissiontest.Commit(t, l, nil)
		filters[side], loaders[side] = l.Options.Filter, l
	}
	return filters, loaders
}

func testOptionalInputAppearanceRoutesCase(t *testing.T, testCaseInput *string,
) {
	t.Helper()

	root := t.TempDir()
	writeOptionalInputCluster(t, root, manifest.KindConfigMap)
	repo := testutil.GitInit(t, root)
	options := &git.CommitOptions{Author: &object.Signature{
		Name: "test", Email: "test@example.test", When: time.Unix(0, 0),
	}}
	base := testutil.GitCommitAll(t, repo, "base", options)
	testutil.WriteFile(t, root, "flux/optional.yaml", `apiVersion: v1
kind: ConfigMap
metadata: {name: optional, namespace: flux-system}
data: {VALUE: available}
`)
	var inputs *baseline.Result
	var err error
	if (*testCaseInput) == "directory" {
		before := t.TempDir()
		writeOptionalInputCluster(t, before, manifest.KindConfigMap)
		inputs, err = baseline.ResolvePaths(t.Context(), root, before)
	} else {
		testutil.GitCommitAll(t, repo, "optional input appears", options)
		cache := cachetest.New(t, t.TempDir(), sourcecache.New)
		inputs, err = baseline.AutoResolve(t.Context(), root, base.String(), cache)
	}
	assert.NoError(t, err)
	{
		_, localErr := fs.ReadFile(
			inputs.BaseInput.FS,
			"flux/optional.yaml",
		)
		assert.FatalIf(t, !errors.Is(
			localErr,
			fs.ErrNotExist,
		), "baseline optional input: got %v, want not found", localErr)
	}
	changes := inputs.Changes
	if (*testCaseInput) == "directory" {
		changes, err = change.Detect(t.Context(), inputs.BaseInput, inputs.Source, manifest.WorkScope{})
		assert.NoError(t, err)
	}

	assert.FatalIf(t, changes == nil, "%v", "resolver returned no change set")

	assert.Diff(t, changes.Paths(), []string{"flux/optional.yaml"})

	st := store.New()
	discovered, err := discovery.Run(t.Context(), discovery.Config{
		Input: inputs.Source, Path: inputs.ScanPath, Store: st,
		SelfURLs: []string{"https://example.test/cluster.git"}, Selection: manifest.Selection{Changed: true},
	})
	assert.NoError(t, err)
	app := testutil.KustomizationID("flux-system", "app")
	cluster := manifest.NamedResource{
		Group: manifest.SourceDomain, Kind: manifest.KindGitRepository,
		Namespace: "flux-system", Name: "cluster",
	}
	input := manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "flux-system", Name: "optional"}

	assert.FatalIf(
		t,
		st.GetObject(app) == nil || st.GetObject(cluster) == nil,
		"%v",
		"discovery did not admit app and cluster",
	)

	record, ok := discovered.Index.Lookup(input)

	assert.FatalIf(
		t,
		!ok || record.File != "flux/optional.yaml",
		"optional input discovery record: %+v, found=%t",
		record,
		ok,
	)

	ownersOfFileResult := discovered.Index.OwnersOfFile(record.File)
	owners := ownersOfFileResult.Owners

	assert.FatalIf(
		t,
		slices.Contains(owners, app),
		"%v",
		"optional input is inside app's rendered ownership",
	)

	optional := false
	for ref := range manifest.References(st.GetObject(app), manifest.ReferenceRelations) {
		optional = optional || ref.NamedResource == input && ref.Use == "substituteFrom" && ref.Optional
	}

	assert.FatalIf(t, !optional, "%v", "app does not declare the optional substitution input")

	empty, err := change.Detect(t.Context(), inputs.Source, inputs.Source, manifest.WorkScope{})
	assert.NoError(t, err)
	unchanged := admissiontest.Filter(
		t,
		admissiontest.Config{Changes: empty, Index: discovered.Index, Inventory: st, Root: ""},
	)

	assert.FatalIf(
		t,
		unchanged.ShouldReconcile(app) || unchanged.ShouldReconcile(cluster),
		"%v",
		"unchanged input selects app or cluster",
	)

	filters, _ := optionalInputFilters(t, [2]*store.SourceArtifact{inputs.BaseInput, inputs.Source}, changes)
	for side, filter := range filters {
		assert.ErrorIf(
			t,
			side == 1 && !filter.ShouldReconcile(input),
			"%v",
			"changed optional input was not selected",
		)

		for _, id := range []manifest.NamedResource{app, cluster} {
			assert.ErrorIf(
				t,
				!filter.ShouldReconcile(id),
				"side %d optional input appearance did not select %s",
				side,
				id,
			)
		}
		assert.Equal(t, filter.ShouldReconcile(testutil.KustomizationID("flux-system", "stable")), false)
	}
}

func testFilterOptionalInputChangesCase(t *testing.T, testCaseInput *struct {
	name string

	kind       string
	beforeName string
	namespace  string
	operation  string
	want       bool
},
) {
	t.Helper()

	before, after := t.TempDir(), t.TempDir()
	writeOptionalInputCluster(t, before, testCaseInput.kind)
	writeOptionalInputCluster(t, after, testCaseInput.kind)
	body := fmt.Sprintf(
		"apiVersion: v1\nkind: %s\nmetadata: {name: %s, namespace: %q}\ndata: {VALUE: b2xk}\n",
		testCaseInput.kind,
		testCaseInput.beforeName,
		testCaseInput.namespace,
	)
	if testCaseInput.operation != "appear" {
		testutil.WriteFile(t, before, "flux/optional.yaml", body)
	}
	if testCaseInput.operation != "remove" {
		testutil.WriteFile(t, after, "flux/optional.yaml", body+"immutable: false\n")
	}
	inputs, err := baseline.ResolvePaths(t.Context(), after, before)
	assert.NoError(t, err)
	changes, err := change.Detect(t.Context(), inputs.BaseInput, inputs.Source, manifest.WorkScope{})
	assert.NoError(t, err)
	filters, loaders := optionalInputFilters(
		t,
		[2]*store.SourceArtifact{inputs.BaseInput, inputs.Source},
		changes,
	)
	f, l := filters[1], loaders[1]
	index := l.Index
	app := testutil.KustomizationID("flux-system", "app")
	cluster := manifest.NamedResource{
		Group:     manifest.SourceDomain,
		Kind:      manifest.KindGitRepository,
		Namespace: "flux-system",
		Name:      "cluster",
	}
	for _, id := range []manifest.NamedResource{app, cluster} {
		assert.Equal(t, f.ShouldReconcile(id), testCaseInput.want)
	}
	if testCaseInput.operation == "remove" {
		for _, name := range []string{"optional", "unrelated"} {
			_, present := index.Lookup(
				manifest.NamedResource{
					Kind:      testCaseInput.kind,
					Namespace: testCaseInput.namespace,
					Name:      name,
				},
			)
			assert.Equal(t, present, false)
		}
	}
	child := &manifest.Kustomization{Name: "emitted", Namespace: "flux-system"}
	proposal, err := l.Prepare(
		t.Context(),
		loader.PrepareRequest{
			Generation: []store.Child{{Obj: child, Productions: []manifest.Production{{Owner: app}}}},
		},
	)
	assert.NoError(t, err)
	admissiontest.Commit(t, l, proposal)
	assert.Equal(t, f.ShouldReconcile(child.Named()), testCaseInput.want)
}

func testOptionalInputPairedSidesCase(t *testing.T, testCaseInput *struct {
	name string

	inputName string
	projected bool
	committed bool
},
) {
	t.Helper()

	before, after := t.TempDir(), t.TempDir()
	inputPath, namespace := "flux/optional.yaml", "flux-system"
	for _, root := range []string{before, after} {
		writeOptionalInputCluster(t, root, manifest.KindConfigMap)
		testutil.WriteFile(
			t,
			root,
			"flux/other.yaml",
			testutil.KustomizationYAML(
				"other",
				"other",
				"  path: ./apps/other\n  sourceRef: {kind: GitRepository, name: cluster, namespace: flux-system}\n  postBuild:\n    substituteFrom: [{kind: ConfigMap, name: optional, optional: true}]\n",
			),
		)
		if testCaseInput.projected {
			testutil.WriteFile(
				t,
				root,
				"flux/inputs.yaml",
				testutil.KustomizationYAML(
					"inputs",
					"flux-system",
					"  path: ./inputs\n  targetNamespace: flux-system\n  sourceRef: {kind: GitRepository, name: cluster}\n",
				),
			)
			inputPath, namespace = "inputs/optional.yaml", ""
		}
	}
	testutil.WriteFile(
		t,
		after,
		inputPath,
		fmt.Sprintf(
			"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: %s, namespace: %q}\ndata: {VALUE: after}\n",
			testCaseInput.inputName,
			namespace,
		),
	)
	inputs, err := baseline.ResolvePaths(t.Context(), after, before)
	assert.NoError(t, err)
	for direction, pair := range [][2]*store.SourceArtifact{{inputs.BaseInput, inputs.Source}, {inputs.Source, inputs.BaseInput}} {
		var changes *change.Set
		if testCaseInput.committed {
			root := t.TempDir()
			writeOptionalInputCluster(t, root, manifest.KindConfigMap)
			testutil.WriteFile(
				t,
				root,
				"flux/other.yaml",
				testutil.KustomizationYAML(
					"other",
					"other",
					"  path: ./apps/other\n  sourceRef: {kind: GitRepository, name: cluster, namespace: flux-system}\n  postBuild:\n    substituteFrom: [{kind: ConfigMap, name: optional, optional: true}]\n",
				),
			)
			body, localErr := fs.ReadFile(inputs.Source.FS, inputPath)
			assert.NoError(t, localErr)
			if direction == 1 {
				testutil.WriteFile(t, root, inputPath, string(body))
			}
			repo := testutil.GitInit(t, root)
			options := &git.CommitOptions{
				Author: &object.Signature{Name: "test", Email: "test@example.test", When: time.Unix(0, 0)},
			}
			base := testutil.GitCommitAll(t, repo, "base", options)
			if direction == 0 {
				testutil.WriteFile(t, root, inputPath, string(body))
			} else {
				assert.NoError(t, os.Remove(filepath.Join(root, inputPath)))
			}
			testutil.GitCommitAll(t, repo, "input change", options)
			resolved, localErr := baseline.AutoResolve(
				t.Context(),
				root,
				base.String(),
				cachetest.New(t, "", sourcecache.New),
			)
			assert.NoError(t, localErr)
			changes, pair = resolved.Changes, [2]*store.SourceArtifact{resolved.BaseInput, resolved.Source}
		} else {
			changes, err = change.Detect(t.Context(), pair[0], pair[1], manifest.WorkScope{})
			assert.NoError(t, err)
		}
		filters, _ := optionalInputFilters(t, pair, changes)
		for _, f := range filters {
			assert.Equal(
				t,
				f.ShouldReconcile(
					testutil.KustomizationID("flux-system", "app"),
				),
				testCaseInput.inputName == "optional",
			)
			assert.Equal(t, f.ShouldReconcile(testutil.KustomizationID("other", "other")), false)
			assert.Equal(
				t,
				f.ShouldReconcile(
					manifest.NamedResource{
						Group:     manifest.SourceDomain,
						Kind:      manifest.KindGitRepository,
						Namespace: "flux-system",
						Name:      "cluster",
					},
				), testCaseInput.inputName == "optional",
			)
		}
	}
}
