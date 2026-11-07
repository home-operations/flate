package orchestrator

import (
	"encoding/json"
	"errors"
	"maps"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

type crdOutput struct {
	selected map[manifest.NamedResource][]map[string]any
	literal  map[manifest.NamedResource][]map[string]any
}

func crdSelectionOutput(t *testing.T, tc crdSelectionCase, cfgValue *Config) crdOutput {
	t.Helper()
	cfg := *cfgValue
	hr := testutil.HelmReleaseID("apps", "selected")
	unrelated := manifest.NamedResource{
		Group: manifest.FluxOperatorDomain, Kind: manifest.KindResourceSet, Namespace: "other", Name: "unrelated",
	}
	widget := manifest.NamedResource{
		Group:     "fixture.example",
		Kind:      "Widget",
		Namespace: "other",
		Name:      "missing-widget",
	}
	full, fullErr := renderTreesForTest(t.Context(), t, []Tree{crdWorkTree(t, tc.present, tc.foreign)}, cfg)
	assert.Equal(t, errors.Is(fullErr, manifest.ErrFlux), true)
	assert.Equal(t, len(full), 1)
	assert.Diff(t, full[0].Result.Blocked[unrelated], []manifest.NamedResource{widget})
	want := witnessOutput(full[0].Result, []manifest.NamedResource{hr})
	literal := map[manifest.NamedResource][]map[string]any{}
	if tc.success {
		literal[hr] = []map[string]any{{
			"apiVersion": "v1", "kind": "ConfigMap",
			"metadata": map[string]any{"name": "selected-output", "namespace": "apps", "labels": map[string]any{
				"helm.toolkit.fluxcd.io/name": "selected", "helm.toolkit.fluxcd.io/namespace": "apps",
			}},
			"data": map[string]any{"value": "rendered"},
		}}
	}
	assert.Diff(t, want, literal)
	return crdOutput{selected: want, literal: literal}
}

func crdWorkTree(t *testing.T, present, foreign bool) Tree {
	t.Helper()
	root := t.TempDir()
	resources := "resources: [selected.yaml, unrelated.yaml"
	if present {
		resources += ", crd.yaml"
		testutil.WriteFile(t, root, "apps/crd.yaml", helmReleaseCRD)
	}
	testutil.WriteFile(t, root, "apps/kustomization.yaml", resources+"]\n")
	testutil.WriteFile(t, root, "flux/kustomization.yaml", "resources: [source.yaml, owner.yaml]\n")
	testutil.WriteFile(
		t,
		root,
		"flux/owner.yaml",
		testutil.KustomizationYAML(
			"owner",
			"apps",
			"  path: ./apps\n  sourceRef: {kind: GitRepository, name: cluster}\n",
		),
	)
	testutil.WriteFile(
		t,
		root,
		"flux/source.yaml",
		"apiVersion: source.toolkit.fluxcd.io/v1\nkind: GitRepository\nmetadata: {"+
			"name: cluster, namespace: apps}\nspec: {url: https://fixture.invalid/crd-"+
			"work}\n",
	)
	selected := `apiVersion: fluxcd.controlplane.io/v1
kind: ResourceSet
metadata: {name: selected, namespace: apps}
spec:
  dependsOn:
    - apiVersion: apiextensions.k8s.io/v1
      kind: CustomResourceDefinition
      name: helmreleases.helm.toolkit.fluxcd.io
  resources:
    - apiVersion: source.toolkit.fluxcd.io/v1
      kind: HelmChart
      metadata: {name: generated, namespace: apps}
      spec:
        chart: charts/demo
        sourceRef: {kind: GitRepository, name: cluster}
    - apiVersion: helm.toolkit.fluxcd.io/v2
      kind: HelmRelease
      metadata: {name: selected, namespace: apps}
      spec:
        releaseName: selected
        chartRef: {kind: HelmChart, name: generated}
`
	if foreign {
		selected = strings.Replace(selected, "apiVersion: apiextensions.k8s.io/v1", "apiVersion: fixture.example/v1", 1)
	}
	testutil.WriteFile(t, root, "apps/selected.yaml", selected)
	testutil.WriteFile(t, root, "apps/unrelated.yaml", `apiVersion: fluxcd.controlplane.io/v1
kind: ResourceSet
metadata: {name: unrelated, namespace: other}
spec:
  dependsOn: [{apiVersion: fixture.example/v1, kind: Widget, name: missing-widget}]
  resources:
    - apiVersion: v1
      kind: ConfigMap
      metadata: {name: unrelated-output, namespace: other}
`)
	testutil.WriteFile(t, root, "charts/demo/Chart.yaml", "apiVersion: v2\nname: demo\nversion: 1.0.0\n")
	testutil.WriteFile(
		t,
		root,
		"charts/demo/templates/cm.yaml",
		"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: selected-output, namesp"+"ace: apps}\ndata: {value: rendered}\n",
	)
	return Tree{
		ID:       manifest.TreeID{Cluster: "crd-cluster", Side: manifest.TreeHead},
		Path:     filepath.Join(root, "flux"),
		RepoRoot: root,
		SelfURLs: []string{"https://fixture.invalid/crd-work"},
	}
}

func TestRenderTrees_CRDSelectionWork(t *testing.T) {
	t.Parallel()
	for _, tc := range []crdSelectionCase{
		{name: "strict absent"},
		{name: "allowed absent", allowed: true, success: true},
		{name: "present", present: true, success: true},
		{name: "foreign group", allowed: true, present: true, foreign: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			testCRDSelectionWork(t, tc)
		})
	}
}

func crdSelectionWork(treeValue *Tree, success bool) map[manifest.WorkKey]manifest.WorkCounts {
	tree := *treeValue

	rs := manifest.NamedResource{
		Group:     manifest.FluxOperatorDomain,
		Kind:      manifest.KindResourceSet,
		Namespace: "apps",
		Name:      "selected",
	}
	hr := testutil.HelmReleaseID("apps", "selected")
	ks := testutil.KustomizationID("apps", "owner")
	source := manifest.NamedResource{
		Group:     manifest.SourceDomain,
		Kind:      manifest.KindGitRepository,
		Namespace: "apps",
		Name:      "cluster",
	}
	chart := manifest.NamedResource{
		Group:     manifest.SourceDomain,
		Kind:      manifest.KindHelmChart,
		Namespace: "apps",
		Name:      "generated",
	}
	wantWork := map[manifest.WorkKey]manifest.WorkCounts{
		{Tree: tree.ID, Source: manifest.BootstrapSourceID, Phase: manifest.WorkAcquisition}: {
			Attempts: 1, Completed: 1, LocalAcquisitions: 1,
		},
		{Tree: tree.ID, Owner: ks, Source: source, Phase: manifest.WorkCache}: {
			Attempts:  1,
			Completed: 1,
		},
		{Tree: tree.ID, Owner: ks, Source: source, Phase: manifest.WorkKustomize}: {Attempts: 1, Completed: 1},
	}
	if success {
		wantWork[manifest.WorkKey{
			Tree: tree.ID, Owner: rs, Source: manifest.BootstrapSourceID, Phase: manifest.WorkEvaluation,
		}] = manifest.WorkCounts{Attempts: 1, Completed: 1}
		wantWork[manifest.WorkKey{Tree: tree.ID, Owner: hr, Source: chart, Phase: manifest.WorkHelm}] = manifest.WorkCounts{
			Attempts:  1,
			Completed: 1,
		}
		wantWork[manifest.WorkKey{Tree: tree.ID, Owner: hr, Source: chart, Phase: manifest.WorkCache}] = manifest.WorkCounts{
			Attempts:  1,
			Completed: 1,
		}
	}
	return wantWork
}

type crdSelectionCase struct {
	name                               string
	allowed, present, foreign, success bool
}

func assertCRDSelectionWork(t *testing.T, treeValue *Tree, result *Result, tc crdSelectionCase) {
	t.Helper()
	tree := *treeValue
	rs := manifest.NamedResource{
		Group:     manifest.FluxOperatorDomain,
		Kind:      manifest.KindResourceSet,
		Namespace: "apps",
		Name:      "selected",
	}
	unrelated := manifest.NamedResource{Group: rs.Group, Kind: rs.Kind, Namespace: "other", Name: "unrelated"}
	hr := testutil.HelmReleaseID("apps", "selected")
	ks := testutil.KustomizationID("apps", "owner")
	entry := testutil.KustomizationID("\x00", "flux")
	source := manifest.NamedResource{
		Group:     manifest.SourceDomain,
		Kind:      manifest.KindGitRepository,
		Namespace: "apps",
		Name:      "cluster",
	}
	chart := manifest.NamedResource{
		Group:     manifest.SourceDomain,
		Kind:      manifest.KindHelmChart,
		Namespace: "apps",
		Name:      "generated",
	}
	crd := manifest.NamedResource{
		Group: "apiextensions.k8s.io",
		Kind:  manifest.KindCustomResourceDefinition,
		Name:  "helmreleases.helm.toolkit.fluxcd.io",
	}
	actual := map[manifest.WorkKey]manifest.WorkCounts{}
	admissions := map[manifest.NamedResource]uint64{
		manifest.BootstrapSourceID: 1, entry: 1, ks: 1, source: 1, rs: 1, unrelated: 1,
	}
	if tc.present {
		admissions[crd] = 1
	}
	if tc.success {
		admissions[chart], admissions[hr] = 1, 1
		admissions[manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "apps", Name: "selected-output"}] = 1
	}
	assert.Diff(t, witnessWork(result.Work, manifest.WorkAdmission), admissions)
	for recordIndex := range result.Work {
		record := &result.Work[recordIndex]
		assert.Equal(t, record.Key.Tree, tree.ID)
		assert.Equal(t, record.Counts.RemoteTransfers, uint64(0))
		if record.Key.Phase == manifest.WorkAdmission {
			assert.Equal(t, record.Counts, manifest.WorkCounts{Attempts: 1, Completed: 1})
		}
		if record.Key.Phase == manifest.WorkAcquisition {
			assert.Equal(t, record.Key.Input, tree.RepoRoot)
		}
		switch string(record.Key.Phase) {
		case string(manifest.WorkEvaluation),
			string(manifest.WorkKustomize),
			string(manifest.WorkHelm),
			string(manifest.WorkAcquisition),
			string(manifest.WorkCache):
			key := record.Key
			key.Input = ""
			if key.Phase == manifest.WorkEvaluation {
				assert.Equal(t, record.Key.Input, "")
			} else {
				assert.Equal(t, record.Key.Input != "", true)
			}
			counts := actual[key]
			counts.Add(&record.Counts)
			actual[key] = counts
		}
	}
	assert.Diff(t, actual, crdSelectionWork(&tree, tc.success))
}

func testCRDSelectionWork(t *testing.T, tc crdSelectionCase) {
	t.Helper()
	rs := manifest.NamedResource{
		Group:     manifest.FluxOperatorDomain,
		Kind:      manifest.KindResourceSet,
		Namespace: "apps",
		Name:      "selected",
	}
	hr := testutil.HelmReleaseID("apps", "selected")
	chart := manifest.NamedResource{
		Group:     manifest.SourceDomain,
		Kind:      manifest.KindHelmChart,
		Namespace: "apps",
		Name:      "generated",
	}
	crd := manifest.NamedResource{
		Group: "apiextensions.k8s.io",
		Kind:  manifest.KindCustomResourceDefinition,
		Name:  "helmreleases.helm.toolkit.fluxcd.io",
	}
	cfg := Config{RecordWork: true, Concurrency: 2}
	if tc.allowed {
		cfg.AllowMissingKinds = []string{manifest.KindCustomResourceDefinition}
	}
	crdSelectionOutputResult := crdSelectionOutput(t, tc, &cfg)
	want, literal := crdSelectionOutputResult.selected, crdSelectionOutputResult.literal
	dependency := crd
	if tc.foreign {
		dependency.Group, dependency.Namespace = "fixture.example", "apps"
	}
	blocked := map[manifest.NamedResource][]manifest.NamedResource{}
	var failures []manifest.NamedResource
	if !tc.success {
		blocked[rs] = []manifest.NamedResource{dependency}
		failures = []manifest.NamedResource{rs}
	}
	var warnings []manifest.Warning
	if tc.allowed && !tc.present {
		warnings = []manifest.Warning{{
			Resource: crd, Subject: manifest.DiagnosticFor(crd, nil), Related: []manifest.NamedResource{rs},
			Category: manifest.WarnMissingCRD,
			Count:    1,
			Message:  "CRD " + crd.String() + " absent from offline inputs; accepted for " + rs.String(),
		}}
	}
	cfg.Selection.Roots = []manifest.NamedResource{{Group: hr.Group, Kind: hr.Kind, Namespace: "apps"}}
	var previous []byte
	for repeat := range 2 {
		tree := crdWorkTree(t, tc.present, tc.foreign)
		runs, runErr := renderTreesForTest(t.Context(), t, []Tree{tree}, cfg)
		assert.Equal(t, errors.Is(runErr, manifest.ErrFlux), !tc.success)
		assert.Equal(t, len(runs), 1)
		result := runs[0].Result
		assert.Diff(t, slices.SortedFunc(maps.Keys(result.Failed), manifest.NamedResource.Compare), failures)
		assert.Diff(t, result.Blocked, blocked)
		assert.Diff(t, result.Warnings, warnings)
		for _, info := range result.Failed {
			assert.Equal(t, info.Status, store.StatusFailed)
		}
		projectedResults0, projectionErrors0 := Select(runs, runErr, cfg.Selection.Roots, nil)
		selected, selectedErr, remainder := projectedResults0, projectionErrors0.Selected, projectionErrors0.Remaining
		assert.NoError(t, selectedErr)
		assert.NoError(t, remainder)
		assert.Diff(t, outputByParent(selected[0].Manifests), want)
		assert.Diff(t, outputByParent(selected[0].Manifests), literal)
		assert.Diff(
			t,
			slices.SortedFunc(maps.Keys(selected[0].Failed), manifest.NamedResource.Compare),
			[]manifest.NamedResource(nil),
		)
		assert.Diff(t, selected[0].Warnings, warnings)
		assert.Equal(t, len(selected[0].Objects), len(literal))
		if tc.success {
			assert.Equal(t, selected[0].Objects[0].Named(), hr)
			assert.Equal(t, runs[0].Store().GetObject(chart) != nil, true)
		}
		assertCRDSelectionWork(t, &tree, result, tc)
		var documents []map[string]any
		for _, batch := range selected[0].Manifests {
			documents = append(documents, batch.Documents...)
		}
		body, err := json.Marshal(documents)
		assert.NoError(t, err)
		if repeat != 0 {
			assert.Equal(t, string(body), string(previous))
		}
		previous = body
	}
}
