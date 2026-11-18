package orchestrator

import (
	"encoding/json"
	"errors"
	"maps"
	"path/filepath"
	"slices"
	"testing"

	"github.com/home-operations/flate/pkg/selection"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
)

func witnessWork(summary manifest.WorkSummary, phase manifest.WorkPhase) map[manifest.NamedResource]uint64 {
	out := make(map[manifest.NamedResource]uint64)
	for recordIndex := range summary {
		record := summary[recordIndex]
		if record.Key.Phase == phase && record.Counts.Attempts != 0 {
			out[record.Key.Owner] += record.Counts.Attempts
		}
	}
	return out
}

func witnessPreparations(summary manifest.WorkSummary) map[manifest.WorkKey]manifest.WorkCounts {
	out := make(map[manifest.WorkKey]manifest.WorkCounts)
	for recordIndex := range summary {
		record := summary[recordIndex]
		if record.Key.Phase == manifest.WorkPreparation {
			key := record.Key
			key.Input = ""
			counts := out[key]
			counts.Add(&record.Counts)
			out[key] = counts
		}
	}
	return out
}

func witnessAdmissions(owners []manifest.NamedResource, namespace string) map[manifest.NamedResource]uint64 {
	a, b := testutil.KustomizationID("apps", "a"), testutil.KustomizationID("apps", "b")
	hr := testutil.HelmReleaseID("apps", "release")
	rs := manifest.NamedResource{
		Group:     manifest.FluxOperatorDomain,
		Kind:      manifest.KindResourceSet,
		Namespace: "apps",
		Name:      "set",
	}
	admission := map[manifest.NamedResource]uint64{
		a: 1, b: 1, hr: 1, rs: 1, manifest.BootstrapSourceID: 1,
		{Group: manifest.SourceDomain, Kind: manifest.KindGitRepository, Namespace: "apps", Name: "cluster"}:     1,
		{Group: manifest.FluxKustomizeDomain, Kind: manifest.KindKustomization, Namespace: "\x00", Name: "flux"}: 1,
		{Kind: manifest.KindConfigMap, Namespace: "apps", Name: "vars"}:                                          1,
	}
	for _, owner := range owners {
		switch owner {
		case a:
			admission[manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: namespace, Name: "a-output"}] = 1
		case b:
			admission[manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "apps", Name: "b-output"}] = 1
		case hr:
			admission[manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "apps", Name: "release-output"}] = 1
			admission[manifest.NamedResource{
				Group:     manifest.SourceDomain,
				Kind:      manifest.KindHelmChart,
				Namespace: "apps",
				Name:      "apps-release",
			}] = 1
		case rs:
			admission[manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "apps", Name: "set-one"}] = 1
		}
	}
	return admission
}

func assertWitnessSources(t *testing.T, run Rendered) {
	t.Helper()
	alias := manifest.NamedResource{
		Group:     manifest.SourceDomain,
		Kind:      manifest.KindGitRepository,
		Namespace: "apps",
		Name:      "cluster",
	}
	chart := manifest.NamedResource{
		Group:     manifest.SourceDomain,
		Kind:      manifest.KindHelmChart,
		Namespace: "apps",
		Name:      "apps-release",
	}
	for recordIndex := range run.Result.Work {
		record := run.Result.Work[recordIndex]
		assert.Equal(t, record.Key.Tree, run.ID)
		assert.Equal(t, record.Counts.RemoteTransfers, uint64(0))
		switch record.Key.Phase {
		case manifest.WorkKustomize:
			source := alias
			if record.Key.Owner == (manifest.NamedResource{}) || record.Key.Owner.ReportHidden() {
				source = manifest.BootstrapSourceID
			}
			assert.Equal(t, record.Key.Source, source)
			assert.Equal(t, record.Key.Input != "", true)
		case manifest.WorkHelm, manifest.WorkPostRender:
			assert.Equal(t, record.Key.Source, chart)
			assert.Equal(t, record.Key.Input != "", true)
		case manifest.WorkEvaluation:
			assert.Equal(t, record.Key.Source, manifest.BootstrapSourceID)
		case manifest.WorkCache:
			source := manifest.BootstrapSourceID
			if record.Key.Owner == testutil.KustomizationID("apps", "a") ||
				record.Key.Owner == testutil.KustomizationID("apps", "b") {
				source = alias
			} else if record.Key.Owner == testutil.HelmReleaseID("apps", "release") {
				source = chart
			}
			assert.Equal(t, record.Key.Source, source)
			assert.Equal(t, record.Key.Input != "", true)
		case manifest.WorkAdmission,
			manifest.WorkPreparation,
			manifest.WorkSubstitution,
			manifest.WorkAcquisition,
			manifest.WorkValidation,
			manifest.WorkIndexDecode,
			manifest.WorkChartDecode:
			continue
		}
	}
}

func testRemovedRequiredInputWork(t *testing.T, name string) {
	t.Helper()
	before := maps.Clone(workWitnessFiles())
	owner := testutil.KustomizationID("apps", "a")
	phase := manifest.WorkKustomize
	var removed []string
	if name == "component" {
		before["apps/a/kustomization.yaml"] = "resources: [cm.yaml]\ncomponents: [../../private]\n"
		before["private/kustomization.yaml"] = "apiVersion: kustomize.config.k8s.io/v1alpha1\nkind: Component\ncommonAnno" +
			"tations: {private: 'yes'}\n"
		removed = []string{"private/kustomization.yaml"}
	} else {
		owner, phase = testutil.HelmReleaseID("apps", "release"), manifest.WorkHelm
		before["charts/demo/Chart.yaml"] += "dependencies: [{name: child, version: 1.0.0}]\n"
		before["charts/demo/charts/child/Chart.yaml"] = "apiVersion: v2\nname: child\nversion: 1.0.0\n"
		before["charts/demo/charts/child/templates/cm.yaml"] = "apiVersion: v1\nkind: ConfigMap\n" +
			"metadata: {name: child-output, namespace: apps}\n"
		removed = []string{"charts/demo/charts/child/Chart.yaml", "charts/demo/charts/child/templates/cm.yaml"}
	}
	after := maps.Clone(before)
	for _, file := range removed {
		delete(after, file)
	}
	roots := [2]string{writeWitness(t, before), writeWitness(t, after)}
	var full [2]*Result
	for side, root := range roots {
		runs, err := renderTreesForTest(t.Context(), t,
			[]Tree{witnessTree(root, manifest.TreeHead)},
			Config{RecordWork: true, Concurrency: 2},
		)
		assert.Equal(t, errors.Is(err, manifest.ErrFlux), name == "component" && side == 1)
		full[side] = runs[0].Result
	}
	var previous []byte
	for repeat := range 2 {
		runs, err := renderTreesForTest(t.Context(), t,
			[]Tree{witnessTree(roots[0], manifest.TreeBase), witnessTree(roots[1], manifest.TreeHead)},
			Config{
				RecordWork:  true,
				Concurrency: 2,
				Selection:   selection.Request{Roots: []manifest.NamedResource{owner}},
			},
		)
		assert.Equal(t, errors.Is(err, manifest.ErrFlux), name == "component")
		for side, run := range runs {
			assert.Diff(
				t,
				visibleWitnessOutput(run.Result),
				witnessOutput(full[side], []manifest.NamedResource{owner}),
			)
			want := manifest.WorkCounts{Attempts: 1, Completed: 1}
			if name == "component" && side == 1 {
				want = manifest.WorkCounts{Attempts: 1, Failures: 1}
			}
			assert.Equal(t, workCounts(run.Result.Work, owner, phase), want)
			failures := 0
			if name == "component" && side == 1 {
				failures = 1
			}
			assert.Equal(t, len(run.Result.Failed), failures)
			assert.Equal(
				t,
				workCounts(run.Result.Work, testutil.KustomizationID("apps", "b"), manifest.WorkKustomize),
				manifest.WorkCounts{},
			)
			evaluation := map[manifest.NamedResource]uint64{}

			assert.Diff(t, witnessWork(run.Result.Work, manifest.WorkEvaluation), evaluation)
			assert.Diff(
				t,
				witnessWork(run.Result.Work, manifest.WorkPostRender),
				map[manifest.NamedResource]uint64{},
			)
			assertWitnessSources(t, run)
		}
		body, err := json.Marshal(
			[][]manifest.DocumentBatch{runs[0].Result.Manifests, runs[1].Result.Manifests},
		)
		assert.NoError(t, err)
		if repeat != 0 {
			assert.Equal(t, string(body), string(previous))
		}
		previous = body
	}
}

func TestRenderTrees_RemovedRequiredInputWork(t *testing.T) {
	t.Parallel()
	for _, name := range []string{"component", "chart-dependency"} {
		t.Run(name, func(t *testing.T) { t.Parallel(); testRemovedRequiredInputWork(t, name) })
	}
}

func witnessTree(root string, side manifest.TreeSide) Tree {
	return Tree{ID: manifest.TreeID{Cluster: ".", Side: side}, RepoRoot: root, Path: filepath.Join(root, "flux")}
}

func witnessOutput(result *Result, owners []manifest.NamedResource) map[manifest.NamedResource][]map[string]any {
	out := visibleWitnessOutput(result)
	for id := range out {
		if !slices.Contains(owners, id) {
			delete(out, id)
		}
	}
	return out
}

func witnessPhaseCounts(
	summary manifest.WorkSummary,
	phase manifest.WorkPhase,
) map[manifest.NamedResource]manifest.WorkCounts {
	out := make(map[manifest.NamedResource]manifest.WorkCounts)
	for recordIndex := range summary {
		record := summary[recordIndex]
		if record.Key.Phase == phase {
			counts := out[record.Key.Owner]
			counts.Add(&record.Counts)
			out[record.Key.Owner] = counts
		}
	}
	return out
}

func workWitnessFiles() map[string]string {
	return map[string]string{
		"flux/kustomization.yaml": "resources: [a.yaml, b.yaml, release.yaml, set.yaml, vars.yaml]\n",

		"flux/a.yaml": testutil.KustomizationYAML(
			"a",
			"apps",
			"  interval: 1h\n  path: ./apps/a\n  sourceRef: {kind: GitRepository, name"+
				": cluster}\n  postBuild:\n    substituteFrom: [{kind: ConfigMap, name: va"+
				"rs}]\n",
		),

		"flux/b.yaml": testutil.KustomizationYAML(
			"b",
			"apps",
			"  path: ./apps/b\n  sourceRef: {kind: GitRepository, name: cluster}\n",
		),

		"flux/vars.yaml": "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: vars, namespace: apps}\ndata: {VALUE: old}\n",

		"flux/release.yaml": "apiVersion: helm.toolkit.fluxcd.io/v2\nkind: HelmRelease\nmetadata: {name" +
			": release, namespace: apps}\nspec:\n  interval: 1h\n  chart:\n    spec:\n" +
			"      chart: charts/demo\n      sourceRef: {kind: GitRepository, name: cl" +
			"uster}\n  values: {value: old}\n",

		"flux/set.yaml": "apiVersion: fluxcd.controlplane.io/v1\nkind: ResourceSet\nmetadata: {name" +
			": set, namespace: apps}\nspec:\n  inputs: [{name: one, value: old}]\n  re" +
			"sources:\n    - apiVersion: v1\n      kind: ConfigMap\n      metadata: {n" +
			"ame: set-<< inputs.name >>, namespace: apps}\n      data: {value: '<< inp" +
			"uts.value >>'}\n",

		"apps/a/kustomization.yaml": "resources: [cm.yaml]\ncomponents: [../../shared]\n",

		"apps/a/cm.yaml": "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: a-output, namespace: ap" +
			"ps}\ndata: {value: '${VALUE}', image: 'busybox:1'}\n",

		"apps/b/kustomization.yaml": "resources: [cm.yaml]\ncomponents: [../../shared]\n",

		"apps/b/cm.yaml": "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: b-output, namespace: ap" + "ps}\ndata: {value: stable}\n",

		"shared/kustomization.yaml": "apiVersion: kustomize.config.k8s.io/v1alpha1\nkind: Component\ncommonAnnotations: {shared: old}\n",

		"charts/demo/Chart.yaml": "apiVersion: v2\nname: demo\nversion: 1.0.0\n",

		"charts/demo/templates/cm.yaml": "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: release-output, namespa" +
			"ce: apps}\ndata: {value: {{ .Values.value | quote }}}\n",
	}
}

func writeWitness(t *testing.T, files map[string]string) string {
	t.Helper()
	root := t.TempDir()
	for name, body := range files {
		testutil.WriteFile(t, root, name, body)
	}
	return root
}

func visibleWitnessOutput(result *Result) map[manifest.NamedResource][]map[string]any {
	out := outputByParent(result.Manifests)
	for id := range out {
		if id == (manifest.NamedResource{}) || id.ReportHidden() {
			delete(out, id)
		}
	}
	return out
}

func TestRenderTrees_SelectedFailureWork(t *testing.T) {
	t.Parallel()
	files := maps.Clone(workWitnessFiles())
	files["apps/b/kustomization.yaml"] = "resources: [missing.yaml]\n"
	root := writeWitness(t, files)
	a, b := testutil.KustomizationID("apps", "a"), testutil.KustomizationID("apps", "b")
	full, err := renderTreesForTest(t.Context(), t,
		[]Tree{witnessTree(root, manifest.TreeHead)},
		Config{RecordWork: true, Concurrency: 2},
	)
	assert.Equal(t, errors.Is(err, manifest.ErrFlux), true)
	for _, tc := range []struct {
		name string
		id   manifest.NamedResource
		fail bool
	}{
		{"unrelated", a, false},
		{"admitted", b, true},
	} {
		t.Run(
			tc.name,
			func(t *testing.T) {
				t.Parallel()
				runs, err := renderTreesForTest(t.Context(), t,
					[]Tree{witnessTree(root, manifest.TreeHead)},
					Config{
						RecordWork:  true,
						Concurrency: 2,
						Selection:   selection.Request{Roots: []manifest.NamedResource{tc.id}},
					},
				)
				assert.Equal(t, errors.Is(err, manifest.ErrFlux), tc.fail)
				result := runs[0].Result
				assert.Diff(
					t,
					visibleWitnessOutput(result),
					witnessOutput(full[0].Result, []manifest.NamedResource{tc.id}),
				)
				counts := workCounts(result.Work, b, manifest.WorkKustomize)
				if tc.fail {
					assert.Equal(t, counts, manifest.WorkCounts{Attempts: 1, Failures: 1})
					assert.Equal(t, len(result.Failed), 1)
				} else {
					assert.Equal(t, counts, manifest.WorkCounts{})
					assert.Equal(t, len(result.Failed), 0)
				}
				assert.Equal(
					t,
					workCounts(result.Work, testutil.HelmReleaseID("apps", "release"), manifest.WorkHelm),
					manifest.WorkCounts{},
				)
			},
		)
	}
}

func assertWitnessPreparations(t *testing.T, got, want map[manifest.WorkKey]manifest.WorkCounts) {
	t.Helper()
	for _, counts := range got {
		assert.Equal(t, counts.Attempts, counts.Completed+counts.Failures)
		assert.Equal(t, counts.OwnerPrepares, counts.Attempts)
	}
	assert.Diff(t, got, want)
}

func witnessCacheLimit(identities int) uint64 {
	// Readiness, membership, and publication can each repeat a cache lookup.
	n := uint64(identities)
	return 3 * (2 + 2*n) * (1 + 8*n)
}

func assertWitnessCacheCounts(t *testing.T, got, want *manifest.WorkCounts, limit uint64) {
	t.Helper()
	if *want == (manifest.WorkCounts{}) {
		assert.Equal(t, *got, *want)
		return
	}
	if got.Attempts < 1 || got.Attempts > limit {
		t.Errorf("cache attempts=%d, want 1..%d", got.Attempts, limit)
	}
	assert.Equal(t, got.Completed+got.Failures, got.Attempts)
	assert.Equal(t, got.CacheService <= got.Completed, true)
	if want.Completed != 0 {
		assert.Equal(t, got.Completed >= 1, true)
	}
	if want.CacheService != 0 {
		assert.Equal(t, got.CacheService >= 1, true)
	}
	if want.Failures == 0 {
		assert.Equal(t, got.Failures, uint64(0))
	}
	assert.Equal(t, *got, manifest.WorkCounts{
		Attempts: got.Attempts, Completed: got.Completed, Failures: got.Failures, CacheService: got.CacheService,
	})
}
