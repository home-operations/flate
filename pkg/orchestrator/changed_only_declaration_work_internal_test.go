package orchestrator

import (
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/change"
	"github.com/home-operations/flate/internal/selection"
	"github.com/home-operations/flate/internal/store"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
)

func TestRenderTrees_ChangedOnlySharedDeclarationWork(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name    string
		builder bool
	}{
		{name: "joryirving"},
		{name: "jfroy-nextflux", builder: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			files := map[string]string{
				"entry/ks.yaml": testutil.KustomizationYAML(
					"parent",
					"flux-system",
					"  interval: 1h\n  path: ./apps\n  sourceRef: {kind: GitRepository, name: flux-system}\n  postBuild:\n    substituteFrom: [{kind: ConfigMap, name: vars}]\n  patches:\n  - target: {kind: Kustomization}\n    patch: |-\n      apiVersion: kustomize.toolkit.fluxcd.io/v1\n      kind: Kustomization\n      metadata: {name: unused}\n      spec:\n        postBuild: {substitute: {MARKER: accepted}}\n",
				),

				"entry/vars.yaml":                         "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: vars, namespace: flux-system}\ndata: {CLUSTER: accepted}\n",
				"apps/kustomization.yaml":                 "resources: [llm]\n",
				"apps/llm/kustomization.yaml":             "namespace: llm\ncomponents: [../../components/namespace]\nreplacements: [{path: ../../components/replacements/ks.yaml}]\nresources: [child.yaml]\n",
				"components/namespace/kustomization.yaml": "apiVersion: kustomize.config.k8s.io/v1alpha1\nkind: Component\nresources: [namespace.yaml]\n",
				"components/namespace/namespace.yaml":     "apiVersion: v1\nkind: Namespace\nmetadata: {name: placeholder}\n",
				"components/replacements/ks.yaml":         "- source: {kind: Namespace, fieldPath: metadata.name}\n  targets:\n  - select: {kind: Kustomization, group: kustomize.toolkit.fluxcd.io}\n    fieldPaths: [spec.targetNamespace]\n    options: {create: true}\n",
				"shared/release.yaml": `apiVersion: helm.toolkit.fluxcd.io/v2
kind: HelmRelease
metadata: {name: whisper}
spec:
  interval: 1h
  chartRef: {kind: HelmChart, name: demo}
  values: {revision: before, marker: "${MARKER:=raw}"}
`,
				"apps/llm/child.yaml": testutil.KustomizationYAML(
					"child",
					"",
					"  interval: 1h\n  path: ./shared\n  sourceRef: {kind: GitRepository, name: flux-system, namespace: flux-system}\n",
				),
				"shared/kustomization.yaml":     "resources: [release.yaml, chart.yaml]\n",
				"shared/chart.yaml":             "apiVersion: source.toolkit.fluxcd.io/v1\nkind: HelmChart\nmetadata: {name: demo}\nspec:\n  interval: 1h\n  chart: charts/demo\n  sourceRef: {kind: GitRepository, name: flux-system}\n",
				"entry/source.yaml":             "apiVersion: source.toolkit.fluxcd.io/v1\nkind: GitRepository\nmetadata: {name: flux-system, namespace: llm}\nspec: {url: https://example.test/shared}\n",
				"charts/demo/Chart.yaml":        "apiVersion: v2\nname: demo\nversion: 1.0.0\n",
				"charts/demo/templates/cm.yaml": "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: release, namespace: llm}\ndata: {revision: {{ .Values.revision | quote }}, marker: {{ .Values.marker | quote }}}\n",
			}
			files["entry/applier.yaml"] = "apiVersion: helm.toolkit.fluxcd.io/v2\nkind: HelmRelease\nmetadata: {name: bootstrap, namespace: flux-system}\nspec:\n  interval: 1h\n  chart:\n    spec:\n      chart: charts/bootstrap\n      sourceRef: {kind: GitRepository, name: flux-system, namespace: flux-system}\n"
			files["charts/bootstrap/Chart.yaml"] = "apiVersion: v2\nname: bootstrap\nversion: 1.0.0\n"
			files["charts/bootstrap/templates/instance.yaml"] = "apiVersion: fluxcd.controlplane.io/v1\nkind: FluxInstance\nmetadata: {name: flux, namespace: flux-system, annotations: {revision: before}}\nspec:\n  sync: {name: flux-system, kind: GitRepository, url: https://example.test/shared, path: ./entry}\n"
			if tc.builder {
				delete(files, "charts/bootstrap/Chart.yaml")
				delete(files, "charts/bootstrap/templates/instance.yaml")
				files["entry/applier.yaml"] = testutil.KustomizationYAML("sync", "flux-system",
					"  interval: 1h\n  path: ./entry\n  sourceRef: {kind: GitRepository, name: flux-system}\n")
				files["entry/kustomization.yaml"] = "resources: [ks.yaml, source.yaml, vars.yaml, applier.yaml]\n"
			}
			if !tc.builder {
				files["entry/bootstrap/release.yaml"] = files["entry/applier.yaml"]
				files["entry/bootstrap/kustomization.yaml"] = "resources: [release.yaml]\n"
				files["entry/applier.yaml"] = testutil.KustomizationYAML(
					"bootstrap",
					"flux-system",
					"  interval: 1h\n  path: ./entry/bootstrap\n  sourceRef: {kind: GitRepository, name: flux-system}\n",
				)
				delete(files, "apps/kustomization.yaml")
			}
			roots := [2]string{writeWitness(t, files), ""}
			files["shared/release.yaml"] = strings.ReplaceAll(
				files["shared/release.yaml"],
				"revision: before",
				"revision: after",
			)

			changed := []string{"shared/release.yaml", "apps/llm/child.yaml"}
			if !tc.builder {
				files["charts/bootstrap/templates/instance.yaml"] = strings.ReplaceAll(
					files["charts/bootstrap/templates/instance.yaml"],
					"revision: before",
					"revision: after",
				)
				changed = append(changed, "charts/bootstrap/templates/instance.yaml")
			}
			files["apps/llm/child.yaml"] += "  timeout: 2m\n"
			roots[1] = writeWitness(t, files)
			inputs := [2]*store.SourceArtifact{testDirectoryInput(t, roots[0]), testDirectoryInput(t, roots[1])}
			trees := make([]Tree, 2)
			for side := range trees {
				trees[side] = Tree{
					ID: manifest.TreeID{
						Cluster: ".",
						Side:    []manifest.TreeSide{manifest.TreeBase, manifest.TreeHead}[side],
					},
					Path: filepath.Join(
						roots[side],
						"entry",
					),
					RepoRoot:        roots[side],
					SelfURLs:        []string{"https://example.test/shared"},
					Input:           inputs[side],
					BaseInput:       inputs[1-side],
					ExternalChanges: change.NewSet(changed),
				}
			}
			runs, err := renderTreesForTest(t.Context(), t,
				trees,
				Config{
					CacheDir:    t.TempDir(),
					Concurrency: 2,
					RecordWork:  true,
					Selection:   selection.Request{Changed: true},
				},
			)
			assert.NoError(t, err)
			assert.Equal(t, len(runs), 2)
			helmWork := map[manifest.NamedResource]uint64{testutil.HelmReleaseID("llm", "whisper"): 1}
			want := map[manifest.NamedResource]uint64{
				testutil.KustomizationID("flux-system", "parent"): 1,
				testutil.KustomizationID("llm", "child"):          1,
			}
			if tc.builder {
				want[testutil.KustomizationID("flux-system", "sync")] = 1
			} else {
				want[testutil.KustomizationID("flux-system", "bootstrap")] = 1
				helmWork[testutil.HelmReleaseID("flux-system", "bootstrap")] = 1
			}
			for side, run := range runs {
				oracle := trees[side]
				oracle.BaseInput, oracle.ExternalChanges = nil, nil
				full, err := renderTreesForTest(t.Context(), t, []Tree{oracle}, Config{Concurrency: 2})
				assert.NoError(t, err)
				actual, err := json.Marshal(run.Result.Manifests)
				assert.NoError(t, err)
				expected, err := json.Marshal(full[0].Result.Manifests)
				assert.NoError(t, err)
				assert.Equal(t, string(actual), string(expected))
				assert.Diff(t, witnessWork(run.Result.Work, manifest.WorkHelm), helmWork)
				assert.Diff(t, witnessWork(run.Result.Work, manifest.WorkKustomize), want)
			}
		})
	}
}

func TestRenderTrees_ChangedOnlyAnonymousPreparation(t *testing.T) {
	t.Parallel()
	files := map[string]string{
		"entry/app.yaml":                testutil.KustomizationYAML("app", "apps", "  interval: 1h\n  path: ./app\n"),
		"app/kustomization.yaml":        "resources: [release.yaml]\n",
		"app/release.yaml":              "apiVersion: helm.toolkit.fluxcd.io/v2\nkind: HelmRelease\nmetadata: {name: app, namespace: apps}\nspec:\n  interval: 1h\n  chart:\n    spec:\n      chart: charts/demo\n      sourceRef: {kind: GitRepository, name: flux-system, namespace: flux-system}\n  values: {revision: before}\n",
		"charts/demo/Chart.yaml":        "apiVersion: v2\nname: demo\nversion: 1.0.0\n",
		"charts/demo/templates/cm.yaml": "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: app, namespace: apps}\ndata: {revision: {{ .Values.revision | quote }}}\n",
	}
	roots := [2]string{writeWitness(t, files), ""}
	files["app/release.yaml"] = strings.ReplaceAll(files["app/release.yaml"], "revision: before", "revision: after")
	roots[1] = writeWitness(t, files)
	inputs := [2]*store.SourceArtifact{testDirectoryInput(t, roots[0]), testDirectoryInput(t, roots[1])}
	trees := make([]Tree, 2)
	for side := range trees {
		trees[side] = Tree{
			ID:   manifest.TreeID{Cluster: ".", Side: manifest.TreeSide(side)},
			Path: roots[side], RepoRoot: roots[side], Input: inputs[side], BaseInput: inputs[1-side],
			ExternalChanges: change.NewSet([]string{"app/release.yaml"}),
		}
	}
	cfg := Config{CacheDir: t.TempDir(), Concurrency: 2, RecordWork: true, Selection: selection.Request{Changed: true}}
	var first [2]string
	for _, regime := range []string{"cold", "warm"} {
		runs, err := renderTreesForTest(t.Context(), t, trees, cfg)
		assert.NoError(t, err)
		assert.Equal(t, len(runs), 2)
		for side, run := range runs {
			counts := uint64(0)
			for i := range run.Result.Work {
				row := &run.Result.Work[i]
				if row.Key.Owner == (manifest.NamedResource{}) && row.Key.Phase == manifest.WorkPreparation &&
					row.Key.Input == "admission" {
					counts += row.Counts.OwnerPrepares
				}
			}
			assert.Equal(t, counts, uint64(3))
			body, err := json.Marshal(run.Result.Manifests)
			assert.NoError(t, err)
			if regime == "cold" {
				first[side] = string(body)
			} else {
				assert.Equal(t, string(body), first[side])
				assert.Diff(
					t,
					witnessWork(run.Result.Work, manifest.WorkKustomize),
					map[manifest.NamedResource]uint64{},
				)
				assert.Diff(t, witnessWork(run.Result.Work, manifest.WorkHelm), map[manifest.NamedResource]uint64{})
			}
		}
	}
}
