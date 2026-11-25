package orchestrator

import (
	"cmp"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/home-operations/flate/internal/engine/baseline"
	"github.com/home-operations/flate/internal/engine/selection"
	sourcecache "github.com/home-operations/flate/internal/engine/source/cache"
	"github.com/home-operations/flate/internal/engine/store"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/assert"
	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/internal/testutil/networktest"
	"github.com/home-operations/flate/internal/testutil/testfixture"
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
					ExternalChanges: detectChanges(t, changed, changed),
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
	persistent := sourcecache.RenderPersistenceDisabledReason() == ""
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
			ExternalChanges: detectChanges(t, []string{"app/release.yaml"}, []string{"app/release.yaml"}),
		}
	}
	cfg := Config{CacheDir: t.TempDir(), Concurrency: 2, RecordWork: true, Selection: selection.Request{Changed: true}}
	admissionInput := witnessAdmissionInput()
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
					row.Key.Input == admissionInput {
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
				wantKustomize := map[manifest.NamedResource]uint64{}
				wantHelm := map[manifest.NamedResource]uint64{}
				if !persistent {
					wantKustomize[testutil.KustomizationID("apps", "app")] = 1
					wantHelm[testutil.HelmReleaseID("apps", "app")] = 1
				}
				assert.Diff(t, witnessWork(run.Result.Work, manifest.WorkKustomize), wantKustomize)
				assert.Diff(t, witnessWork(run.Result.Work, manifest.WorkHelm), wantHelm)
			}
		}
	}
}

func TestRenderTrees_ChangedOnlyOrderingWork(t *testing.T) {
	t.Parallel()
	t.Run("head-only-transformed-target", func(t *testing.T) {
		t.Parallel()
		assertHeadOnlyOrderingWork(t, false)
	})
	const middleSpec = "  path: ./middle\n  dependsOn: [{name: leaf}]\n"
	for _, tc := range []struct {
		name       string
		middleSpec string
		missing    bool
		blocked    bool
	}{
		{name: "intact", middleSpec: middleSpec},
		{name: "missing-leaf", middleSpec: middleSpec, missing: true, blocked: true},
		{
			name: "suspended-middle-with-missing-leaf", middleSpec: middleSpec + "  suspend: true\n",
			missing: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			files := map[string]string{
				"entry/app.yaml": testutil.KustomizationYAML("app", "apps",
					"  path: ./app\n  dependsOn: [{name: middle}]\n"),
				"entry/middle.yaml": testutil.KustomizationYAML("middle", "apps", tc.middleSpec),
				"entry/leaf.yaml":   testutil.KustomizationYAML("leaf", "apps", "  path: ./leaf\n"),
			}
			for _, name := range []string{"app", "middle", "leaf"} {
				files[name+"/kustomization.yaml"] = "resources: [cm.yaml]\n"
				files[name+"/cm.yaml"] = "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: " + name + "}\ndata: {revision: before}\n"
			}
			if tc.missing {
				delete(files, "entry/leaf.yaml")
			}

			roots := [2]string{writeWitness(t, files), ""}
			const edit = "app/cm.yaml"
			files[edit] = strings.ReplaceAll(files[edit], "before", "after")
			roots[1] = writeWitness(t, files)
			inputs := [2]*store.SourceArtifact{testDirectoryInput(t, roots[0]), testDirectoryInput(t, roots[1])}
			trees := make([]Tree, 2)
			for side := range trees {
				trees[side] = Tree{
					ID:   manifest.TreeID{Cluster: ".", Side: manifest.TreeSide(side)},
					Path: filepath.Join(roots[side], "entry"), RepoRoot: roots[side],
					Input: inputs[side], BaseInput: inputs[1-side],
					ExternalChanges: detectChanges(t, []string{edit}, []string{edit}),
				}
			}

			runs, err := renderTreesForTest(t.Context(), t, trees, Config{
				CacheDir: t.TempDir(), Concurrency: 2, RecordWork: true,
				Selection: selection.Request{Changed: true},
			})
			assert.Equal(t, err != nil, tc.blocked)
			assert.Equal(t, len(runs), 2)
			app := testutil.KustomizationID("apps", "app")
			wantKustomize := map[manifest.NamedResource]uint64{app: 1}
			if tc.blocked {
				wantKustomize = map[manifest.NamedResource]uint64{}
			}
			for side, run := range runs {
				assert.FatalIf(t, run.Result == nil, "side %d missing result: %v", side, err)
				kustomize := witnessWork(run.Result.Work, manifest.WorkKustomize)
				helm := witnessWork(run.Result.Work, manifest.WorkHelm)
				t.Logf("side %d: Kustomize %v, Helm %v", side, kustomize, helm)
				assert.Diff(t, kustomize, wantKustomize)
				assert.Diff(t, helm, map[manifest.NamedResource]uint64{})
				if tc.blocked {
					assertMissingOrderingLeaf(t, run.Result, app)
				}
			}
		})
	}
}

func TestRenderTrees_ChangedOnlyProjectedOrderingWork(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name, fixture, reader, renamedProducer             string
		web, values, outside                               uint64
		addTarget, kustomize, configMapOnly, unrelatedEdit bool
	}{
		{name: "helmrelease-rename", web: 1},
		{name: "kustomization-rename-patch-reader", web: 1, kustomize: true},
		{name: "helmrelease-rename-patch-dependency", web: 1},
		{name: "helmrelease-rename-patch-dependency-rename", reader: "web2", web: 1},
		{name: "helmrelease-rename-patch-dependency-descendant", web: 1},
		{name: "helmrelease-rename-patch-dependency-selector", web: 1},
		{name: "helmrelease-rename-patch-dependency-strategic", web: 1, outside: 1},
		{name: "unrelated-edit", fixture: "helmrelease-rename-patch-dependency-strategic", outside: 1, unrelatedEdit: true},
		{name: "helmrelease-rename-patch-reader-kind", web: 1},
		{name: "helmrelease-rename-patch-target", web: 1},
		{name: "helmrelease-rename-patch-target-and-producer", web: 1, renamedProducer: "database-renamed"},
		{name: "helmrelease-rename-flux-patch", web: 1},
		{name: "helmrelease-rename-patched-configmap", web: 1, configMapOnly: true},
		{name: "helmrelease-rename-expression", web: 1, values: 1},
		{name: "target-addition", fixture: "helmrelease-rename-patch-dependency", addTarget: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			fixture := testfixture.Materialize(t, "readiness-parity", cmp.Or(tc.fixture, tc.name))
			root := filepath.Join(fixture.Repo, "..", "helmrelease-rename")
			base := "HEAD~1"
			old := testutil.HelmReleaseID("web", "db")
			next := testutil.HelmReleaseID("web", "database")
			wantHelm := [2]map[manifest.NamedResource]uint64{{old: 1}, {next: 1}}
			wantBlocked := [2][]manifest.NamedResource{nil, {old}}
			reader := testutil.HelmReleaseID("web", cmp.Or(tc.reader, "web"))
			switch {
			case tc.configMapOnly:
				wantBlocked[manifest.TreeHead] = nil
			case tc.kustomize:
				reader = testutil.KustomizationID("flux-system", "app")
				wantHelm = [2]map[manifest.NamedResource]uint64{{}, {}}
				wantBlocked[manifest.TreeHead] = []manifest.NamedResource{
					testutil.KustomizationID("flux-system", "infra"),
				}
			case tc.addTarget:
				base = "HEAD"
				wantBlocked[manifest.TreeHead] = nil
				addOrderingTarget(t, root)
			case tc.unrelatedEdit:
				base = "HEAD"
				wantHelm = [2]map[manifest.NamedResource]uint64{{}, {}}
				wantBlocked[manifest.TreeHead] = nil
				testutil.WriteFile(t, root, "outside/config.yaml",
					"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: outside}\ndata: {version: changed}\n")
			}
			cache := cachetest.New(t, "", sourcecache.New)
			resolved, err := baseline.AutoResolve(t.Context(), baseline.Config{
				Network: networktest.New(t), Path: filepath.Join(root, "flux"),
				Base: base, Cache: cache,
			})
			assert.NoError(t, err)
			runs, err := renderTreesForTest(t.Context(), t, scopeInputTrees(resolved, resolved.SelfURLs), Config{
				Cache: cache, Concurrency: 2, RecordWork: true, Selection: selection.Request{Changed: true},
			})
			assert.Equal(t, err != nil, !tc.unrelatedEdit && !tc.addTarget && !tc.configMapOnly)
			assert.Equal(t, len(runs), 2)
			for _, run := range runs {
				side := run.ID.Side
				assert.FatalIf(t, run.Result == nil, "side %d missing result: %v", side, err)
				failure, failed := run.Result.Failed[reader]
				assert.Equal(t, failed, len(wantBlocked[side]) > 0)
				assert.Diff(t, failure.BlockedBy(), wantBlocked[side])
				helm := witnessWork(run.Result.Work, manifest.WorkHelm)
				kustomize := witnessWork(run.Result.Work, manifest.WorkKustomize)
				t.Logf("side %d: Kustomize %v, Helm %v", run.ID.Side, kustomize, helm)
				if tc.addTarget {
					assert.Equal(t, helm[reader], uint64(0))
				} else {
					assert.Diff(t, helm, wantHelm[side])
				}
				_, deletedEdgeFailed := run.Result.Failed[testutil.HelmReleaseID("web", "without-dependency")]
				assert.Equal(t, deletedEdgeFailed, false)
				assert.Equal(t, kustomize[testutil.KustomizationID("flux-system", "outside")], tc.outside)
				assert.Equal(t, kustomize[testutil.KustomizationID("flux-system", "web")], tc.web)
				assert.Equal(t, kustomize[testutil.KustomizationID("flux-system", "values")], tc.values)
				if tc.renamedProducer != "" {
					producer := [2]string{"database", tc.renamedProducer}[side]
					assert.Equal(t, kustomize[testutil.KustomizationID("flux-system", producer)], uint64(1))
				}
			}
		})
	}
}

func addOrderingTarget(t *testing.T, root string) {
	t.Helper()
	contents, err := os.ReadFile(filepath.Join(root, "database", "db.yaml"))
	assert.NoError(t, err)
	added := strings.Replace(string(contents), "name: database", "name: db", 1)
	testutil.WriteFile(t, root, "database/added.yaml", added)
	testutil.WriteFile(t, root, "database/kustomization.yaml", "resources: [db.yaml, added.yaml]\n")
}

func TestRenderTrees_ChangedOnlyGeneratedOrderingWork(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		scenario, name, entry          string
		producer, prerequisite, reader manifest.NamedResource
		phase                          manifest.WorkPhase
		producerAttempts               uint64
	}{
		{
			scenario: "helm-dependencies", name: "changed-only-render-emitted-dependency", entry: "flux",
			producer:     testutil.HelmReleaseID("flux-system", "emitter"),
			prerequisite: testutil.KustomizationID("flux-system", "rendered-dependency"),
			reader:       testutil.KustomizationID("flux-system", "consumer"), phase: manifest.WorkHelm,
			producerAttempts: 1,
		},
		{
			scenario: "dependency-readiness", name: "depends-on-produced-target-substitution-diff",
			entry: "kubernetes/hegira/flux", producer: testutil.KustomizationID("apps", "producer"),
			prerequisite: testutil.KustomizationID("apps", "generated-raw-done"),
			reader:       testutil.KustomizationID("apps", "consumer"), phase: manifest.WorkKustomize,
			producerAttempts: 2,
		},
	} {
		t.Run(tc.scenario, func(t *testing.T) {
			t.Parallel()
			fixture := testfixture.Materialize(t, tc.scenario, tc.name)
			cache := cachetest.New(t, "", sourcecache.New)
			resolved, err := baseline.AutoResolve(t.Context(), baseline.Config{
				Network: networktest.New(t), Path: filepath.Join(fixture.Repo, tc.entry), Base: "HEAD", Cache: cache,
			})
			assert.NoError(t, err)
			trees := scopeInputTrees(resolved, resolved.SelfURLs)
			runs, err := renderTreesForTest(t.Context(), t, trees, Config{
				Cache: cache, Concurrency: 2, RecordWork: true, Selection: selection.Request{Changed: true},
			})
			assert.NoError(t, err)
			assert.Equal(t, len(runs), 2)
			var producerAttempts uint64
			for _, run := range runs {
				kustomize := witnessWork(run.Result.Work, manifest.WorkKustomize)
				helm := witnessWork(run.Result.Work, manifest.WorkHelm)
				t.Logf("side %d: Kustomize %v, Helm %v", run.ID.Side, kustomize, helm)
				producerAttempts += workCounts(run.Result.Work, tc.producer, tc.phase).Attempts
				assert.Equal(t, kustomize[tc.prerequisite], uint64(0))
				assert.Equal(t, kustomize[tc.reader], uint64(1))
				assert.Equal(t, run.store.GetDeclaration(tc.prerequisite) != nil, true)
			}
			assert.Equal(t, producerAttempts, tc.producerAttempts)
		})
	}
}

func assertMissingOrderingLeaf(t *testing.T, result *Result, app manifest.NamedResource) {
	t.Helper()
	middle := testutil.KustomizationID("apps", "middle")
	assert.Diff(t, result.Failed[app].BlockedBy(), []manifest.NamedResource{middle})
	message := result.Failed[app].String()
	assert.Equal(t, strings.Contains(message, "dependency not found") && strings.Contains(message, "apps/leaf"), true)
	for _, batch := range result.Manifests {
		assert.Equal(t, batch.Parent.Named() == app, false)
	}
}

func TestRenderTrees_OrderingPrerequisiteControllerFailure(t *testing.T) {
	t.Parallel()
	files := map[string]string{
		"entry/dependency.yaml": testutil.KustomizationYAML("dependency", "apps",
			"  path: ./dependency\n  postBuild: {substitute: {OTHER: present}}\n"),
		"entry/app.yaml": testutil.KustomizationYAML("app", "apps",
			"  path: ./app\n  dependsOn: [{name: dependency}]\n"),
		"dependency/kustomization.yaml": "resources: [cm.yaml]\n",
		"dependency/cm.yaml":            "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: dependency}\ndata: {value: '${ABSENT}'}\n",
		"app/kustomization.yaml":        "resources: [cm.yaml]\n",
		"app/cm.yaml":                   "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: app}\n",
	}
	root := writeWitness(t, files)
	app := testutil.KustomizationID("apps", "app")
	dependency := testutil.KustomizationID("apps", "dependency")
	runs, err := renderTreesForTest(t.Context(), t, []Tree{{
		ID:   manifest.TreeID{Cluster: ".", Side: manifest.TreeHead},
		Path: filepath.Join(root, "entry"), RepoRoot: root, Input: testDirectoryInput(t, root),
	}}, Config{
		CacheDir:            t.TempDir(),
		Concurrency:         2,
		RecordWork:          true,
		StrictSubstitutions: true,
		Selection:           selection.Request{Roots: []manifest.NamedResource{app}},
	})
	assert.Equal(t, err != nil, true)
	assert.FatalIf(t, len(runs) != 1 || runs[0].Result == nil, "missing result: %v", err)
	result := runs[0].Result
	assert.Diff(t, witnessWork(result.Work, manifest.WorkKustomize), map[manifest.NamedResource]uint64{dependency: 1})
	assert.Diff(t, witnessWork(result.Work, manifest.WorkHelm), map[manifest.NamedResource]uint64{})
	assert.Equal(t, result.Failed[dependency].Outcome(), store.OutcomeFailed)
	assert.Diff(t, result.Failed[app].BlockedBy(), []manifest.NamedResource{dependency})
	assert.Equal(t, strings.Contains(result.Failed[app].String(), "ABSENT"), true)
	for _, batch := range result.Manifests {
		assert.Equal(t, batch.Parent.Named() == app, false)
	}
}
