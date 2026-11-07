package orchestrator

import (
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"testing/fstest"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/diff"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

func clusterSourceQualifiedTrees(t *testing.T) []Tree {
	t.Helper()
	trees := make([]Tree, 0, 2)
	for _, cluster := range []manifest.ClusterID{"alpha", "omega"} {
		root := t.TempDir()
		for side := manifest.TreeBase; side <= manifest.TreeHead; side++ {
			name := string(cluster)
			if cluster == "alpha" {
				name += []string{"-old", "-new"}[side]
			}
			url := "https://fixture.invalid/" + string(cluster)
			input := fstest.MapFS{
				"entry/kustomization.yaml": {Data: []byte("resources: [source.yaml, owner.yaml, settings.yaml]\n")},
				"entry/source.yaml": {Data: []byte(
					"apiVersion: source.toolkit.fluxcd.io/v1\nkind: GitRepository\n" +
						"metadata: {name: self, namespace: apps}\nspec: {url: " + url + "}\n",
				)},
				"entry/owner.yaml": {Data: []byte(testutil.KustomizationYAML(
					"owner", "apps",
					"  path: ./app\n  sourceRef: {kind: GitRepository, name: self}\n"+
						"  postBuild: {substituteFrom: [{kind: ConfigMap, name: settings}]}\n",
				))},
				"entry/settings.yaml": {Data: []byte(
					"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: settings, namespace: apps}\ndata: {NAME: " + name + "}\n",
				)},
				"app/kustomization.yaml": {Data: []byte("resources: [cm.yaml]\n")},
				"app/cm.yaml": {
					Data: []byte("apiVersion: v1\nkind: ConfigMap\nmetadata: {name: '${NAME}', namespace: apps}\n"),
				},
			}
			trees = append(trees, Tree{
				ID:       manifest.TreeID{Cluster: cluster, Side: side},
				RepoRoot: root,
				Path:     filepath.Join(root, "entry"),
				SelfURLs: []string{url},
				Input:    &store.SourceArtifact{LocalPath: root, LocalRoot: root, FS: input},
			})
		}
	}
	return trees
}

func fluxInstanceSyncTrees(t *testing.T) []Tree {
	t.Helper()
	const url = "https://fixture.invalid/entry"
	trees := make([]Tree, 0, 2)
	for side := manifest.TreeBase; side <= manifest.TreeHead; side++ {
		root := t.TempDir()
		testutil.WriteFile(
			t,
			root,
			"entry/source.yaml",
			"apiVersion: source.toolkit.fluxcd.io/v1\nkind: GitRepository\n"+
				"metadata: {name: self, namespace: apps}\nspec: {url: "+url+"}\n",
		)
		testutil.WriteFile(
			t,
			root,
			"entry/generator.yaml",
			"apiVersion: helm.toolkit.fluxcd.io/v2\nkind: HelmRelease\nmetadata: {name: generator, namespace: apps}\n"+
				"spec:\n  chart:\n    spec:\n      chart: charts/bootstrap\n"+
				"      sourceRef: {kind: GitRepository, name: self, namespace: apps}\n",
		)
		testutil.WriteFile(t, root, "charts/bootstrap/Chart.yaml", "apiVersion: v2\nname: bootstrap\nversion: 1.0.0\n")
		application := "apiVersion: fluxcd.controlplane.io/v1\nkind: FluxInstance\nmetadata: {name: flux, namespace: apps}\n" +
			"spec:\n  sync: {name: self, kind: GitRepository, url: " + url + ", ref: main, path: ./entry}\n"
		testutil.WriteFile(
			t,
			root,
			"charts/bootstrap/templates/application.yaml",
			"{{ print "+fmt.Sprintf("%q", application)+" }}",
		)
		testutil.WriteFile(
			t,
			root,
			"entry/workload.yaml",
			testutil.KustomizationYAML(
				"workload",
				"apps",
				"  interval: "+[]string{"1h", "2h"}[side]+"\n  path: ./workload\n  sourceRef: {kind: GitRepository, name: self}\n",
			),
		)
		testutil.WriteFile(t, root, "workload/kustomization.yaml", "resources: [cm.yaml]\n")
		testutil.WriteFile(
			t,
			root,
			"workload/cm.yaml",
			"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: unchanged, namespace: apps}\n",
		)
		trees = append(trees, Tree{
			ID:       manifest.TreeID{Cluster: ".", Side: side},
			RepoRoot: root,
			Path:     filepath.Join(root, "entry"),
			SelfURLs: []string{url},
		})
	}
	return trees
}

func clusterDiffForTest(t *testing.T, pair []Rendered) []byte {
	t.Helper()
	projectedResults0, projectionErrors0 := Select(
		pair,
		errors.Join(pair[0].Err, pair[1].Err),
		[]manifest.NamedResource{testutil.KustomizationID("", "")},
		nil,
	)
	selected, err, _ := projectedResults0, projectionErrors0.Selected, projectionErrors0.Remaining
	assert.NoError(t, err)
	body, warnings, suppressed, err := diff.Compare(
		diff.Snapshot{
			Subjects: selected[0].Subjects, Manifests: selected[0].Manifests,
			Unavailable: selected[0].Unavailable, ProducerKeys: selected[0].ProducerKeys,
		},
		diff.Snapshot{
			Subjects: selected[1].Subjects, Manifests: selected[1].Manifests,
			Unavailable: selected[1].Unavailable, ProducerKeys: selected[1].ProducerKeys,
		},
		nil,
		diff.Options{Format: diff.FormatJSON},
	)
	assert.NoError(t, err)
	assert.Equal(t, len(warnings), 0)
	assert.Equal(t, len(suppressed), 0)
	return body
}

func TestRenderTrees_ClusterSourceQualifiedChanges(t *testing.T) {
	t.Parallel()
	trees := clusterSourceQualifiedTrees(t)
	slices.Reverse(trees)
	runs, err := renderTreesForTest(t.Context(), t, trees, Config{RecordWork: true, Concurrency: 2})
	assert.NoError(t, err)
	assert.Equal(t, len(runs), 4)
	owner := testutil.KustomizationID("apps", "owner")
	source := manifest.NamedResource{
		Group:     manifest.SourceDomain,
		Kind:      manifest.KindGitRepository,
		Namespace: "apps",
		Name:      "self",
	}
	var unchangedBuilds manifest.WorkCounts
	for _, run := range runs {
		assert.NoError(t, run.Err)
		declaration, ok := run.Store().Get[*manifest.GitRepository](source)
		if !ok {
			t.Fatalf("cluster %s lost its source", run.ID.Cluster)
		}
		assert.Equal(t, declaration.URL, "https://fixture.invalid/"+string(run.ID.Cluster))
		if run.ID.Cluster == "alpha" {
			documents := outputByParent(run.Result.Manifests)[owner]
			assert.Equal(t, len(documents), 1)
			assert.Equal(
				t,
				documents[0]["metadata"].(map[string]any)["name"],
				any([]string{"alpha-old", "alpha-new"}[run.ID.Side]),
			)
			assert.Equal(
				t,
				workCounts(run.Result.Work, owner, manifest.WorkSubstitution),
				manifest.WorkCounts{Attempts: 1, Completed: 1},
			)
		} else {
			assert.Equal(t, len(outputByParent(run.Result.Manifests)[owner]), 0)
			assert.Equal(t, workCounts(run.Result.Work, owner, manifest.WorkKustomize), manifest.WorkCounts{})
			unchangedBuilds.Add(new(workCounts(run.Result.Work, manifest.NamedResource{}, manifest.WorkKustomize)))
			assert.Equal(
				t,
				workCounts(run.Result.Work, manifest.NamedResource{}, manifest.WorkSubstitution),
				manifest.WorkCounts{},
			)
			assert.Equal(
				t,
				workCounts(run.Result.Work, manifest.NamedResource{}, manifest.WorkHelm),
				manifest.WorkCounts{},
			)
		}
		for recordIndex := range run.Result.Work {
			record := run.Result.Work[recordIndex]
			assert.Equal(t, record.Key.Tree, run.ID)
			if record.Key.Owner == owner && record.Key.Phase == manifest.WorkKustomize {
				assert.Equal(t, record.Key.Source, source)
			}
		}
	}
	assert.Equal(t, unchangedBuilds, manifest.WorkCounts{})
	for i := 0; i < len(runs); i += 2 {
		pair := runs[i : i+2]
		full := make([]Rendered, 2)
		for side := range full {
			tree := trees[slices.IndexFunc(trees, func(tree Tree) bool { return tree.ID == pair[side].ID })]
			isolated, err := renderTreesForTest(t.Context(), t, []Tree{tree}, Config{RecordWork: true, Concurrency: 2})
			assert.NoError(t, err)
			assert.Equal(t, len(isolated), 1)
			full[side] = isolated[0]
		}
		assert.Equal(t, string(clusterDiffForTest(t, pair)), string(clusterDiffForTest(t, full)))
	}
}

func clusterUnavailableTrees(t *testing.T, failedSide manifest.TreeSide) []Tree {
	t.Helper()
	trees := make([]Tree, 0, 6)
	for _, cluster := range []manifest.ClusterID{"broken", "empty", "healthy"} {
		for side := manifest.TreeBase; side <= manifest.TreeHead; side++ {
			root := t.TempDir()
			testutil.WriteFile(t, root, "entry/kustomization.yaml", "resources: [owner.yaml]\n")
			testutil.WriteFile(
				t,
				root,
				"entry/owner.yaml",
				testutil.KustomizationYAML("owner", "apps", "  path: ./app\n"),
			)
			builder := "resources: [cm.yaml]\n"
			if cluster == "broken" && side == failedSide {
				builder = "resources: [missing.yaml]\n"
			}
			if cluster == "empty" && side == failedSide {
				builder = "resources: []\n"
			}
			testutil.WriteFile(t, root, "app/kustomization.yaml", builder)
			testutil.WriteFile(
				t,
				root,
				"app/cm.yaml",
				fmt.Sprintf(
					"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: output, namespace: apps}\ndata: {value: '%d'}\n",
					side,
				),
			)
			trees = append(trees, Tree{
				ID:       manifest.TreeID{Cluster: cluster, Side: side},
				RepoRoot: root,
				Path:     filepath.Join(root, "entry"),
			})
		}
	}
	return trees
}

func TestRenderTrees_ClusterUnavailableAndEmpty(t *testing.T) {
	t.Parallel()
	for _, failedSide := range []manifest.TreeSide{manifest.TreeBase, manifest.TreeHead} {
		t.Run(fmt.Sprintf("failed_side_%d", failedSide), func(t *testing.T) {
			t.Parallel()
			trees := clusterUnavailableTrees(t, failedSide)
			runs, err := renderTreesForTest(t.Context(), t, trees, Config{RecordWork: true, Concurrency: 2})
			if !errors.Is(err, manifest.ErrFlux) || len(runs) != 6 {
				t.Fatalf("lost failed cluster: runs=%+v error=%v", runs, err)
			}
			for i := 0; i < len(runs); i += 2 {
				assertClusterUnavailablePair(t, runs[i:i+2], failedSide)
			}
		})
	}
}

func TestRenderTrees_FluxInstanceSyncOnlyChange(t *testing.T) {
	t.Parallel()
	trees := fluxInstanceSyncTrees(t)
	producer := testutil.HelmReleaseID("apps", "generator")
	instance := manifest.NamedResource{
		Group:     manifest.FluxOperatorDomain,
		Kind:      "FluxInstance",
		Namespace: "apps",
		Name:      "flux",
	}
	full := make([]Rendered, 2)
	for side, tree := range trees {
		isolated, err := renderTreesForTest(t.Context(), t, []Tree{tree}, Config{RecordWork: true, Concurrency: 2})
		assert.NoError(t, err)
		assert.Equal(t, len(isolated), 1)
		full[side] = isolated[0]
	}
	var first []byte
	for repeat := range 2 {
		runs, err := renderTreesForTest(t.Context(), t, trees, Config{RecordWork: true, Concurrency: 2})
		assert.NoError(t, err)
		assert.Equal(t, len(runs), 2)
		var templates manifest.WorkCounts
		for side, run := range runs {
			assert.NoError(t, run.Err)
			assert.Equal(
				t,
				manifest.DiagnosticFor(
					run.entryBinding.Producer,
					run.Store().GetObject(run.entryBinding.Producer),
				).Resource,
				instance,
			)
			assert.Equal(t, run.Store().EmissionOwner(instance), producer)
			documents := outputByParent(run.Result.Manifests)[run.entryBinding.Producer]
			found := false
			for _, document := range documents {
				if document["kind"] == "Kustomization" && document["metadata"].(map[string]any)["name"] == "workload" {
					found = true
					assert.Equal(t, document["spec"].(map[string]any)["interval"], any([]string{"1h", "2h"}[side]))
				}
			}
			assert.Equal(t, found, true)
			templates.Add(new(workCounts(run.Result.Work, producer, manifest.WorkHelm)))
			for recordIndex := range run.Result.Work {
				record := run.Result.Work[recordIndex]
				assert.Equal(t, record.Key.Tree, run.ID)
			}
		}
		assert.Equal(t, templates, manifest.WorkCounts{Attempts: 1, Completed: 1})
		body := clusterDiffForTest(t, runs)
		assert.Equal(t, string(body), string(clusterDiffForTest(t, full)))
		if repeat == 0 {
			first = body
		} else {
			assert.Equal(t, string(body), string(first))
		}
	}
}

func sharedClusterTrees(t *testing.T) []Tree {
	t.Helper()
	const selfURL = "https://fixture.invalid/three-clusters"
	app, other := testutil.KustomizationID("apps", "app"), testutil.KustomizationID("apps", "other")
	trees := make([]Tree, 0, 6)
	for side := manifest.TreeBase; side <= manifest.TreeHead; side++ {
		root := t.TempDir()
		testutil.WriteFile(
			t,
			root,
			"source.yaml",
			"apiVersion: source.toolkit.fluxcd.io/v1\nkind: GitRepository\n"+
				"metadata: {name: cluster, namespace: apps}\nspec: {url: "+selfURL+"}\n",
		)
		testutil.WriteFile(t, root, "shared/app/kustomization.yaml", "resources: [cm.yaml]\n")
		testutil.WriteFile(
			t,
			root,
			"shared/app/cm.yaml",
			"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: app-output, namespace: apps}\n"+
				"data: {cluster: '${CLUSTER}', value: "+[]string{"before", "after"}[side]+"}\n",
		)
		testutil.WriteFile(t, root, "private/app/kustomization.yaml", "resources: [cm.yaml]\n")
		testutil.WriteFile(
			t,
			root,
			"private/app/cm.yaml",
			"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: other-output, namespace: apps}\ndata: {value: unchanged}\n",
		)
		for _, cluster := range []manifest.ClusterID{"alpha", "omega", "third"} {
			name, path := app.Name, "shared/app"
			if cluster == "third" {
				name, path = other.Name, "private/app"
			}
			testutil.WriteFile(
				t,
				root,
				string(cluster)+"/kustomization.yaml",
				"resources: [app.yaml, ../source.yaml]\n",
			)
			testutil.WriteFile(
				t,
				root,
				string(cluster)+"/app.yaml",
				testutil.KustomizationYAML(
					name, "apps",
					"  path: ./"+path+"\n  sourceRef: {kind: GitRepository, name: cluster}\n"+
						"  postBuild: {substitute: {CLUSTER: "+string(cluster)+"}}\n",
				),
			)
			trees = append(trees, Tree{
				ID:       manifest.TreeID{Cluster: cluster, Side: side},
				RepoRoot: root,
				Path:     filepath.Join(root, string(cluster)),
				SelfURLs: []string{selfURL},
			})
		}
	}
	return trees
}

func assertClusterUnavailablePair(t *testing.T, pair []Rendered, failedSide manifest.TreeSide) {
	t.Helper()
	owner := testutil.KustomizationID("apps", "owner")
	projectedResults1, projectionErrors1 := Select(
		pair,
		errors.Join(pair[0].Err, pair[1].Err),
		[]manifest.NamedResource{owner},
		nil,
	)
	selected, selectErr, _ := projectedResults1, projectionErrors1.Selected, projectionErrors1.Remaining
	for side, run := range pair {
		assert.Equal(t, run.Absent, false)
		if run.Orchestrator == nil || run.Result == nil {
			t.Fatalf("present side lost its execution: %+v", run)
		}
		failed := run.ID.Cluster == "broken" && run.ID.Side == failedSide
		assert.Equal(t, run.Err != nil, failed)
		assert.Equal(t, len(selected[side].Unavailable) != 0, failed)
		want := manifest.WorkCounts{Attempts: 1, Completed: 1}
		if failed {
			want.Completed, want.Failures = 0, 1
		}
		assert.Equal(t, workCounts(run.Result.Work, owner, manifest.WorkKustomize), want)
		if run.ID.Cluster == "empty" && run.ID.Side == failedSide {
			assert.Equal(t, len(outputByParent(run.Result.Manifests)[owner]), 0)
		}
		for recordIndex := range run.Result.Work {
			record := run.Result.Work[recordIndex]
			assert.Equal(t, record.Key.Tree, run.ID)
		}
	}
	assert.Equal(t, selectErr != nil, pair[0].ID.Cluster == "broken")
	body, warnings, suppressed, compareErr := diff.Compare(
		diff.Snapshot{
			Subjects: selected[0].Subjects, Manifests: selected[0].Manifests,
			Unavailable: selected[0].Unavailable, ProducerKeys: selected[0].ProducerKeys,
		},
		diff.Snapshot{
			Subjects: selected[1].Subjects, Manifests: selected[1].Manifests,
			Unavailable: selected[1].Unavailable, ProducerKeys: selected[1].ProducerKeys,
		},
		nil,
		diff.Options{Format: diff.FormatJSON},
	)
	assert.NoError(t, compareErr)
	assert.Equal(t, len(warnings), 0)
	var report struct {
		Changes []diff.Change `json:"changes"`
	}
	assert.NoError(t, json.Unmarshal(body, &report))
	if pair[0].ID.Cluster == "broken" {
		assert.Equal(t, len(report.Changes), 0)
		assert.Equal(t, len(suppressed) > 0, true)
		return
	}
	assert.Equal(t, len(report.Changes), 1)
	assert.Equal(t, len(suppressed), 0)
	if pair[0].ID.Cluster == "empty" {
		status := "removed"
		if failedSide == manifest.TreeBase {
			status = "added"
		}
		assert.Equal(t, strings.Contains(string(body), `"status": "`+status+`"`), true)
	}
}

func assertSharedClusterWork(t *testing.T, run Rendered, raw map[string]manifest.WorkCounts) {
	t.Helper()
	app, other := testutil.KustomizationID("apps", "app"), testutil.KustomizationID("apps", "other")
	source := manifest.NamedResource{
		Group:     manifest.SourceDomain,
		Kind:      manifest.KindGitRepository,
		Namespace: "apps",
		Name:      "cluster",
	}
	assert.Equal(t, len(run.Result.Warnings), 0)
	var support manifest.WorkCounts
	for recordIndex := range run.Result.Work {
		record := run.Result.Work[recordIndex]
		assert.Equal(t, record.Key.Tree, run.ID)
		assert.Equal(t, record.Counts.RemoteTransfers, uint64(0))
		if record.Key.Phase != manifest.WorkKustomize {
			continue
		}
		if record.Key.Owner == (manifest.NamedResource{}) || record.Key.Owner.ReportHidden() {
			support.Add(&record.Counts)
		}
		if record.Key.Owner == app {
			assert.Equal(t, record.Key.Source, source)
			assert.Equal(t, record.Key.Input != "", true)
			counts := raw[record.Key.Input]
			counts.Add(&record.Counts)
			raw[record.Key.Input] = counts
		}
	}
	assert.Equal(t, support, manifest.WorkCounts{})
	assert.Equal(t, workCounts(run.Result.Work, other, manifest.WorkKustomize), manifest.WorkCounts{})
	assert.Equal(t, workCounts(run.Result.Work, manifest.NamedResource{}, manifest.WorkHelm), manifest.WorkCounts{})
	assert.Equal(
		t,
		workCounts(run.Result.Work, manifest.NamedResource{}, manifest.WorkEvaluation),
		manifest.WorkCounts{},
	)
	if run.ID.Cluster == "third" {
		assert.Equal(t, workCounts(run.Result.Work, app, manifest.WorkKustomize), manifest.WorkCounts{})
		assert.Equal(
			t,
			workCounts(run.Result.Work, manifest.NamedResource{}, manifest.WorkSubstitution),
			manifest.WorkCounts{},
		)
		return
	}
	docs := visibleWitnessOutput(run.Result)[app]
	assert.Equal(t, len(docs), 1)
	assert.Diff(t, docs[0]["data"], map[string]any{
		"cluster": string(run.ID.Cluster),
		"value":   []string{"before", "after"}[run.ID.Side],
	})
	assert.Equal(
		t,
		workCounts(run.Result.Work, app, manifest.WorkSubstitution),
		manifest.WorkCounts{Attempts: 1, Completed: 1},
	)
}

func TestRenderTrees_ThreeClusterSharedInput(t *testing.T) {
	t.Parallel()
	app := testutil.KustomizationID("apps", "app")
	trees := sharedClusterTrees(t)
	full := make(map[manifest.TreeID]*Result)
	for _, tree := range trees {
		isolated, err := renderTreesForTest(t.Context(), t, []Tree{tree}, Config{RecordWork: true, Concurrency: 2})
		assert.NoError(t, err)
		assert.Equal(t, len(isolated), 1)
		full[tree.ID] = isolated[0].Result
	}
	var first []byte
	for repeat := range 2 {
		runs, err := renderTreesForTest(t.Context(), t, trees, Config{RecordWork: true, Concurrency: 2})
		assert.NoError(t, err)
		assert.Equal(t, len(runs), 6)
		raw := map[string]manifest.WorkCounts{}
		outputs := make([]map[string][]map[string]any, 0, len(runs))
		for _, run := range runs {
			owners := []manifest.NamedResource{app}
			if run.ID.Cluster == "third" {
				owners = nil
			}
			got := visibleWitnessOutput(run.Result)
			assert.Diff(t, got, witnessOutput(full[run.ID], owners))
			assertSharedClusterWork(t, run, raw)
			encoded := make(map[string][]map[string]any, len(got))
			for id, documents := range got {
				encoded[id.String()] = documents
			}
			outputs = append(outputs, encoded)
		}
		assert.Equal(t, len(raw), 2)
		for _, counts := range raw {
			assert.Equal(t, counts, manifest.WorkCounts{Attempts: 1, Completed: 1})
		}
		body, err := json.Marshal(outputs)
		assert.NoError(t, err)
		if repeat == 0 {
			first = body
		} else {
			assert.Equal(t, string(body), string(first))
		}
	}
}
