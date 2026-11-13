package orchestrator

import (
	"errors"
	"fmt"
	"maps"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/loader"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

func TestRenderTrees_SubstitutionAdmissionAndDiff(t *testing.T) {
	t.Parallel()
	for _, regime := range []string{"missing", "shallow"} {
		t.Run("dependency namespaces/"+regime, func(t *testing.T) {
			t.Parallel()
			testDependencyNamespaceHistory(t, regime)
		})
	}

	baseRoot, headRoot := t.TempDir(), t.TempDir()
	for i, root := range []string{baseRoot, headRoot} {
		expression := "${VALUE}-base"
		if i == 1 {
			expression = "${VALUE}-head"
		}
		writeSubstitutionCluster(t, root, "external", expression)
		testutil.WriteFile(t, root, "kustomization.yaml", "resources: [flux.yaml, stable.yaml]\n")
		testutil.WriteFile(
			t,
			root,
			"stable.yaml",
			substitutionKS(
				"stable",
				"stable",
				"  postBuild:\n    substituteFrom: [{kind: ConfigMap, name: external}]\n",
			),
		)
		testutil.WriteFile(t, root, "stable/kustomization.yaml", "resources: [cm.yaml]\n")
		testutil.WriteFile(
			t,
			root,
			"stable/cm.yaml",
			"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: stable, namespace: apps}\ndata: {value: '${VALUE}'}\n",
		)
	}
	source := suppliedSubstitution("external")
	cfg := treeConfig{
		CacheDir:            t.TempDir(),
		Concurrency:         4,
		Supply:              []store.Declaration{source},
		StrictSubstitutions: true,
		Tree:                Tree{},
	}
	renderedTrees1, err := renderTreesForTest(t.Context(), t,
		[]Tree{
			{
				RepoRoot:        baseRoot,
				ID:              manifest.TreeID{Cluster: ".", Side: manifest.TreeBase},
				Input:           cfg.BaseInput,
				BaseInput:       cfg.Input,
				ExternalChanges: cfg.ExternalChanges,
			},
			{
				RepoRoot:        headRoot,
				ID:              manifest.TreeID{Cluster: ".", Side: manifest.TreeHead},
				Input:           cfg.Input,
				BaseInput:       cfg.BaseInput,
				ExternalChanges: cfg.ExternalChanges,
			},
		},
		cfg.Config,
	)
	base, head := renderedTrees1[0], renderedTrees1[1]
	assert.FatalIf(t, err != nil, "%v", err)
	source.Obj.(*manifest.ConfigMap).Data["VALUE"] = "caller-change"
	source.Doc["data"].(map[string]any)["VALUE"] = "caller-change"
	consumer := testutil.KustomizationID("apps", "consumer")
	stable := testutil.KustomizationID("apps", "stable")
	for i, side := range []Rendered{base, head} {
		for _, input := range side.Orchestrator.cfg.Supply {
			assert.Equal(t, input.Obj.(*manifest.ConfigMap).Data["VALUE"], any("supplied"))
			assert.Equal(t, input.Doc["data"].(map[string]any)["VALUE"], any("supplied"))
		}
		want := "supplied-base"
		if i == 1 {
			want = "supplied-head"
		}
		{
			got := substitutionValue(t, side.Result, consumer)
			assert.FatalIf(t, got != want, "changed consumer got %q, want %q", got, want)
		}
		assert.FatalIf(
			t,
			!side.selection.Snapshot().Reconcile(consumer) || side.selection.Snapshot().Reconcile(stable) ||
				side.Store().GetArtifact(stable, store.ArtifactContent) != nil ||
				!side.Store().
					ReportHidden(source.Obj.Named()),
			"%v",
			"selected-consumer admission or supplied-object isolation failed",
		)
	}
	for _, side := range []Rendered{base, head} {
		batches := slices.DeleteFunc(canonicalBatches(side.Result), func(batch struct {
			Parent    manifest.NamedResource `json:"Parent"`
			Documents []map[string]any       `json:"Documents"`
		},
		) bool {
			return batch.Parent == side.entryBinding.Producer
		})
		assert.FatalIf(
			t,
			len(batches) != 1 || batches[0].Parent != consumer || len(batches[0].Documents) != 1,
			"unexpected producer batches: %+v",
			batches,
		)
		doc := batches[0].Documents[0]
		meta, _ := doc["metadata"].(map[string]any)
		assert.FatalIf(
			t,
			doc["kind"] != manifest.KindConfigMap || meta["name"] != "rendered",
			"supplied input entered output: %+v",
			doc,
		)
	}
}

func TestRender_SubstitutionChangesDoNotSelectConsumers(t *testing.T) {
	t.Parallel()
	baseRoot, headRoot := t.TempDir(), t.TempDir()
	for _, root := range []string{baseRoot, headRoot} {
		writeSubstitutionCluster(t, root, "external", "${VALUE}")
	}
	for _, value := range []string{"first", "second"} {
		source := suppliedSubstitution("external")
		source.Obj.(*manifest.ConfigMap).Data["VALUE"] = value
		source.Doc = testutil.ObjectDocument(source.Obj)
		o := newSubstitutionOrchestrator(
			t,
			treeConfig{
				Supply:    []store.Declaration{source},
				Path:      headRoot,
				BaseInput: testDirectoryInput(t, baseRoot),
			},
		)
		res, err := o.render(t.Context(), t)
		assert.FatalIf(t, err != nil, "%v", err)
		id := testutil.KustomizationID("apps", "consumer")
		assert.FatalIf(
			t,
			o.selection.Snapshot().Reconcile(id) || len(res.Manifests) != 0,
			"%v",
			"external input selected unchanged consumer",
		)
	}
}

func TestRenderTrees_SuppliedTransitionsMatchFull(t *testing.T) {
	t.Parallel()
	for _, transition := range [][2]string{{"absent", "real"}, {"empty", "real"}, {"real", "absent"}, {"real", "empty"}} {
		t.Run(strings.Join(transition[:], "-"), func(t *testing.T) {
			t.Parallel()
			testRenderTreesSuppliedTransitionsMatchFullCase1(t, &transition)
		})
	}
}

func testRenderTreesSuppliedTransitionsMatchFullCase1(t *testing.T, transition *[2]string) {
	t.Helper()

	roots := []string{t.TempDir(), t.TempDir()}
	for i, root := range roots {
		writeSubstitutionCluster(t, root, "settings", "${VALUE}")
		if transition[i] != "absent" {
			value := "repository"
			if transition[i] == "empty" {
				value = "ENC[AES256_GCM,data:x,iv:y,tag:z,type:str]"
			}
			testutil.WriteFile(
				t,
				root,
				"settings.yaml",
				"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: settings, namespace: apps}\ndata: {VALUE: '"+value+"'}\n",
			)
			testutil.WriteFile(t, root, "kustomization.yaml", "resources: [flux.yaml, settings.yaml]\n")
		}
	}
	cfg := Config{
		CacheDir:    t.TempDir(),
		Concurrency: 2,
		Supply:      []store.Declaration{suppliedSubstitution("settings")},
	}
	consumer := testutil.KustomizationID("apps", "consumer")
	for range 2 {
		testRenderTreesSuppliedTransitionsMatchFullCase1Part1(
			t,
			&testRenderTreesSuppliedTransitionsMatchFullCase1Part1Inputs{
				roots:      &roots,
				cfg:        &cfg,
				transition: &transition,
				consumer:   &consumer,
			},
		)
	}
}

func testRenderTreesSuppliedTransitionsMatchFullCase1Part1(
	t *testing.T, inputs *testRenderTreesSuppliedTransitionsMatchFullCase1Part1Inputs,
) {
	t.Helper()
	paired, err := renderTreesForTest(t.Context(), t,
		[]Tree{
			{RepoRoot: (*inputs.roots)[0], ID: manifest.TreeID{Cluster: ".", Side: manifest.TreeBase}},
			{RepoRoot: (*inputs.roots)[1], ID: manifest.TreeID{Cluster: ".", Side: manifest.TreeHead}},
		}, (*inputs.cfg),
	)
	assert.NoError(t, err)
	testRenderTreesSuppliedTransitionsMatchFullCase1Part1Tail(
		t,
		&testRenderTreesSuppliedTransitionsMatchFullCase1Part1TailInputs{
			roots:      &inputs.roots,
			cfg:        &inputs.cfg,
			transition: &inputs.transition,
			consumer:   &inputs.consumer,
			paired:     &paired,
		},
	)
}

//nolint:paralleltest // The test calls a helper that changes process-global state.
func TestBootstrap_SubstitutionDuplicatesReplaceWholeObjects(t *testing.T) {
	root := t.TempDir()
	writeSubstitutionCluster(t, root, "external", "${VALUE}-${OLD:-gone}")
	first := suppliedSubstitution("external")
	first.Obj.(*manifest.ConfigMap).Data["OLD"] = "old"
	last := suppliedSubstitution("external")
	last.Obj.(*manifest.ConfigMap).Data["VALUE"] = "last"
	last.Doc = testutil.ObjectDocument(last.Obj)
	first.Doc = testutil.ObjectDocument(first.Obj)
	o := newSubstitutionOrchestrator(t, treeConfig{Supply: []store.Declaration{first, last}, Path: root})
	res, err := o.render(t.Context(), t)
	assert.FatalIf(t, err != nil, "%s", err)

	id := testutil.KustomizationID("apps", "consumer")
	if got := substitutionValue(t, res, id); got != "last-gone" {
		t.Fatalf("duplicate maps merged: %q", got)
	}
	assert.FatalIf(t, !maps.Equal(
		o.store.ReferenceFacts(last.Obj.Named(), nil, manifest.NamedResource{}).Data.(*manifest.ConfigMap).Data,
		last.Obj.(*manifest.ConfigMap).Data,
	), "%s", "accepted input differs from last whole object")

	for _, encryptedLast := range []bool{false, true} {
		t.Run(fmt.Sprintf("encrypted_last=%t", encryptedLast), func(t *testing.T) {
			encrypted := suppliedSubstitution("external")
			encrypted.Obj.(*manifest.ConfigMap).Data = map[string]any{
				"Z":     "ENC[AES256_GCM,data:x,iv:y,tag:z,type:str]",
				"A":     "ENC[AES256_GCM,data:x,iv:y,tag:z,type:str]",
				"EMPTY": "",
			}
			child, outcome, _ := loader.Prepare(
				manifest.DocOccurrence{Document: testutil.ObjectDocument(encrypted.Obj)},
				manifest.ParseDocOptions{RequiredIdentity: true},
			)
			assert.NoError(t, outcome.Err)
			encrypted.Obj, encrypted.Doc, encrypted.Warnings = child.Obj, child.Doc, child.Warnings
			inputs := []store.Declaration{encrypted, last}
			if encryptedLast {
				inputs = []store.Declaration{last, encrypted}
			}
			current := newSubstitutionOrchestrator(t, treeConfig{Supply: inputs, Path: t.TempDir()})
			data := current.store.ReferenceFacts(last.Obj.Named(), nil, manifest.NamedResource{}).Data
			want := last.Obj.(*manifest.ConfigMap).Data
			if encryptedLast {
				want = map[string]any{"EMPTY": ""}
			}
			assert.Diff(t, data.(*manifest.ConfigMap).Data, want)
			warnings := current.store.Warnings()
			count := 0
			if encryptedLast {
				count = 1
			}
			assert.Equal(t, len(warnings), count)
			if encryptedLast {
				assert.Equal(t, warnings[0].Count, 1)
				assert.Equal(t, warnings[0].Category, manifest.WarnSOPSUnreadable)
				assert.Diff(t, warnings[0].Detail, []string{"A", "Z"})
			}
		})
	}
}

//nolint:paralleltest // The test calls a helper that changes process-global state.
func TestSupply_IndexedIdentityDoesNotPublish(t *testing.T) {
	for _, namespace := range []string{"apps", "", "other"} {
		t.Run("namespace_"+namespace, func(t *testing.T) {
			source := suppliedSubstitution("external")
			o := newSubstitutionOrchestrator(t, treeConfig{Supply: []store.Declaration{source}, Path: t.TempDir()})
			id := source.Obj.Named()
			indexed := id
			indexed.Namespace = namespace
			o.store.SetDiscoveryIndex(
				&manifest.DiscoveryIndex{
					Records: map[manifest.NamedResource]manifest.DiscoveryRecord{indexed: {Path: "repository.yaml"}},
				},
			)
			assert.FatalIf(t, o.store.ReferenceFacts(id, nil, manifest.NamedResource{}).Data == nil ||
				!o.store.ReportHidden(id), "%s", "indexed evidence changed supply or published it")
		})
	}
}

func canonicalBatches(res *Result) []struct {
	Parent    manifest.NamedResource `json:"Parent"`
	Documents []map[string]any       `json:"Documents"`
} {
	var batches []struct { //nolint:prealloc // Empty batches must remain nil when encoded as JSON.
		Parent    manifest.NamedResource `json:"Parent"`
		Documents []map[string]any       `json:"Documents"`
	}
	for _, id := range slices.SortedFunc(maps.Keys(outputByParent(res.Manifests)), manifest.NamedResource.Compare) {
		batches = append(batches, struct {
			Parent    manifest.NamedResource `json:"Parent"`
			Documents []map[string]any       `json:"Documents"`
		}{Parent: id, Documents: outputByParent(res.Manifests)[id]})
	}
	return batches
}

func TestRenderTrees_RequiredPostBuildPermission(t *testing.T) {
	t.Parallel()
	for _, kind := range []string{manifest.KindSecret, manifest.KindConfigMap} {
		for _, allowed := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/allowed=%t", kind, allowed), func(t *testing.T) {
				t.Parallel()
				testRenderTreesRequiredPostBuildPermissionCase1(t, &kind, &allowed)
			})
		}
	}
}

func testRenderTreesRequiredPostBuildPermissionCase1(t *testing.T, kind *string, allowed *bool) {
	t.Helper()

	roots := []string{t.TempDir(), t.TempDir()}
	trees := make([]Tree, 2)
	for i, root := range roots {
		testutil.WriteFile(
			t,
			root,
			"flux.yaml",
			substitutionKS(
				"consumer",
				"app",
				"  postBuild: {substituteFrom: [{kind: "+(*kind)+", name: absent}]}\n",
			),
		)
		testutil.WriteFile(t, root, "app/kustomization.yaml", "resources: [cm.yaml]\n")
		testutil.WriteFile(
			t,
			root,
			"app/cm.yaml",
			fmt.Sprintf(
				"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: rendered}\ndata: {value: '${VALUE}-%d'}\n",
				i,
			),
		)
		trees[i] = Tree{RepoRoot: root, ID: manifest.TreeID{Cluster: ".", Side: manifest.TreeSide(i)}}
	}
	cfg := Config{Concurrency: 2}
	if *allowed {
		cfg.AllowMissingKinds = []string{
			manifest.KindSecret,
			manifest.KindConfigMap,
			manifest.KindCustomResourceDefinition,
		}
	}
	runs, err := renderTreesForTest(t.Context(), t, trees, cfg)
	if *allowed {
		assert.NoError(t, err)
		assert.Equal(t, len(runs), 2)
		for _, run := range runs {
			id := testutil.KustomizationID("apps", "consumer")
			assert.NoError(t, run.Err)
			assert.FatalIf(t, run.Result == nil, "missing result on side %v", run.ID)
			assert.Equal(t, len(run.Result.Failed), 0)
			output := outputByParent(run.Result.Manifests)[id]
			assert.Equal(t, len(output), 1)
			assert.Diff(t, output[0]["data"], map[string]any{"value": fmt.Sprintf("${VALUE}-%d", run.ID.Side)})
			warnings := run.Orchestrator.store.Warnings()
			assert.Equal(t, len(warnings), 1)
			assert.Equal(t, warnings[0].Category, manifest.WarnUnresolvedSubstitution)
			assert.Equal(t, warnings[0].Count, 1)
		}
		return
	}
	assert.FatalIf(t, err == nil || len(runs) != 2,
		"paired required input admitted: %+v %v", runs, err)

	for _, run := range runs {
		id := testutil.KustomizationID("apps", "consumer")
		assert.FatalIf(t, run.Result == nil || run.Result.Failed[id].Status != store.StatusFailed ||
			len(outputByParent(run.Result.Manifests)[id]) != 0,
			"required input admitted on side %v: %+v", run.ID, run.Result)
	}
}

//nolint:paralleltest // The test calls a helper that changes process-global state.
func TestRender_SubstitutionDeferredEntry(t *testing.T) {
	root := t.TempDir()
	testutil.WriteFile(t, root, "entry/kustomization.yaml", "resources: [root.yaml, child.yaml, source.yaml]\n")
	testutil.WriteFile(
		t,
		root,
		"entry/root.yaml",
		substitutionKS("root", "entry", "  postBuild: {substituteFrom: [{kind: ConfigMap, name: settings}]}\n"),
	)
	testutil.WriteFile(
		t,
		root,
		"entry/child.yaml",
		substitutionKS(
			"${NAME}",
			"leaf",
			"  sourceRef: {kind: GitRepository, name: '${SOURCE}'}\n  dependsOn: [{name: '${DEP}'}]\n",
		),
	)
	testutil.WriteFile(
		t,
		root,
		"entry/source.yaml",
		"apiVersion: source.toolkit.fluxcd.io/v1\nkind: GitRepository\nmetadata: {name: '${SOURCE}', namespace: apps}\nspec: {url: https://fixture.invalid/self}\n",
	)
	testutil.WriteFile(
		t,
		root,
		"data/kustomization.yaml",
		"namespace: apps\nconfigMapGenerator: [{name: settings, literals: [NAME=effective, SOURCE=late-self, DEP=ready]}]\ngeneratorOptions: {disableNameSuffixHash: true}\n",
	)
	testutil.WriteFile(t, root, "ready/kustomization.yaml", "resources: []\n")
	testutil.WriteFile(t, root, "leaf/kustomization.yaml", "resources: [cm.yaml]\n")
	testutil.WriteFile(
		t,
		root,
		"leaf/cm.yaml",
		"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: workload, namespace: apps}\n",
	)
	o, err := newTestOrchestrator(
		t,
		Tree{Path: filepath.Join(root, "entry"), RepoRoot: root, SelfURLs: []string{"https://fixture.invalid/self"}},
		Config{Concurrency: 2},
	)
	assert.NoError(t, err)
	for _, name := range []string{"data", "ready"} {
		result1, localErr := manifest.ParseDoc(
			map[string]any{
				"apiVersion": "kustomize.toolkit.fluxcd.io/v1",
				"kind":       "Kustomization",
				"metadata":   map[string]any{"name": name, "namespace": "apps"},
				"spec":       map[string]any{"path": "./" + name},
			},
			manifest.ParseDocOptions{},
		)
		obj := result1.Object

		assert.NoError(t, localErr)
		o.store.AddObject(obj)
	}
	assert.NoError(t, o.bootstrap(t.Context()))
	owner := testutil.KustomizationID("apps", "root")
	assert.Equal(t, o.entryBinding.Producer, owner)
	child := testutil.KustomizationID("apps", "effective")
	assert.FatalIf(
		t,
		o.store.GetObject(child) != nil,
		"%v",
		"deferred entry published an effective child during discovery",
	)
	result, err := o.collectResult(t.Context(), o.run(t.Context()))
	t.Cleanup(result.Close)
	assert.FatalIf(t, err != nil,
		"deferred entry did not progress: %v; failed=%v blocked=%v",
		err,
		result.Failed,
		result.Blocked)

	effective, ok := o.store.GetObject(child).(*manifest.Kustomization)
	assert.FatalIf(t, !ok || effective.SourceID().Name != "late-self" || len(effective.DependsOn) != 1 ||
		effective.DependsOn[0].Name != "ready", "effective graph was not admitted together: %+v", effective)
	assert.FatalIf(
		t,
		o.store.GetArtifact(effective.SourceID(), store.ArtifactContent) == nil,
		"%v",
		"late self-source was not admitted",
	)
	assert.FatalIf(t, o.store.EmissionOwner(child) != owner, "%v", "effective child lost entry publication ownership")
	emission, ok := o.store.GetArtifact(owner, store.ArtifactContent).(*store.Emission)
	assert.FatalIf(
		t,
		!ok || !emission.Complete || emission.Render == nil,
		"%v",
		"effective emission has no accepted render receipt",
	)
	assert.FatalIf(t, len(outputByParent(result.Manifests)[child]) != 1, "%v", "effective child did not render")
}

func TestRenderTrees_IgnoredRequiredSubstitution(t *testing.T) {
	t.Parallel()
	roots := [2]string{t.TempDir(), t.TempDir()}
	trees := make([]Tree, 2)
	for side, root := range roots {
		testutil.WriteFile(
			t,
			root,
			"flux/consumer.yaml",
			"apiVersion: kustomize.toolkit.fluxcd.io/v1\nkind: Kustomization\nmetadata: {name: consumer, namespace: flux-system}\nspec:\n interval: 10m\n path: ./app\n sourceRef: {kind: GitRepository, name: flux-system, namespace: flux-system}\n postBuild:\n  substituteFrom: [{kind: ConfigMap, name: vars}]\n",
		)
		testutil.WriteFile(
			t,
			root,
			"flux/vars.yaml",
			"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: vars, namespace: flux-system}\ndata:\n FLAG: old\n",
		)
		testutil.WriteFile(t, root, "app/kustomization.yaml", "resources: [cm.yaml]\n")
		testutil.WriteFile(
			t,
			root,
			"app/cm.yaml",
			"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: consumed, namespace: apps}\ndata:\n flag: ${FLAG}\n",
		)
		ignore := ""
		if side == 1 {
			ignore = "vars.yaml\n"
		}
		testutil.WriteFile(t, root, "flux/.krmignore", ignore)
		trees[side] = Tree{
			ID:       manifest.TreeID{Cluster: ".", Side: manifest.TreeSide(side)},
			RepoRoot: root,
			Path:     filepath.Join(root, "flux"),
		}
	}
	runs, runErr := renderTreesForTest(t.Context(), t, trees, Config{Concurrency: 2})
	assert.FatalIf(t, len(runs) != 2, "runs=%v err=%v", runs, runErr)
	consumer := testutil.KustomizationID("flux-system", "consumer")
	projectedResults0, projectionErrors0 := Select(
		runs,
		runErr,
		[]manifest.NamedResource{testutil.KustomizationID("", "")},
		nil,
	)
	selected, selectedErr, remainder := projectedResults0, projectionErrors0.Selected, projectionErrors0.Remaining
	for _, run := range runs {
		adaptedResult1 := run.selection.Snapshot()
		needed, output := adaptedResult1.Reconcile(consumer), adaptedResult1.Output(consumer)
		status, present := run.Store().GetStatus(consumer)
		t.Logf(
			"side=%d needed=%t selected=%t status=%s present=%t rawFailure=%v selectedFailure=%v rawError=%v selectedError=%v",
			run.ID.Side,
			needed,
			output,
			status.Status,
			present,
			run.Result.Failed[consumer],
			selected[run.ID.Side].Failed[consumer],
			run.Err,
			selectedErr,
		)
		assert.Equal(t, needed, true)
		assert.Equal(t, output, true)
	}
	assert.Equal(t, runs[1].Result.Failed[consumer].Status, store.StatusFailed)
	assert.Equal(t, selected[1].Failed[consumer], runs[1].Result.Failed[consumer])
	assert.Equal(t, len(outputByParent(runs[1].Result.Manifests)[consumer]), 0)
	assert.Diff(
		t,
		selected[1].Blocked[consumer],
		[]manifest.NamedResource{{Kind: manifest.KindConfigMap, Namespace: "flux-system", Name: "vars"}},
	)
	assert.FatalIf(t, !errors.Is(selectedErr, manifest.ErrFlux), "selected failure lost: %v", selectedErr)
	assert.NoError(t, remainder)
}

type testRenderTreesSuppliedTransitionsMatchFullCase1Part1TailInputs struct {
	roots **[]string

	cfg **Config

	transition ***[2]string

	consumer **manifest.
			NamedResource

	paired *[]Rendered
}

func testRenderTreesSuppliedTransitionsMatchFullCase1Part1Tail(
	t *testing.T,
	inputs *testRenderTreesSuppliedTransitionsMatchFullCase1Part1TailInputs,
) {
	t.Helper()

	for i, root := range *(*inputs.roots) {
		full, err := renderTreesForTest(t.Context(), t,
			[]Tree{{RepoRoot: root, ID: manifest.TreeID{Cluster: ".", Side: manifest.TreeHead}}}, (*(*inputs.cfg)),
		)
		assert.NoError(t, err)
		want := "supplied"
		if (*(*inputs.transition))[i] == "real" {
			want = "repository"
		}
		assert.Equal(t, substitutionValue(t, full[0].Result, (*(*inputs.consumer))), want)
		assert.Equal(t, substitutionValue(t, (*inputs.paired)[i].Result, (*(*inputs.consumer))), want)
		origin := manifest.SuppliedDeclaration
		if (*(*inputs.transition))[i] != "absent" {
			origin = manifest.RepositoryDeclaration
		}
		id := (*inputs.cfg).Supply[0].Obj.Named()
		assert.Equal(t, (*inputs.paired)[i].Store().Policy(id).Origin, origin)
		assert.Equal(t, full[0].Store().Policy(id).Origin, origin)
		for _, result := range []*Result{(*inputs.paired)[i].Result, full[0].Result} {
			testSuppliedOutputData(t, result, id, (*(*inputs.transition))[i] == "real")
		}
		var wantWarnings []manifest.Warning
		if (*(*inputs.transition))[i] == "real" {
			wantWarnings = []manifest.Warning{
				{
					Category: manifest.WarnSuppliedPrecedence,
					Message:  "repository data takes precedence over supplied data",
					Detail:   []string{"ConfigMap/apps/settings: VALUE (ci-values.yaml)"},
					Count:    1,
				},
			}
		}
		assert.Diff(t, full[0].Result.ClusterWarnings, wantWarnings)
		if i == 0 {
			assert.Equal(t, len((*inputs.paired)[i].Result.ClusterWarnings), 0)
		} else {
			assert.Diff(
				t, (*inputs.paired)[i].Result.ClusterWarnings, []manifest.Warning{
					{
						Category: manifest.WarnSuppliedPrecedence,
						Message:  "repository data takes precedence over supplied data",
						Detail:   []string{"ConfigMap/apps/settings: VALUE (ci-values.yaml)"},
						Count:    1,
					},
				},
			)
		}
	}
}

type testRenderTreesSuppliedTransitionsMatchFullCase1Part1Inputs struct {
	roots      *[]string
	cfg        *Config
	transition **[2]string
	consumer   *manifest.NamedResource
}

func testSuppliedOutputData(t *testing.T, result *Result, id manifest.NamedResource, readable bool) {
	t.Helper()
	for _, documents := range outputByParent(result.Manifests) {
		for _, document := range documents {
			if manifest.DocIdentity(document) != id {
				continue
			}
			data, _ := document["data"].(map[string]any)
			if readable {
				assert.Equal(t, data["VALUE"], any("repository"))
			} else {
				_, present := data["VALUE"]
				assert.Equal(t, present, false)
			}
		}
	}
}
