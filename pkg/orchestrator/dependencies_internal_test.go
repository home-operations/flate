package orchestrator

import (
	"fmt"
	"io/fs"
	"strings"
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

//nolint:paralleltest // The test calls a helper that changes process-global state.
func TestRender_GeneratedValuesAndParentGates(t *testing.T) {
	var generatedName string
	for _, tc := range []struct {
		workers int
	}{{2}, {4}} {
		t.Run(fmt.Sprintf("workers_%d", tc.workers), func(t *testing.T) {
			testRenderGeneratedValuesAndParentGatesCase(
				t,
				&testRenderGeneratedValuesAndParentGatesCaseInputs{tc: &tc, generatedName: &generatedName},
			)
		})
	}
}

func TestRenderTrees_ExcludedOrderingAndPromotion(t *testing.T) {
	t.Parallel()

	for _, broken := range []bool{false, true} {
		t.Run(fmt.Sprintf("broken_source_%v", broken), func(t *testing.T) {
			t.Parallel()
			testRenderTreesExcludedOrderingAndPromotionCase(
				t,
				&broken,
			)
		},
		)
	}
	t.Run("selected malformed reference", func(t *testing.T) {
		t.Parallel()
		testRenderTreesExcludedOrderingAndPromotionSelectedMalformedReference(t)
	},
	)
}

func TestRenderTrees_UnknownOrderingTargetStillFails(t *testing.T) {
	t.Parallel()

	baseDir, headDir := t.TempDir(), t.TempDir()
	testutil.WriteFilteredOrderingCluster(t, baseDir, "v1", false)
	testutil.WriteFilteredOrderingCluster(t, headDir, "v2", false)
	for _, dir := range []string{baseDir, headDir} {
		testutil.WriteFile(t, dir, "flux/selected.yaml", ksYAML("selected", "apps/selected", "unknown"))
	}
	renderedTrees2, err := RenderTrees(
		t.Context(),
		[]Tree{
			{RepoRoot: baseDir, ID: manifest.TreeID{Cluster: ".", Side: manifest.TreeBase}},
			{RepoRoot: headDir, ID: manifest.TreeID{Cluster: ".", Side: manifest.TreeHead}},
		},
		Config{Concurrency: 2, CacheDir: t.TempDir()},
	)
	base, head := renderedTrees2[0], renderedTrees2[1]

	assert.FatalIf(t, err == nil, "%v", "selected unknown ordering target was ignored")

	for _, side := range []Rendered{base, head} {
		info := side.Result.Failed[testutil.KustomizationID("flux-system", "selected")]

		assert.FatalIf(
			t,
			info.Status != store.StatusFailed || !strings.Contains(info.String(), "unknown: dependency not found"),
			"missing target diagnosis lost: %+v",
			info,
		)
	}
}

func testRenderTreesExcludedOrderingAndPromotionCase(
	t *testing.T,
	testCaseInput *bool,
) {
	t.Helper()

	baseDir, headDir := t.TempDir(), t.TempDir()
	testutil.WriteFilteredOrderingCluster(t, baseDir, "v1", (*testCaseInput))
	testutil.WriteFilteredOrderingCluster(t, headDir, "v2", (*testCaseInput))
	for _, dir := range []string{baseDir, headDir} {
		testutil.WriteFile(
			t,
			dir,
			"apps/monitor-b/kustomization.yaml",
			"resources: [missing.yaml, hr.yaml, invalid.yaml]\n",
		)
		testutil.WriteFile(
			t,
			dir,
			"apps/monitor-b/invalid.yaml",
			"apiVersion: fluxcd.controlplane.io/v1\nkind: ResourceSet\nmetadata: {name: invalid, namespace: flux-system}\nspec:\n  dependsOn: [{apiVersion: example.test/, kind: Widget, name: missing}]\n",
		)
	}
	renderedTrees1, err := RenderTrees(
		t.Context(),
		[]Tree{
			{RepoRoot: baseDir, ID: manifest.TreeID{Cluster: ".", Side: manifest.TreeBase}},
			{RepoRoot: headDir, ID: manifest.TreeID{Cluster: ".", Side: manifest.TreeHead}},
		},
		Config{Concurrency: 4, CacheDir: t.TempDir()},
	)
	base, head := renderedTrees1[0], renderedTrees1[1]

	assert.FatalIf(
		t,
		(err != nil) != (*testCaseInput),
		"RenderTrees error=%v, want failure=%v",
		err,
		(*testCaseInput),
	)

	for _, side := range []Rendered{base, head} {
		assert.FatalIf(t, side.Result == nil, "%v", "snapshot result missing")

		st := side.Store()
		selected := testutil.HelmReleaseID("flux-system", "selected")
		assertExcludedOrderingTargets(t, &side, st)
		if *testCaseInput {
			source := manifest.NamedResource{
				Group:     "source.toolkit.fluxcd.io",
				Kind:      manifest.KindOCIRepository,
				Namespace: "flux-system",
				Name:      "selected-source",
			}
			assert.FatalIf(t, st.GetObject(source) == nil || side.Result.Failed[source].Status != store.StatusFailed ||
				side.Result.Failed[selected].Status != store.StatusFailed,

				"selected source failure hidden: objects=%v failures=%v",
				st.GetObject(source),
				side.Result.Failed)

			continue
		}
		assert.FatalIf(t, side.Err != nil || len(side.Result.Failed) != 0 || len(side.Result.Blocked) != 0,
			"excluded chain contaminated snapshot: error=%v failures=%v", side.Err, side.Result.Failed)

		assert.FatalIf(
			t,
			st.GetArtifact(
				testutil.KustomizationID("flux-system", "selected"),
				store.ArtifactContent,
			) == nil,
			"%v",
			"selected KS was skipped instead of reconciled",
		)

		chart := manifest.NamedResource{
			Group:     "source.toolkit.fluxcd.io",
			Kind:      manifest.KindHelmChart,
			Namespace: "flux-system",
			Name:      "shared-chart",
		}

		assert.FatalIf(
			t,
			st.GetObject(chart) == nil || !side.filter.ShouldReconcile(chart),
			"required shared chart was not admitted: %s",
			chart,
		)

		otherSource := manifest.NamedResource{
			Group:     "source.toolkit.fluxcd.io",
			Kind:      manifest.KindOCIRepository,
			Namespace: "flux-system",
			Name:      "selected-source",
		}

		{
			info, known := side.statusOf(
				otherSource,
			)
			assert.FatalIf(t, side.filter.ShouldReconcile(otherSource) ||
				(known || st.GetObject(otherSource) != nil) &&
					info.Outcome() != store.OutcomeUnchanged, "excluded source sibling was activated: %+v", info)
		}

		assert.FatalIf(
			t,
			st.GetArtifact(otherSource, store.ArtifactContent) != nil,
			"%v",
			"excluded source sibling was fetched",
		)

		for _, id := range []manifest.NamedResource{
			selected, testutil.KustomizationID("flux-system", "selected"),
			{Group: "source.toolkit.fluxcd.io", Kind: manifest.KindGitRepository, Namespace: "flux-system", Name: "shared"},
		} {
			assert.FatalIf(
				t,
				st.GetObject(id) == nil || !side.filter.ShouldReconcile(id),
				"selected source or consumer missing: %s",
				id,
			)

			{
				info, _ := st.GetStatus(id)
				assert.FatalIf(
					t,
					info.Status != store.StatusReady,
					"selected resource not Ready: %s %+v",
					id,
					info,
				)
			}
		}

		assert.FatalIf(t, st.GetObject(
			manifest.NamedResource{
				Kind:      manifest.KindConfigMap,
				Namespace: "flux-system",
				Name:      "selected-values",
			},
		) == nil, "%v", "required values data was not rendered")
	}
	testRenderTreesExcludedOrderingAndPromotionCaseTail(
		t,
		&testRenderTreesExcludedOrderingAndPromotionCaseTailInputs{
			testCaseInput: &testCaseInput,
			base:          &base,
			head:          &head,
		},
	)
}

func testRenderTreesExcludedOrderingAndPromotionCaseTail(
	t *testing.T,
	tailCaseInput *testRenderTreesExcludedOrderingAndPromotionCaseTailInputs,
) {
	t.Helper()
	if !(*(*tailCaseInput.testCaseInput)) {
		for i, side := range []Rendered{(*tailCaseInput.base), (*tailCaseInput.head)} {
			batches := canonicalBatches(side.Result)

			assert.FatalIf(
				t,
				len(batches) != 3 || batches[0].Parent != testutil.HelmReleaseID("flux-system", "selected") ||
					batches[1].Parent != side.entryBinding.Producer ||
					batches[2].Parent != testutil.KustomizationID(
						"flux-system",
						"selected",
					),
				"unexpected selected producer batches: %+v",
				batches,
			)

			var declarations []map[string]any
			for _, name := range []string{"selected", "monitor-a", "monitor-b", "source"} {
				body, err := fs.ReadFile(side.cfg.Input.FS, "flux/"+name+".yaml")
				assert.NoError(t, err)
				parsed, err := testutil.DecodeDocs(strings.NewReader(string(body)))
				assert.NoError(t, err)
				declarations = append(declarations, parsed...)
			}
			assert.Diff(t, batches[1].Documents, declarations)

			assert.FatalIf(
				t,
				len(batches[0].Documents) != 1 || len(batches[2].Documents) != 2,
				"unexpected selected document order: %+v",
				batches,
			)

			cm := batches[0].Documents[0]
			meta, _ := cm["metadata"].(map[string]any)
			data, _ := cm["data"].(map[string]any)
			want := "v1"
			if i == 1 {
				want = "v2"
			}

			assert.FatalIf(t, cm["kind"] != manifest.KindConfigMap || meta["name"] != "selected-rendered" ||
				data["greeting"] != want, "rendered chart values = %+v, want %s", cm, want)

			hrDoc, valuesDoc := batches[2].Documents[0], batches[2].Documents[1]
			valuesMeta, _ := valuesDoc["metadata"].(map[string]any)
			hrMeta, _ := hrDoc["metadata"].(map[string]any)
			spec, _ := hrDoc["spec"].(map[string]any)
			values, _ := spec["values"].(map[string]any)
			assert.FatalIf(t, valuesDoc["kind"] != manifest.KindConfigMap || valuesMeta["name"] != "selected-values" ||
				hrDoc["kind"] != manifest.KindHelmRelease ||
				hrMeta["name"] != "selected" ||
				values["greeting"] != want,
				"selected producer document order or values differ: %+v", batches[2].Documents)
		}
	}
}

func testRenderTreesExcludedOrderingAndPromotionSelectedMalformedReference(t *testing.T) {
	t.Helper()

	dir := t.TempDir()
	testutil.WriteFile(t, dir, "kustomization.yaml", "resources: [ks.yaml, peer.yaml, good.yaml]\n")
	testutil.WriteFile(t, dir, "ks.yaml", ksYAML("apps", "apps", ""))
	testutil.WriteFile(t, dir, "peer.yaml", ksYAML("peer", "broken", ""))
	testutil.WriteFile(t, dir, "good.yaml", ksYAML("good", "good", ""))
	testutil.WriteFile(t, dir, "good/kustomization.yaml", "resources: [cm.yaml]\n")
	testutil.WriteFile(t, dir, "good/cm.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: healthy}\n")
	testutil.WriteFile(t, dir, "broken/kustomization.yaml", "resources: [missing.yaml, ../apps/invalid.yaml]\n")
	testutil.WriteFile(t, dir, "apps/kustomization.yaml", "resources: [invalid.yaml]\n")
	testutil.WriteFile(
		t,
		dir,
		"apps/invalid.yaml",
		"apiVersion: fluxcd.controlplane.io/v1\nkind: ResourceSet\nmetadata: {name: invalid, namespace: flux-system}\nspec:\n  dependsOn: [{apiVersion: example.test/, kind: Widget, name: missing}]\n",
	)
	renderedSingleton1, err := RenderTrees(
		t.Context(),
		[]Tree{{Path: dir, RepoRoot: dir, ID: manifest.TreeID{Cluster: ".", Side: manifest.TreeHead}}},
		Config{Concurrency: 2, CacheDir: t.TempDir()},
	)

	assert.FatalIf(t, len(renderedSingleton1) != 1, "missing render: %v", err)

	o := renderedSingleton1[0].Orchestrator
	res := renderedSingleton1[0].Result

	assert.FatalIf(
		t,
		err == nil || !strings.Contains(err.Error(), "ResourceSet.fluxcd.controlplane.io/flux-system/invalid") ||
			strings.Count(
				err.Error(),
				"ResourceSet spec.dependsOn[0].apiVersion",
			) != 1,
		"selected malformed reference lost its source: %v",
		err,
	)

	{
		info := res.Failed[testutil.KustomizationID("flux-system", "peer")]
		assert.FatalIf(t, !strings.Contains(
			info.String(),
			"missing.yaml",
		), "input error overwrote the reader's render failure: %+v", info)
	}

	runs := []Rendered{{Orchestrator: o, Result: res, Err: err}}
	for _, name := range []string{"apps", "good"} {
		_, selectedErr, remainder := Select(
			runs,
			err,
			[]manifest.NamedResource{testutil.KustomizationID("flux-system", name)},
			nil,
		)

		assert.FatalIf(
			t,
			remainder != nil || (selectedErr != nil) != (name == "apps"),
			"selection %s: error=%v remainder=%v",
			name,
			selectedErr,
			remainder,
		)
	}
}

type testRenderTreesExcludedOrderingAndPromotionCaseTailInputs struct {
	testCaseInput **bool

	base *Rendered

	head *Rendered
}

func assertExcludedOrderingTargets(t *testing.T, side *Rendered, st *store.Store) {
	t.Helper()
	for _, id := range []manifest.NamedResource{
		testutil.KustomizationID("flux-system", "monitor-a"), testutil.KustomizationID("flux-system", "monitor-b"),
		testutil.HelmReleaseID("flux-system", "monitor-a"),
		testutil.HelmReleaseID("flux-system", "monitor-b"),
		{Kind: manifest.KindConfigMap, Namespace: "flux-system", Name: "excluded-values"},
	} {
		assert.ErrorIf(t, (id.Kind != manifest.KindKustomization && st.GetObject(id) != nil) ||
			side.filter.ShouldReconcile(id), "excluded sibling or ordering target admitted: %s", id)
	}
	excludedSource := manifest.NamedResource{
		Group:     "source.toolkit.fluxcd.io",
		Kind:      manifest.KindGitRepository,
		Namespace: "flux-system",
		Name:      "excluded-source",
	}

	{
		info, known := side.statusOf(
			excludedSource,
		)
		assert.FatalIf(t, side.filter.ShouldReconcile(excludedSource) ||
			(known || st.GetObject(excludedSource) != nil) &&
				info.Outcome() != store.OutcomeUnchanged, "discovered excluded source was activated: %+v", info)
	}

	assert.FatalIf(
		t,
		st.GetArtifact(excludedSource, store.ArtifactContent) != nil,
		"%v",
		"excluded source was fetched",
	)
}

type testRenderGeneratedValuesAndParentGatesTailInputs struct {
	o **Orchestrator
}

func testRenderGeneratedValuesAndParentGatesTail(
	t *testing.T,
	inputs *testRenderGeneratedValuesAndParentGatesTailInputs,
) {
	t.Helper()

	if info, _ := (*inputs.o).store.GetStatus(
		testutil.KustomizationID("flux-system", "apps"),
	); info.Status != store.StatusReady {
		t.Fatalf("enclosing KS status=%+v", info)
	}
}

type testRenderGeneratedValuesAndParentGatesCaseInputs struct {
	tc *struct {
		workers int
	}
	generatedName *string
}

func testRenderGeneratedValuesAndParentGatesCase(
	t *testing.T,
	inputs *testRenderGeneratedValuesAndParentGatesCaseInputs,
) {
	t.Helper()

	dir := t.TempDir()
	testutil.WriteGeneratedValuesCluster(t, dir)
	const cipher = "ENC[AES256_GCM,data:YWJj,iv:ZGVm,tag:Z2hp,type:str]"
	testutil.WriteFile(
		t,
		dir,
		"charts/app/templates/secret.yaml",
		"apiVersion: v1\nkind: Secret\nmetadata:\n  name: {{ .Release.Name }}-secret\n  namespace: flux-system\nstringData:\n  token: '"+cipher+"'\n",
	)
	o, err := newTestOrchestrator(
		t,
		Tree{Path: dir, RepoRoot: dir},
		Config{CacheDir: t.TempDir(), Concurrency: inputs.tc.workers},
	)
	assert.FatalIf(t, err != nil, "%s", err)
	assert.FatalIf(
		t,
		o.hrc.Store != o.store || o.hrc.Tasks != o.tasks,
		"%s",
		"HelmRelease base controller wiring changed",
	)

	if errInner := o.bootstrap(t.Context()); errInner != nil {
		t.Fatal(errInner)
	}
	a := testutil.HelmReleaseID("flux-system", "a")
	assert.FatalIf(t, o.store.GetObject(a) != nil ||
		parentsForTest(o.index)[a] != testutil.KustomizationID("flux-system", "apps"),
		"raw HR arrived or parent gate missing: %s parent=%s", a, parentsForTest(o.index)[a])

	res, err := o.collectResult(o.run(t.Context()))
	assert.FatalIf(t, err != nil || len(res.Failed) != 0 || len(res.Blocked) != 0,
		"Render err=%v failures=%v blockers=%v", err, res.Failed, res.Blocked)

	for _, name := range []string{"a", "b"} {
		id := testutil.HelmReleaseID("flux-system", name)
		hr, ok := o.store.Get[*manifest.HelmRelease](id)
		assert.FatalIf(t, !ok,
			"HR missing: %s", id)

		if name == "a" {
			assert.FatalIf(
				t,
				len(hr.ValuesFrom) != 1 || !strings.HasPrefix(hr.ValuesFrom[0].Name, "generated-values-"),
				"HR did not retain rewritten reference: %+v",
				hr,
			)

			ref := hr.ValuesFrom[0].Name
			assert.FatalIf(
				t,
				(*inputs.generatedName) != "" && ref != (*inputs.generatedName),
				"generated reference varies by entry or concurrency: %q != %q",
				ref,
				(*inputs.generatedName),
			)
			(*inputs.generatedName) = ref
			cmID := manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: id.Namespace, Name: ref}
			cm, okInner := o.store.Get[*manifest.ConfigMap](cmID)
			assert.FatalIf(t, !okInner || cm.Data["values.yaml"] != "greeting: hello\nreplicas: 2\n",
				"generated values unavailable: %+v", cm)
		}
		renderedID := manifest.NamedResource{
			Kind:      manifest.KindConfigMap,
			Namespace: id.Namespace,
			Name:      name + "-rendered",
		}
		rendered, ok := o.store.Get[*manifest.ConfigMap](renderedID)
		assert.FatalIf(t, !ok || rendered.Data["greeting"] != "hello" || rendered.Data["replicas"] != "2",
			"effective chart values differ: %+v", rendered)

		secretID := manifest.NamedResource{
			Kind:      manifest.KindSecret,
			Namespace: id.Namespace,
			Name:      name + "-secret",
		}
		secret, ok := o.store.Get[*manifest.Secret](secretID)
		assert.FatalIf(t, !ok,
			"emitted Secret missing: %s", secretID)

		{
			result1, err := secret.Value("token")
			value, present := result1.Value, result1.Found
			assert.FatalIf(t, err != nil || present || value != "",
				"encrypted token retained: %q %t %v", value, present, err)
		}

		if info, _ := o.store.GetStatus(id); info.Status != store.StatusReady {
			t.Fatalf("HR status=%+v", info)
		}
	}
	testRenderGeneratedValuesAndParentGatesTail(t, &testRenderGeneratedValuesAndParentGatesTailInputs{o: &o})
}
