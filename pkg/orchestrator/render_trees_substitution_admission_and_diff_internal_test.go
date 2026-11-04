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
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

func TestRenderTrees_SubstitutionAdmissionAndDiff(t *testing.T) {
	t.Parallel()

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
		WipeSecrets:         true,
		Supply:              []store.Declaration{source},
		StrictSubstitutions: true,
		Tree:                Tree{},
	}
	renderedTrees1, err := RenderTrees(
		t.Context(),
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
	consumer := testutil.KustomizationID("apps", "consumer")
	stable := testutil.KustomizationID("apps", "stable")
	for i, side := range []Rendered{base, head} {
		want := "supplied-base"
		if i == 1 {
			want = "supplied-head"
		}
		{
			got := substitutionValue(t, side.Result, consumer)
			assert.FatalIf(t, got != want, "changed consumer got %q, want %q", got, want)
		}
		assert.FatalIf(t, !side.filter.ShouldReconcile(consumer) || side.filter.ShouldReconcile(stable) ||
			side.Store().GetArtifact(stable, store.ArtifactContent) != nil ||
			!side.Store().
				ReportHidden(source.Obj.Named()),
			"%v", "selected-consumer admission or supplied-object isolation failed")
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

//nolint:paralleltest // The test calls a helper that changes process-global state.
func TestRender_SubstitutionChangesDoNotSelectConsumers(t *testing.T) {
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
		res, err := o.render(t.Context())
		assert.FatalIf(t, err != nil, "%v", err)
		id := testutil.KustomizationID("apps", "consumer")
		assert.FatalIf(
			t,
			o.filter.ShouldReconcile(id) || len(res.Manifests) != 0,
			"%v",
			"external input selected unchanged consumer",
		)
	}
}

func TestRenderTrees_SuppliedTransitionsMatchFull(t *testing.T) {
	t.Parallel()
	for _, transition := range [][2]string{{"absent", "real"}, {"wiped", "real"}, {"real", "absent"}, {"real", "wiped"}} {
		t.Run(strings.Join(transition[:], "-"), func(t *testing.T) {
			t.Parallel()
			testRenderTreesSuppliedTransitionsMatchFullCase(
				t,
				&transition,
			)
		},
		)
	}
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
	res, err := o.render(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	id := testutil.KustomizationID("apps", "consumer")
	if got := substitutionValue(t, res, id); got != "last-gone" {
		t.Fatalf("duplicate maps merged: %q", got)
	}
	if !maps.Equal(o.store.ReferenceFacts(last.Obj.Named(),
		nil,
		false,
		manifest.NamedResource{}).Data.(*manifest.ConfigMap).Data,
		last.Obj.(*manifest.ConfigMap).Data) {
		t.Fatal("accepted input differs from last whole object")
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
			if o.store.ReferenceFacts(id, nil, false, manifest.NamedResource{}).Data == nil ||
				!o.store.ReportHidden(id) {
				t.Fatal("indexed evidence changed supply or published it")
			}
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

func TestRenderTrees_RequiredPostBuildNotWaived(t *testing.T) {
	t.Parallel()
	for _, kind := range []string{manifest.KindSecret, manifest.KindConfigMap} {
		for _, allowed := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/allowed=%t", kind, allowed), func(t *testing.T) {
				t.Parallel()
				testRenderTreesRequiredPostBuildNotWaivedCase(
					t,
					testRenderTreesRequiredPostBuildNotWaivedCaseInputs{kind: &kind, allowed: &allowed},
				)
			},
			)
		}
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
		parsed, localErr := manifest.ParseDoc(
			map[string]any{
				"apiVersion": "kustomize.toolkit.fluxcd.io/v1",
				"kind":       "Kustomization",
				"metadata":   map[string]any{"name": name, "namespace": "apps"},
				"spec":       map[string]any{"path": "./" + name},
			},
			manifest.ParseDocOptions{},
		)
		obj := parsed.Object
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
	result, err := o.collectResult(o.run(t.Context()))
	if err != nil {
		t.Fatalf("deferred entry did not progress: %v; failed=%v blocked=%v",
			err,
			result.Failed,
			result.Blocked,
		)
	}
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

//nolint:paralleltest // The test calls a helper that changes process-global state.
func TestRender_RootSecretOverridesProducerPlaceholder(t *testing.T) {
	root := t.TempDir()
	testutil.WriteFile(t, root, "kustomization.yaml", "resources: [secret.yaml, producer.yaml, consumer.yaml]\n")
	testutil.WriteFile(
		t,
		root,
		"secret.yaml",
		"apiVersion: v1\nkind: Secret\nmetadata: {name: settings, namespace: apps}\nstringData: {VALUE: repository}\n",
	)
	testutil.WriteFile(
		t,
		root,
		"producer.yaml",
		"apiVersion: external-secrets.io/v1\nkind: ExternalSecret\nmetadata: {name: settings, namespace: apps}\nspec:\n  target: {name: settings}\n  data: [{secretKey: VALUE, remoteRef: {key: fixture}}]\n",
	)
	testutil.WriteFile(
		t,
		root,
		"consumer.yaml",
		substitutionKS("consumer", "leaf", "  postBuild: {substituteFrom: [{kind: Secret, name: settings}]}\n"),
	)
	testutil.WriteFile(t, root, "leaf/kustomization.yaml", "resources: [cm.yaml]\n")
	testutil.WriteFile(
		t,
		root,
		"leaf/cm.yaml",
		"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: rendered, namespace: apps}\ndata: {value: '${VALUE}'}\n",
	)
	input := &manifest.Secret{Name: "settings", Namespace: "apps", StringData: map[string]any{"VALUE": "private-value"}}
	o := newSubstitutionOrchestrator(t,
		treeConfig{
			Path:     root,
			RepoRoot: root,
			Supply: []store.Declaration{{
				Doc:    testutil.ObjectDocument(input),
				Obj:    input,
				Origin: manifest.SuppliedDeclaration,
			}},
		})
	assert.NoError(t, o.bootstrap(t.Context()))
	secret := input.Named()
	if o.store.GetObject(secret) != input &&
		o.store.GetObject(secret).(manifest.Data).HasKeys() == false ||
		o.store.EmissionOwner(secret) != (manifest.NamedResource{}) {
		t.Fatal("root-owned Secret published before its producer")
	}
	record := o.index.Records[secret]
	before, ok := record.Declaration.(*manifest.Secret)
	if !ok || before.StringData["VALUE"] != "repository" || record.BuilderOwner != o.entryBinding.Producer {
		t.Fatalf("root-owned Secret lost immutable entry evidence: %+v", record)
	}
	result, err := o.collectResult(o.run(t.Context()))
	assert.NoError(t, err)
	assert.Equal(t, substitutionValue(t, result, testutil.KustomizationID("apps", "consumer")), "repository")
	after, ok := o.store.GetObject(secret).(*manifest.Secret)
	if !ok || after.StringData["VALUE"] != "repository" {
		t.Fatalf("runtime repository precedence lost: %+v", after)
	}
	assert.Equal(t, o.store.EmissionOwner(secret), o.entryBinding.Producer)
	_, _, inventory := o.store.GetEmission(o.entryBinding.Producer)
	inventory.Children, inventory.Complete = nil, true
	o.store.RecordEmitted(o.entryBinding.Producer, &inventory, nil)
	inventory.Finish()
	o.store.DeleteObject(secret)
	facts := o.store.ReferenceFacts(secret, nil, false, manifest.NamedResource{})
	if facts.Data == nil {
		t.Fatal("repository data removed the invocation supplied snapshot")
	}
	valueResult, err := facts.Data.Value("VALUE")
	supplied := valueResult.Value
	assert.NoError(t, err)
	assert.Equal(t, supplied, "private-value")
	assert.Equal(t, input.StringData["VALUE"], "private-value")
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
	runs, runErr := RenderTrees(t.Context(), trees, Config{Concurrency: 2})
	assert.FatalIf(t, len(runs) != 2, "runs=%v err=%v", runs, runErr)
	consumer := testutil.KustomizationID("flux-system", "consumer")
	selected, selectedErr, remainder := Select(
		runs,
		runErr,
		[]manifest.NamedResource{testutil.KustomizationID("", "")},
		nil,
	)
	for _, run := range runs {
		membershipResult1 := run.filter.Membership(consumer)
		needed, output, _, _ := membershipResult1.Needed, membershipResult1.Selected, membershipResult1.Dirty, membershipResult1.Revision
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

func testRenderTreesSuppliedTransitionsMatchFullCase(
	t *testing.T,
	testCaseInput *[2]string,
) {
	t.Helper()

	roots := []string{t.TempDir(), t.TempDir()}
	for i, root := range roots {
		writeSubstitutionCluster(t, root, "settings", "${VALUE}")
		if testCaseInput[i] != "absent" {
			value := "repository"
			if testCaseInput[i] == "wiped" {
				value = manifest.ValuePlaceholder("VALUE")
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
		WipeSecrets: true,
		Supply:      []store.Declaration{suppliedSubstitution("settings")},
	}
	consumer := testutil.KustomizationID("apps", "consumer")
	for range 2 {
		paired, err := RenderTrees(
			t.Context(),
			[]Tree{
				{RepoRoot: roots[0], ID: manifest.TreeID{Cluster: ".", Side: manifest.TreeBase}},
				{RepoRoot: roots[1], ID: manifest.TreeID{Cluster: ".", Side: manifest.TreeHead}},
			},
			cfg,
		)
		assert.NoError(t, err)
		for i, root := range roots {
			full, err := RenderTrees(
				t.Context(),
				[]Tree{{RepoRoot: root, ID: manifest.TreeID{Cluster: ".", Side: manifest.TreeHead}}},
				cfg,
			)
			assert.NoError(t, err)
			want := "supplied"
			if testCaseInput[i] == "real" {
				want = "repository"
			}
			assert.Equal(t, substitutionValue(t, full[0].Result, consumer), want)
			assert.Equal(t, substitutionValue(t, paired[i].Result, consumer), want)
			origin := map[string]manifest.DeclarationOrigin{"absent": manifest.SuppliedDeclaration, "real": manifest.RepositoryDeclaration, "wiped": manifest.RepositoryDeclaration}[testCaseInput[i]]
			id := cfg.Supply[0].Obj.Named()
			assert.Equal(t, paired[i].Store().Policy(id).Origin, origin)
			assert.Equal(t, full[0].Store().Policy(id).Origin, origin)
			var wantWarnings []manifest.Warning
			if testCaseInput[i] == "real" {
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
				assert.Equal(t, len(paired[i].Result.ClusterWarnings), 0)
			} else {
				assert.Diff(
					t,
					paired[i].Result.ClusterWarnings,
					[]manifest.Warning{
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
}

type testRenderTreesRequiredPostBuildNotWaivedCaseInputs struct {
	kind    *string
	allowed *bool
}

func testRenderTreesRequiredPostBuildNotWaivedCase(
	t *testing.T,
	testCaseInput testRenderTreesRequiredPostBuildNotWaivedCaseInputs,
) {
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
				"  postBuild: {substituteFrom: [{kind: "+(*testCaseInput.kind)+", name: absent}]}\n",
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
	cfg := Config{Concurrency: 2, WipeSecrets: true}
	if *testCaseInput.allowed {
		cfg.AllowMissingKinds = []string{
			manifest.KindSecret,
			manifest.KindConfigMap,
			manifest.KindCustomResourceDefinition,
		}
	}
	runs, err := RenderTrees(t.Context(), trees, cfg)
	assert.FatalIf(t, err == nil || len(runs) != 2, "paired required input admitted: %+v %v", runs, err)
	for _, run := range runs {
		id := testutil.KustomizationID("apps", "consumer")
		assert.FatalIf(t, run.Result == nil || run.Result.Failed[id].Status != store.StatusFailed ||
			len(
				outputByParent(run.Result.Manifests)[id],
			) != 0, "required input admitted on side %v: %+v", run.ID, run.Result)
	}
}
