package orchestrator

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"testing/fstest"

	"github.com/home-operations/flate/internal/engine/change"
	"github.com/home-operations/flate/internal/engine/controllers/base"
	"github.com/home-operations/flate/internal/engine/kustomize"
	"github.com/home-operations/flate/internal/engine/loader"
	"github.com/home-operations/flate/internal/engine/schedule"
	"github.com/home-operations/flate/internal/engine/selection"
	"github.com/home-operations/flate/internal/engine/source"
	"github.com/home-operations/flate/internal/engine/store"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/assert"
	"github.com/home-operations/flate/pkg/manifest"
)

func TestRenderTrees_ArtifactInputCurrentSelection(t *testing.T) {
	t.Parallel()
	generator := manifest.NamedResource{
		Group:     "source.extensions.fluxcd.io",
		Kind:      "ArtifactGenerator",
		Namespace: "apps",
		Name:      "generator",
	}
	artifact := manifest.NamedResource{
		Group:     manifest.SourceDomain,
		Kind:      manifest.KindExternalArtifact,
		Namespace: "apps",
		Name:      "generated",
	}
	consumer := testutil.KustomizationID("apps", "consumer")
	trees := make([]Tree, 0, 2)
	var full [2]*Result
	for side, value := range []string{"old", "new"} {
		tree := artifactWorkTree(t, value, manifest.TreeSide(side))
		trees = append(trees, tree)
		runs, err := renderTreesForTest(t.Context(), t, []Tree{tree}, Config{RecordWork: true, Concurrency: 2})
		assert.NoError(t, err)
		full[side] = runs[0].Result
		assert.Equal(
			t,
			workCounts(full[side].Work, consumer, manifest.WorkKustomize),
			manifest.WorkCounts{Attempts: 1, Completed: 1},
		)
		assert.Equal(
			t,
			workCounts(full[side].Work, generator, manifest.WorkEvaluation),
			manifest.WorkCounts{Attempts: 1, Completed: 1},
		)
		assert.Equal(
			t,
			workCounts(
				full[side].Work,
				generator,
				manifest.WorkAcquisition,
			),
			manifest.WorkCounts{
				Attempts:          1,
				Completed:         1,
				LocalAcquisitions: 1,
			},
		)
		assert.Equal(t, runs[0].store.EmissionOwner(artifact), generator)
		payload, ok := runs[0].store.GetArtifact(artifact, store.ArtifactContent).(*store.SourceArtifact)
		if !ok {
			t.Fatal("full render did not publish generated content")
		}
		assert.Equal(t, payload.Key != "", true)
		for recordIndex := range full[side].Work {
			record := full[side].Work[recordIndex]
			assert.Equal(t, record.Key.Tree, tree.ID)
			assert.Equal(t, record.Counts.RemoteTransfers, uint64(0))
			if record.Key.Owner == consumer && record.Key.Phase == manifest.WorkKustomize {
				assert.Equal(t, record.Key.Source, artifact)
			}
			if record.Key.Owner == generator &&
				(record.Key.Phase == manifest.WorkEvaluation || record.Key.Phase == manifest.WorkAcquisition) {
				assert.Equal(t, record.Key.Source, manifest.NamedResource{
					Group: manifest.SourceDomain, Kind: manifest.KindGitRepository, Namespace: "apps", Name: "missing",
				})
			}
			if record.Key.Owner == generator && record.Key.Phase == manifest.WorkAcquisition {
				assert.Equal(t, record.Key.Input != "" && record.Key.Input != payload.Key, true)
			}
		}
		docs := outputByParent(full[side].Manifests)[consumer]
		assert.Equal(t, len(docs), 1)
		assert.Diff(t, docs[0]["data"], map[string]any{"value": value})
	}
	assertArtifactSelection(t, trees, full)
	for _, tc := range []struct {
		name    string
		missing manifest.TreeSide
	}{
		{name: "removal", missing: manifest.TreeHead},
		{name: "reverse removal", missing: manifest.TreeBase},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assertArtifactRemoval(t, tc.missing, generator, artifact)
		})
	}
}

func TestRenderTrees_ArtifactInputUnreadCopy(t *testing.T) {
	t.Parallel()
	generator := manifest.NamedResource{
		Group:     "source.extensions.fluxcd.io",
		Kind:      "ArtifactGenerator",
		Namespace: "apps",
		Name:      "generator",
	}
	trees := make([]Tree, 2)
	for side, value := range []string{"old", "new"} {
		trees[side] = artifactWorkTree(t, "old", manifest.TreeSide(side))
		testutil.WriteFile(t, trees[side].RepoRoot, "app/shared/unread.txt", value)
	}
	runs, err := renderTreesForTest(t.Context(), t, trees, Config{RecordWork: true, Concurrency: 2})
	assert.NoError(t, err)
	for _, run := range runs {
		for _, name := range []string{"consumer", "second", "unrelated"} {
			id := testutil.KustomizationID("apps", name)
			assert.Equal(t, workCounts(run.Result.Work, id, manifest.WorkKustomize), manifest.WorkCounts{})
		}
		assert.Equal(t, workCounts(run.Result.Work, generator, manifest.WorkAcquisition).Completed, uint64(1))
	}
}

func assertArtifactRemoval(
	t *testing.T, missing manifest.TreeSide, generator, artifact manifest.NamedResource,
) {
	t.Helper()
	consumer := testutil.KustomizationID("apps", "consumer")
	ctx := t.Context()
	r, err := NewInvocation(ctx, Config{CacheDir: t.TempDir(), RecordWork: true, Concurrency: 2})
	assert.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, r.Close()) })
	var trees [2]Tree
	for side := range trees {
		tree := artifactWorkTree(t, "old", manifest.TreeSide(side))
		if manifest.TreeSide(side) == missing {
			generatorPath := filepath.Join(tree.Path, "generator.yaml")
			body, readErr := os.ReadFile(generatorPath)
			assert.NoError(t, readErr)
			assert.NoError(t, os.Remove(generatorPath))
			testutil.WriteFile(t, tree.RepoRoot, "factory/generator.yaml",
				strings.Replace(string(body), "name: generated", "name: replacement", 1))
			testutil.WriteFile(t, tree.RepoRoot, "factory/kustomization.yaml", "resources: [generator.yaml]\n")
			testutil.WriteFile(
				t, tree.RepoRoot, "flux/factory.yaml",
				testutil.KustomizationYAML("factory", "apps", "  path: ./factory\n"),
			)
		}
		trees[side] = tree
	}
	inputs := source.NewHTTPInputs(r.Config.Cache, r.Config.Network, r.budget)
	t.Cleanup(inputs.Close)
	r.trees, err = r.prepareTrees(ctx, trees[:], &inputs)
	assert.NoError(t, err)
	r.results = make([]Rendered, len(r.trees))
	r.stores = make([]*store.Store, len(r.trees))
	r.byID = make(map[manifest.TreeID]int, len(r.trees))
	t.Cleanup(r.stopRemaining)
	r.discoverTrees(ctx)
	r.Config.projections.Clear()
	assert.NoError(t, r.stopFailedDiscovery())
	peer := r.results[missing].Orchestrator
	assert.FatalIf(t, !peer.loader.IsFileIndexed(generator), "missing-side generator is absent from discovery")
	assert.Equal(t, peer.store.GetObject(generator), manifest.BaseManifest(nil))
	assert.Equal(t, peer.store.GetArtifact(artifact, store.ArtifactContent), store.Artifact(nil))

	present := 1 - int(missing)
	assert.NoError(t, r.qualifyReaders(ctx, 0))
	r.prepareSession(ctx, present)
	assert.NoError(t, r.results[present].Err)
	assert.FatalIf(
		t,
		r.stores[present].GetArtifact(artifact, store.ArtifactContent) == nil,
		"present side did not publish output",
	)
	assert.NoError(t, r.qualifyReaders(ctx, 0))
	comparison, generated := peer.cfg.ExternalChanges.CompareSource(artifact, missing, nil, nil)
	assert.Equal(t, generated, true)
	assert.Equal(t, comparison, change.Comparison{})

	pair := change.SourcePair{
		Stores:  [2]*store.Store{r.stores[0], r.stores[1]},
		Indexes: [2]*manifest.DiscoveryIndex{r.results[0].index, r.results[1].index},
	}
	control := &artifactInventoryControl{
		reconcileController: peer.controllers.ArtifactGenerator,
		before: func(ctx context.Context) {
			_, _, partial := peer.store.GetEmission(generator)
			defer partial.Finish()
			peer.store.RecordEmitted(generator, &partial, nil)
			inventory, ok := peer.store.GetArtifact(generator, store.ArtifactContent).(*store.Emission)
			if !ok || inventory.Complete {
				t.Error("expected a current partial inventory")
			}
			paired, qualifyErr := change.Qualify(ctx, nil, pair, manifest.WorkScope{})
			if qualifyErr != nil {
				t.Error(qualifyErr)
				return
			}
			read, isGenerated := paired.CompareSource(artifact, missing, nil, nil)
			assert.Equal(t, isGenerated, true)
			assert.Equal(t, read, change.Comparison{})
		},
	}
	peer.controllers.ArtifactGenerator = control
	assert.NoError(t, r.constructTrees(ctx))
	peer.controllers.ArtifactGenerator = control.reconcileController
	assert.Equal(t, control.called, true)
	assert.FatalIf(t, peer.store.GetObject(generator) == nil, "discovery did not load the peer generator")
	inventory, ok := peer.store.GetArtifact(generator, store.ArtifactContent).(*store.Emission)
	assert.FatalIf(
		t,
		!ok || !inventory.Complete || !inventory.OutputReads.Valid(),
		"expected current complete inventory",
	)
	comparison, generated = peer.cfg.ExternalChanges.CompareSource(artifact, missing, nil, nil)
	assert.Equal(t, generated, true)
	assert.Equal(t, comparison, change.Comparison{Known: true, Changed: true})

	r.completeDiscovery(ctx)
	assert.NoError(t, r.qualifyDeclarations(ctx))
	r.installAdmission(ctx)
	r.renderTrees(ctx)
	for side, run := range r.results {
		want := manifest.WorkCounts{Attempts: 1, Completed: 1}
		docs := outputByParent(run.Result.Manifests)[consumer]
		status, _ := r.stores[side].GetStatus(consumer)
		if manifest.TreeSide(side) == missing {
			assert.Equal(t, run.Err != nil, true)
			assert.Equal(
				t, workCounts(run.Result.Work, generator, manifest.WorkEvaluation),
				manifest.WorkCounts{Attempts: 2, Completed: 2},
			)
			assert.Equal(t, workCounts(run.Result.Work, consumer, manifest.WorkKustomize), manifest.WorkCounts{})
			assert.Equal(t, len(docs), 0)
			assert.Equal(t, status.Outcome(), store.OutcomeBlocked)
			assert.Diff(t, status.BlockedBy(), []manifest.NamedResource{artifact})
			continue
		}
		assert.NoError(t, run.Err)
		assert.Equal(t, workCounts(run.Result.Work, generator, manifest.WorkEvaluation), want)
		assert.Equal(t, workCounts(run.Result.Work, consumer, manifest.WorkKustomize), want)
		assert.Equal(t, len(docs), 1)
		assert.Diff(t, docs[0]["data"], map[string]any{"value": "old"})
		assert.Equal(t, status.Outcome(), store.OutcomePassed)
	}
}

type artifactInventoryControl struct {
	reconcileController

	before func(context.Context)
	once   sync.Once
	called bool
}

func (c *artifactInventoryControl) ReconcileNode(
	ctx context.Context, id manifest.NamedResource, level manifest.DrainLevel, previous schedule.Result,
) *base.Execution {
	c.once.Do(func() {
		c.called = true
		c.before(ctx)
	})
	return c.reconcileController.ReconcileNode(ctx, id, level, previous)
}

func artifactWorkTree(t *testing.T, value string, side manifest.TreeSide) Tree {
	t.Helper()
	files := map[string]string{
		"flux/generator.yaml": "apiVersion: source.extensions.fluxcd.io/v1beta1\nkind: ArtifactGenerator\n" +
			"metadata: {name: generator, namespace: apps}\nspec:\n  sources: [{alias: " +
			"repo, kind: GitRepository, name: missing}]\n  artifacts:\n    - name: gen" +
			"erated\n      copy: [{from: '@repo/app/', to: '@artifact/app/'}]\n",

		"flux/consumer.yaml": testutil.KustomizationYAML(
			"consumer",
			"apps",
			"  path: ./app/app/package\n  sourceRef: {kind: ExternalArtifact, name: generated}\n",
		),

		"flux/second.yaml": testutil.KustomizationYAML(
			"second", "apps", "  path: ./app/app/second\n  sourceRef: {kind: ExternalArtifact, name: generated}\n",
		),

		"flux/unrelated.yaml": testutil.KustomizationYAML(
			"unrelated", "apps",
			"  path: ./app/app/unrelated\n  sourceRef: {kind: ExternalArtifact, name: generated}\n",
		),

		"flux/stable.yaml": testutil.KustomizationYAML("stable", "apps", "  path: ./stable\n"),

		"app/package/kustomization.yaml": "resources: [../shared]\n",
		"app/second/kustomization.yaml":  "resources: [../shared]\nnameSuffix: -second\n",
		"app/shared/kustomization.yaml":  "resources: [cm.yaml]\n",
		"app/shared/cm.yaml": "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: generated-output, names" +
			"pace: apps}\ndata: {value: " + value + "}\n",

		"app/unrelated/kustomization.yaml": "resources: [cm.yaml]\n",
		"app/unrelated/cm.yaml":            "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: unrelated-output, namespace: apps}\n",

		"stable/kustomization.yaml": "resources: [cm.yaml]\n",

		"stable/cm.yaml": "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: stable-output, namespace: apps}\n",
	}
	root := writeWitness(t, files)
	return Tree{ID: manifest.TreeID{Cluster: ".", Side: side}, RepoRoot: root, Path: filepath.Join(root, "flux")}
}

func assertArtifactSelection(t *testing.T, trees []Tree, full [2]*Result) {
	t.Helper()
	stable := testutil.KustomizationID("apps", "stable")
	consumer := testutil.KustomizationID("apps", "consumer")
	second := testutil.KustomizationID("apps", "second")
	unrelated := testutil.KustomizationID("apps", "unrelated")
	generator := manifest.NamedResource{
		Group:     "source.extensions.fluxcd.io",
		Kind:      "ArtifactGenerator",
		Namespace: "apps",
		Name:      "generator",
	}
	artifact := manifest.NamedResource{
		Group:     manifest.SourceDomain,
		Kind:      manifest.KindExternalArtifact,
		Namespace: "apps",
		Name:      "generated",
	}
	var first []byte
	for repeat := range 2 {
		runs, err := renderTreesForTest(t.Context(), t, trees, Config{RecordWork: true, Concurrency: 2})
		assert.NoError(t, err)
		outputs := make([][]manifest.DocumentBatch, 0, len(runs))
		for _, run := range runs {
			assert.Diff(
				t,
				visibleWitnessOutput(run.Result),
				witnessOutput(full[run.ID.Side], []manifest.NamedResource{consumer, second, generator}),
			)
			assert.Equal(t, len(outputByParent(run.Result.Manifests)[stable]), 0)
			for _, id := range []manifest.NamedResource{stable, unrelated} {
				assert.Equal(t, workCounts(run.Result.Work, id, manifest.WorkKustomize), manifest.WorkCounts{})
			}
			for _, id := range []manifest.NamedResource{consumer, second} {
				assert.Equal(
					t, workCounts(run.Result.Work, id, manifest.WorkKustomize),
					manifest.WorkCounts{Attempts: 1, Completed: 1},
				)
			}
			assert.Equal(
				t,
				workCounts(run.Result.Work, generator, manifest.WorkEvaluation),
				manifest.WorkCounts{Attempts: 1, Completed: 1},
			)
			assert.Equal(
				t,
				workCounts(run.Result.Work, generator, manifest.WorkAcquisition),
				manifest.WorkCounts{Attempts: 1, Completed: 1, LocalAcquisitions: 1},
			)
			assert.Equal(t, run.store.GetArtifact(artifact, store.ArtifactContent) != nil, true)
			for recordIndex := range run.Result.Work {
				record := run.Result.Work[recordIndex]
				assert.Equal(t, record.Key.Tree, run.ID)
				assert.Equal(t, record.Counts.RemoteTransfers, uint64(0))
			}
			outputs = append(outputs, run.Result.Manifests)
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

func TestPromotionChildren_KeepEachPackageOwnerAndSource(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	testutil.WriteFile(t, root, "app/kustomization.yaml", "resources: []\n")
	input := &store.SourceArtifact{Provider: store.ProviderInput, LocalPath: root, FS: os.DirFS(root)}
	st := store.New()
	st.SetArtifact(manifest.BootstrapSourceID, input)
	l := loader.New(st)
	l.SourceRoot = root
	app := filepath.Join(root, "app")
	_, err := l.Load(t.Context(), app, manifest.NamedResource{}, nil)
	assert.NoError(t, err)
	want := map[manifest.NamedResource]store.Child{}
	for _, ns := range []string{"first", "second"} {
		gitSource := &manifest.GitRepository{Name: "input", Namespace: ns}
		st.AddObject(gitSource)
		st.SetArtifact(gitSource.Named(), input)
		owner := &manifest.Kustomization{Name: "package", Namespace: ns, Path: "./app"}
		owner.SourceRef.Kind, owner.SourceRef.Name = gitSource.Named().Kind, gitSource.Name
		st.AddObject(owner)
		support := new(store.ReadSet)
		support.Init(st, owner.Named(), nil)
		support.GetArtifact(gitSource.Named(), store.ArtifactContent)
		support.GetDeclaration(owner.Named())
		t.Cleanup(support.Retire)
		child := &manifest.HelmRelease{Name: "demo", Namespace: ns}
		assert.NoError(t, l.RegisterBuilt(t.Context(), loader.BuiltInput{
			Owner: owner.Named(), Source: gitSource.Named(), Root: app, Support: support,
		}, []store.Child{{Obj: child}}))
		want[child.Named()] = store.Child{
			Obj: child,
			Productions: []manifest.Production{
				{Owner: owner.Named(), Source: gitSource.Named(), File: "app/kustomization.yaml"},
			},
		}
		assert.Equal(t, st.GetDeclaration(child.Named()), nil)
	}
	o := &Orchestrator{loader: l, store: st, index: l.Index}
	for range 2 {
		got := o.promotionChildren()
		assert.Equal(t, len(got), len(want))
		for id, child := range want {
			assert.Equal(t, got[id].Obj, child.Obj)
			assert.Diff(t, got[id].Productions, child.Productions)
		}
	}
}

func TestKustomizeInputPair_RetiresReplacedDeclarations(t *testing.T) {
	t.Parallel()
	artifact := (&manifest.ExternalArtifact{Name: "generated", Namespace: "apps"}).Named()
	before := &manifest.Kustomization{Name: "reader", Namespace: "apps", Path: "app"}
	after := &manifest.Kustomization{Name: "reader", Namespace: "apps", Path: "previous"}
	for _, ks := range []*manifest.Kustomization{before, after} {
		ks.SourceRef.Kind, ks.SourceRef.Name = artifact.Kind, artifact.Name
	}
	var facts selection.SourceFacts
	facts.Source, facts.Context.Side = artifact, manifest.TreeHead
	facts.Snapshots[0] = &store.SourceArtifact{Content: "base", FS: fstest.MapFS{
		"app/kustomization.yaml": {Data: []byte("resources: [../inputs/removed.yaml]")},
		"inputs/removed.yaml":    {},
	}}
	facts.Snapshots[1] = &store.SourceArtifact{Content: "head", FS: fstest.MapFS{
		"previous/kustomization.yaml": {Data: []byte("resources: [../inputs/previous.yaml]")},
		"next/kustomization.yaml":     {Data: []byte("resources: [../inputs/next.yaml]")},
		"inputs/previous.yaml":        {},
		"inputs/next.yaml":            {},
	}}
	id := before.Named()
	var pair kustomizeInputPair
	for side, ks := range []*manifest.Kustomization{before, after} {
		pair[side] = &kustomizeInputSide{
			store:   store.New(),
			indexed: map[manifest.NamedResource]kustomizeInputDeclaration{id: {object: ks}},
			prepared: map[manifest.NamedResource]*kustomize.LocalInputs{
				id: kustomize.ObserveLocalInputs(t.Context(), &kustomize.RenderRequest{
					Source: facts.Snapshots[side], Spec: ks, SubPath: ks.Path,
				}),
			},
		}
	}
	paths, complete := pair.read(t.Context(), id, nil, &facts)
	assert.Equal(t, complete, true)
	assert.Diff(t, paths[0].Files, []string{"app/kustomization.yaml", "inputs/removed.yaml"})
	assert.Diff(t, paths[1].Files, []string{"inputs/previous.yaml", "previous/kustomization.yaml"})

	replacement := after.Clone()
	replacement.Path = "missing"
	paths, complete = pair.read(t.Context(), id, replacement, &facts)
	assert.Equal(t, complete, false)
	assert.Diff(t, paths[0].Files, []string{"app/kustomization.yaml", "inputs/removed.yaml"})
	assert.Equal(t, len(paths[1].Files), 0)

	successor := replacement.Clone()
	successor.Path = "next"
	paths, complete = pair.read(t.Context(), id, successor, &facts)
	assert.Equal(t, complete, true)
	assert.Diff(t, paths[1].Files, []string{"inputs/next.yaml", "next/kustomization.yaml"})

	guard := &manifest.ConfigMap{Name: "input", Namespace: "apps"}
	pair[0].store.AddObject(guard)
	var reads store.ReadSet
	reads.Init(pair[0].store, artifact, nil)
	defer reads.Retire()
	reads.GetObject(guard.Named())
	facts.Snapshots[0] = facts.Snapshots[0].WithReads(&reads)
	pair[0].store.AddObject(&manifest.ConfigMap{
		Name: "input", Namespace: "apps", Data: map[string]any{"value": "new"},
	})
	paths, complete = pair.read(t.Context(), id, successor, &facts)
	assert.Equal(t, complete, false)
	assert.Equal(t, len(paths[0].Files), 0)
	assert.Diff(t, paths[1].Files, []string{"inputs/next.yaml", "next/kustomization.yaml"})
}
