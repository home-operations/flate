package orchestrator

import (
	"context"
	"errors"
	"slices"
	"sync"
	"testing"

	"github.com/google/go-cmp/cmp"

	"github.com/home-operations/flate/pkg/controllers/base"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/helm"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/schedule"
	"github.com/home-operations/flate/pkg/selection"
	"github.com/home-operations/flate/pkg/source"
	sourcecache "github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/source/helmchart"
	"github.com/home-operations/flate/pkg/source/oci"
	"github.com/home-operations/flate/pkg/store"
	"github.com/home-operations/flate/pkg/task"
)

func TestRenderTrees_EntryPublicationSourcePreparation(t *testing.T) {
	t.Parallel()
	beforeDir, afterDir := excludedOrderingFixture(t, true)
	invocation := prepareOrderingTrees(t, []Tree{
		{RepoRoot: beforeDir, ID: manifest.TreeID{Cluster: ".", Side: manifest.TreeBase}},
		{RepoRoot: afterDir, ID: manifest.TreeID{Cluster: ".", Side: manifest.TreeHead}},
	}, &Config{RecordWork: true, Concurrency: 2, CacheDir: t.TempDir()})
	o := invocation.results[1].Orchestrator
	o.configureControllers()
	id := manifest.NamedResource{
		Group:     manifest.SourceDomain,
		Kind:      manifest.KindOCIRepository,
		Namespace: "flux-system",
		Name:      "selected-source",
	}
	entry := manifest.NewEntryKustomization(manifest.BootstrapSourceID, ".", manifest.NamedResource{}).Named()
	obj := o.store.GetObject(id)
	beforeFacts := o.loader.SourceFacts(manifest.BootstrapSourceID, nil)
	beforeSourceFacts := o.loader.SourceFacts(id, nil)
	beforeIndex := o.loader.SelectionIndex()
	beforeRecord := beforeIndex.Records[id]
	beforeProductions := slices.Clone(o.store.Productions(id, beforeIndex))
	_, beforeDoc, beforeOutput := o.store.GetEmission(id)
	beforeOutput.Finish()
	beforeRefs := slices.Collect(manifest.References(obj, manifest.ReferenceRelations))
	var caller store.ReadSet
	caller.Init(o.store, id, nil)
	defer caller.Close()
	var work manifest.WorkRecorder
	caller.SetWork(&work)
	var preparation *selection.Preparation
	defer func() { preparation.Retire() }()
	proposal, err := o.selection.Prepare(
		t.Context(),
		selection.PrepareRequest{
			Missing:     o.cfg.missing,
			Reads:       &caller,
			Preparation: &preparation,
			Generation:  []store.Child{{Obj: obj, Origin: o.store.Policy(id).Origin}},
		},
	)
	assert.NoError(t, err)
	assert.Diff(
		t,
		work.Scope().Admission().Counts,
		manifest.WorkCounts{Attempts: 1, Completed: 1, OwnerPrepares: 1, FileEntries: 2},
	)
	pendingRevision := proposal.Revision
	done := make(chan *base.Execution, 1)
	o.tasks.Go(t.Context(), "entry publisher", func(ctx context.Context) {
		done <- o.ksc.ReconcileNode(ctx, entry, manifest.DrainNone, schedule.Result{})
	})
	execution := <-done
	o.tasks.BlockTillDone()
	defer execution.Retire()
	current := o.store.GetObject(id)
	afterFacts := o.loader.SourceFacts(manifest.BootstrapSourceID, nil)
	afterSourceFacts := o.loader.SourceFacts(id, nil)
	_, afterDoc, afterOutput := o.store.GetEmission(id)
	afterOutput.Finish()
	assert.Equal(t, manifest.Equal(obj, current), true)
	assert.Equal(t, manifest.ValueEqual(beforeDoc, afterDoc), true)
	assert.Equal(t, o.loader.SelectionIndex() == beforeIndex, true)
	assert.Diff(t, o.loader.SelectionIndex().Records[id], beforeRecord)
	afterProductions := o.store.Productions(id, beforeIndex)
	assert.Equal(t, len(beforeProductions), 2)
	assert.Equal(t, len(afterProductions), 3)
	assert.Equal(t, slices.Contains(afterProductions, beforeProductions[0]), true)
	assert.Equal(t, slices.Contains(afterProductions, beforeProductions[1]), true)
	assert.Equal(
		t,
		slices.Contains(afterProductions, manifest.Production{Owner: entry, Source: manifest.BootstrapSourceID}),
		true,
	)
	assert.Equal(t, afterFacts.Comparison == beforeFacts.Comparison, true)
	assert.Equal(t, afterFacts.Input == beforeFacts.Input, true)
	assert.Equal(t, afterFacts.Changes == beforeFacts.Changes, true)
	assert.Equal(t, afterFacts.Delta == beforeFacts.Delta, true)
	assert.Equal(t, afterFacts.Snapshots == beforeFacts.Snapshots, true)
	assert.Equal(t, afterFacts.Declarations == beforeFacts.Declarations, true)
	assert.Equal(t, afterFacts.ConfiguredInput == beforeFacts.ConfiguredInput, true)
	assert.Diff(t, afterFacts.Context, beforeFacts.Context)
	assert.Diff(t, afterFacts.Files, beforeFacts.Files)
	assert.Diff(t, afterSourceFacts.Files, beforeSourceFacts.Files)
	assert.Equal(t, afterSourceFacts.Input == beforeSourceFacts.Input, true)
	assert.Equal(t, afterSourceFacts.Declaration == beforeSourceFacts.Declaration, true)
	assert.Equal(t, afterSourceFacts.Delta == beforeSourceFacts.Delta, true)
	assert.Equal(
		t,
		cmp.Diff(
			beforeRefs,
			slices.Collect(manifest.References(current, manifest.ReferenceRelations)),
			cmp.AllowUnexported(manifest.Reference{}),
		),
		"",
	)
	assert.Equal(t, caller.Valid(), true)
	result, committed, err := o.selection.Admit(t.Context(), selection.AdmissionOptions{
		Request: proposal.Request, Stage: selection.Execution,
	})
	proposal.Retire()
	assert.NoError(t, err)
	assert.Equal(t, committed, true)
	assert.Equal(t, result.Request.Preparation == &preparation, true)
	assert.Equal(t, result.Revision > pendingRevision, true)
	assert.Equal(t, slices.Contains(result.Needed, id), true)
	assert.Diff(
		t,
		work.Scope().Admission().Counts,
		manifest.WorkCounts{Attempts: 2, Completed: 2, OwnerPrepares: 2, FileEntries: 4},
	)
	for range o.store.Sweep() {
	}
	var fresh store.ReadSet
	fresh.Init(o.store, id, nil)
	fresh.SetWork(&work)
	defer fresh.Close()
	next, err := o.selection.Prepare(
		t.Context(),
		selection.PrepareRequest{
			Missing:     o.cfg.missing,
			Reads:       &fresh,
			Preparation: &preparation,
			Generation:  []store.Child{{Obj: current, Origin: o.store.Policy(id).Origin}},
		},
	)
	assert.NoError(t, err)
	defer next.Retire()
	assert.Diff(
		t,
		work.Scope().Admission().Counts,
		manifest.WorkCounts{Attempts: 2, Completed: 2, OwnerPrepares: 2, FileEntries: 4},
	)
}

//nolint:nonamedreturns // gocritic requires names for the distinct base and head directories.
func excludedOrderingFixture(t *testing.T, broken bool) (baseDir, headDir string) {
	t.Helper()
	baseDir, headDir = t.TempDir(), t.TempDir()
	testutil.WriteFilteredOrderingCluster(t, baseDir, "v1", broken)
	testutil.WriteFilteredOrderingCluster(t, headDir, "v2", broken)
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
			"apiVersion: fluxcd.controlplane.io/v1\nkind: ResourceSet\nmetadata: {name"+
				": invalid, namespace: flux-system}\nspec:\n  dependsOn: [{apiVersion: exa"+
				"mple.test/, kind: Widget, name: missing}]\n",
		)
	}
	return baseDir, headDir
}

func prepareOrderingTrees(t *testing.T, treeInputs []Tree, cfg *Config) *renderInvocation {
	t.Helper()
	assert.NoError(t, snapshotSubstitutions(cfg))
	ctx := t.Context()
	cfg.Network = new(source.Network)
	cache, err := sourcecache.New(ctx, cfg.CacheDir)
	assert.NoError(t, err)
	t.Cleanup(func() {
		assert.NoError(t, cache.Close())
		cfg.Network.Close()
	})
	cfg.Cache = cache
	cfg.HelmClient = helm.NewClient(cache)
	cfg.chartFetcher = helmchart.New(
		&oci.Fetcher{Cache: cache, Network: cfg.Network},
		helmchart.Config{Cache: cache, Network: cfg.Network, Concurrency: cfg.Concurrency},
	)
	cfg.projections = new(manifest.ProducerProjections)
	budget := task.NewBounded(cfg.Concurrency)
	inputs := source.NewHTTPInputs(cache, cfg.Network, budget)
	t.Cleanup(inputs.Close)
	trees, err := prepareTrees(
		ctx,
		treeInputs,
		cfg,
		&inputs,
	)
	assert.NoError(t, err)
	invocation := &renderInvocation{
		cfg:     cfg,
		trees:   trees,
		results: make([]Rendered, len(trees)),
		budget:  budget,
		stores:  make([]*store.Store, len(trees)),
		byID:    make(map[manifest.TreeID]int, len(trees)),
	}
	t.Cleanup(invocation.stopRemaining)
	invocation.discoverTrees(ctx)
	cfg.projections.Clear()
	assert.NoError(t, invocation.stopFailedDiscovery())
	assert.NoError(t, invocation.constructTrees(ctx))
	invocation.completeDiscovery(ctx)
	assert.NoError(t, invocation.qualifyDeclarations(ctx))
	invocation.installAdmission(ctx)
	return invocation
}

func renderOrderingTreesForTest(t *testing.T, before, after string) ([]Rendered, error) {
	t.Helper()
	invocation := prepareOrderingTrees(t, []Tree{
		{RepoRoot: before, ID: manifest.TreeID{Cluster: ".", Side: manifest.TreeBase}},
		{RepoRoot: after, ID: manifest.TreeID{Cluster: ".", Side: manifest.TreeHead}},
	}, &Config{RecordWork: true, Concurrency: 4, CacheDir: t.TempDir()})
	entry := manifest.NewEntryKustomization(manifest.BootstrapSourceID, ".", manifest.NamedResource{}).Named()

	for _, run := range invocation.results {
		o := run.Orchestrator
		published := make(chan struct{})
		o.controllers.Kustomization = &orderedPublisherController{
			reconcileController: o.ksc,
			publisher:           entry,
			published:           published,
			t:                   t,
		}
		o.controllers.Source = &afterPublicationController{
			reconcileController: o.src,
			published:           published,
			tasks:               o.tasks,
		}
	}
	invocation.renderTrees(t.Context())
	clusterAdvisories(invocation.results, invocation.stores)
	var err error
	for _, run := range invocation.results {
		t.Cleanup(run.Result.Close)
		err = errors.Join(err, run.Err)
	}
	return invocation.results, err
}

type orderedPublisherController struct {
	reconcileController

	publisher manifest.NamedResource
	published chan struct{}
	once      sync.Once
	t         *testing.T
}

func (c *orderedPublisherController) ReconcileNode(
	ctx context.Context,
	id manifest.NamedResource,
	level manifest.DrainLevel,
	previous schedule.Result,
) *base.Execution {
	execution := c.reconcileController.ReconcileNode(ctx, id, level, previous)
	if id == c.publisher {
		assert.Equal(c.t, execution.Outcome(), schedule.Terminal)
		c.once.Do(func() { close(c.published) })
	}
	return execution
}

type afterPublicationController struct {
	reconcileController

	published <-chan struct{}
	only      manifest.NamedResource
	tasks     *task.Service
}

func (c *afterPublicationController) ReconcileNode(
	ctx context.Context,
	id manifest.NamedResource,
	level manifest.DrainLevel,
	previous schedule.Result,
) *base.Execution {
	if c.only == (manifest.NamedResource{}) || id == c.only {
		c.tasks.YieldSlot(func() {
			select {
			case <-c.published:
			case <-ctx.Done():
			}
		})
	}
	return c.reconcileController.ReconcileNode(ctx, id, level, previous)
}

func TestRenderTrees_SourcePublicationKeepsHelmReleasePreparation(t *testing.T) {
	t.Parallel()
	beforeDir, afterDir := excludedOrderingFixture(t, true)
	invocation := prepareOrderingTrees(t, []Tree{
		{RepoRoot: beforeDir, ID: manifest.TreeID{Cluster: ".", Side: manifest.TreeBase}},
		{RepoRoot: afterDir, ID: manifest.TreeID{Cluster: ".", Side: manifest.TreeHead}},
	}, &Config{RecordWork: true, Concurrency: 2, CacheDir: t.TempDir()})
	o := invocation.results[1].Orchestrator
	o.configureControllers()
	dispatcher := dagDispatcher{o}
	scheduler := schedule.New(dispatcher, dispatcher)
	defer o.watchDAG(scheduler)()
	reconcile := func(c reconcileController, id manifest.NamedResource, previous *base.Execution) *base.Execution {
		done := make(chan *base.Execution, 1)
		o.tasks.Go(t.Context(), "controlled controller", func(ctx context.Context) {
			done <- c.ReconcileNode(ctx, id, manifest.DrainNone, schedule.Result{Assessment: previous})
		})
		execution := <-done
		o.tasks.BlockTillDone()
		t.Cleanup(execution.Retire)
		return execution
	}
	entry := manifest.NewEntryKustomization(manifest.BootstrapSourceID, ".", manifest.NamedResource{}).Named()
	assert.Equal(t, reconcile(o.ksc, entry, nil).Outcome(), schedule.Terminal)
	id := testutil.HelmReleaseID("flux-system", "selected")
	parent := testutil.KustomizationID("flux-system", "selected")
	sourceID := manifest.NamedResource{
		Group:     manifest.SourceDomain,
		Kind:      manifest.KindOCIRepository,
		Namespace: "flux-system",
		Name:      "selected-source",
	}
	assert.Equal(t, reconcile(o.ksc, parent, nil).Outcome(), schedule.Terminal)
	first := reconcile(o.hrc, id, nil)
	assert.Equal(t, first.Outcome(), schedule.Blocked)
	assert.Equal(t, first.Valid(), true)
	assert.Diff(t, first.Blocked, []manifest.NamedResource{sourceID})
	assert.Diff(
		t,
		first.Reads.WorkScope().Admission().Counts,
		manifest.WorkCounts{Attempts: 1, Completed: 1, OwnerPrepares: 1},
	)
	sourceExecution := reconcile(o.src, sourceID, nil)
	assert.Equal(t, sourceExecution.Outcome(), schedule.Blocked)
	assert.Equal(t, first.Valid(), false)
	second := reconcile(o.hrc, id, first)
	assert.Equal(t, second.Outcome(), schedule.Blocked)
	assert.Equal(t, second.Valid(), true)
	assert.Diff(t, second.Blocked, []manifest.NamedResource{sourceID})
	work := second.Reads.Work()
	assert.Equal(t, work.Key, manifest.WorkKey{
		Tree: manifest.TreeID{Cluster: ".", Side: manifest.TreeHead}, Owner: id, Source: sourceID,
	})
	counts := manifest.WorkCounts{Attempts: 1, Completed: 1, OwnerPrepares: 1}
	assert.Diff(t, work.Scope().Admission().Counts, counts)
	before := make(map[manifest.NamedResource]manifest.BaseManifest)
	documents := make(map[manifest.NamedResource]map[string]any)
	productions := make(map[manifest.NamedResource][]manifest.Production)
	references := make(map[manifest.NamedResource][]manifest.Reference)
	referenceFacts := make(map[manifest.NamedResource]store.ReferenceFacts)
	var captured store.ReadSet
	captured.Init(o.store, id, nil)
	defer captured.Close()
	inputs := append(slices.Collect(second.Reads.Inputs()),
		entry,
		manifest.BootstrapSourceID,
		testutil.KustomizationID("flux-system", "monitor-a"),
		testutil.KustomizationID("flux-system", "monitor-b"),
		manifest.NamedResource{Group: manifest.FluxKustomizeDomain, Kind: manifest.KindKustomization},
	)
	slices.SortFunc(inputs, manifest.NamedResource.Compare)
	inputs = slices.Compact(inputs)
	entryBefore := o.entryBinding
	t.Logf(
		"before terminal source publication: inputs=%v entry=%v source=%v entry-valid=%v counts=%+v",
		inputs,
		entryBefore.Producer,
		entryBefore.Source,
		entryBefore.Reads.Valid(),
		counts,
	)
	for _, input := range inputs {
		object, document, output := captured.GetEmission(input)
		output.Finish()
		before[input], documents[input] = object, document
		productions[input] = slices.Clone(captured.Productions(input, o.loader.SelectionIndex()))
		references[input] = slices.Collect(manifest.References(object, manifest.ReferenceRelations))
		referenceFacts[input] = o.store.ReferenceFacts(input, o.loader.SelectionIndex(), id)
		if input.KindInfo().SourceArtifact {
			captured.GetArtifact(input, store.ArtifactContent|store.ArtifactRevision)
		}
	}
	beforeFacts := o.loader.SourceFacts(sourceID, nil)
	beforeBootstrap := o.loader.SourceFacts(manifest.BootstrapSourceID, nil)
	shared := manifest.NamedResource{
		Group:     manifest.SourceDomain,
		Kind:      manifest.KindGitRepository,
		Namespace: "flux-system",
		Name:      "shared",
	}
	beforeShared := o.loader.SourceFacts(shared, nil)
	var caller store.ReadSet
	caller.Init(o.store, id, nil)
	defer caller.Close()
	settled := make(chan struct{})
	o.tasks.Go(t.Context(), "settle missing source Secret", func(ctx context.Context) {
		sourceExecution.Settle(ctx, manifest.DrainCascade)
		close(settled)
	})
	<-settled
	o.tasks.BlockTillDone()
	status, _ := o.store.GetStatus(sourceID)
	assert.Equal(t, status.Status, store.StatusFailed)
	assert.Equal(t, second.Valid(), false)
	assert.Equal(t, caller.Valid(), true)
	assert.Equal(t, o.entryBinding, entryBefore)
	assert.Equal(t, o.entryBinding.Reads.Valid(), true)
	t.Logf("published source status=%s reason=%s message=%s", status.Status, status.Reason, status.Message)
	assert.Diff(t, work.Scope().Admission().Counts, counts)
	var current store.ReadSet
	current.Init(o.store, id, nil)
	defer current.Close()
	for _, input := range inputs {
		object, document, output := current.GetEmission(input)
		output.Finish()
		assert.Equal(t, manifest.Equal(object, before[input]), true)
		assert.Equal(t, manifest.ValueEqual(document, documents[input]), true)
		assert.Diff(t, current.Productions(input, o.loader.SelectionIndex()), productions[input])
		assert.Equal(
			t,
			cmp.Diff(
				references[input],
				slices.Collect(manifest.References(object, manifest.ReferenceRelations)),
				cmp.AllowUnexported(manifest.Reference{}),
			),
			"",
		)
		facts, oldFacts := o.store.ReferenceFacts(input, o.loader.SelectionIndex(), id), referenceFacts[input]
		assert.Equal(t, facts.Generated, oldFacts.Generated)
		assert.Equal(t, facts.Offline, oldFacts.Offline)
		assert.Equal(t, facts.Private, oldFacts.Private)
		assert.Equal(t, facts.Accepted, oldFacts.Accepted)
		assert.Equal(t, facts.Data == nil, oldFacts.Data == nil)
		if facts.Data != nil && oldFacts.Data != nil {
			assert.Diff(t, facts.Data.Snapshot(), oldFacts.Data.Snapshot())
		}
		assert.Diff(t, facts.Materializers, oldFacts.Materializers)
		assert.Diff(t, facts.Productions, oldFacts.Productions)
		assert.Equal(t, len(facts.ReferenceData), len(oldFacts.ReferenceData))
		for i := range min(len(facts.ReferenceData), len(oldFacts.ReferenceData)) {
			next, previous := facts.ReferenceData[i], oldFacts.ReferenceData[i]
			assert.Equal(t, next.Production, previous.Production)
			assert.Diff(t, next.DeclaredKeys, previous.DeclaredKeys)
			assert.Equal(t, next.OfflineData == nil, previous.OfflineData == nil)
			if next.OfflineData != nil && previous.OfflineData != nil {
				assert.Diff(t, next.OfflineData.Snapshot(), previous.OfflineData.Snapshot())
			}
			assert.Diff(t, next.Warnings, previous.Warnings)
		}

		if input.KindInfo().SourceArtifact {
			current.GetArtifact(input, store.ArtifactContent|store.ArtifactRevision)
		}
		assert.Equal(t, current.MatchesContent(&captured, input), true)
	}
	for _, pair := range [][2]selection.SourceFacts{
		{beforeFacts, o.loader.SourceFacts(sourceID, nil)},
		{beforeBootstrap, o.loader.SourceFacts(manifest.BootstrapSourceID, nil)},
		{beforeShared, o.loader.SourceFacts(shared, nil)},
	} {
		before, after := pair[0], pair[1]
		assert.Equal(t, after.Comparison == before.Comparison, true)
		assert.Equal(t, after.Input == before.Input, true)
		assert.Equal(t, after.Initial == before.Initial, true)
		assert.Equal(t, after.Source, before.Source)
		assert.Equal(t, after.Origin, before.Origin)
		assert.Equal(t, after.Path, before.Path)
		assert.Equal(t, after.ToIndex, before.ToIndex)
		assert.Equal(t, after.Paired, before.Paired)
		assert.Equal(t, after.InitialKnown, before.InitialKnown)
		assert.Equal(t, after.Pinned, before.Pinned)
		assert.Equal(t, after.External, before.External)
		assert.Equal(t, after.Derived, before.Derived)
		assert.Equal(t, after.ConfiguredInput == before.ConfiguredInput, true)
		assert.Equal(t, after.Declaration == before.Declaration, true)
		assert.Equal(t, after.Changes == before.Changes, true)
		assert.Equal(t, after.Delta == before.Delta, true)
		assert.Equal(t, after.Snapshots == before.Snapshots, true)
		assert.Equal(t, after.Declarations == before.Declarations, true)
		assert.Diff(t, after.Files, before.Files)
		assert.Diff(t, after.Context, before.Context)
	}
	third := reconcile(o.hrc, id, second)
	assert.Equal(t, third.Outcome(), schedule.DependencyFailed)
	assert.Equal(t, third.Valid(), true)
	assert.Equal(t, len(third.Blocked), 0)
	t.Logf(
		"after source publication and execution retry: key=%+v counts=%+v",
		work.Scope().Admission().Key,
		work.Scope().Admission().Counts,
	)
	assert.Diff(t, work.Scope().Admission().Counts, counts)
	var fresh store.ReadSet
	fresh.Init(o.store, id, nil)
	fresh.SetWork(work)
	defer fresh.Close()
	proposal, err := o.selection.Prepare(t.Context(), selection.PrepareRequest{
		Missing: o.cfg.missing,
		Reads:   &fresh, Preparation: &third.AdmissionPreparation,
		Generation: []store.Child{{Obj: o.store.GetObject(id), Origin: o.store.Policy(id).Origin}},
	})
	assert.NoError(t, err)
	defer proposal.Retire()
	assert.Equal(t, fresh.Valid(), true)
	t.Logf(
		"next equal generation: revision=%d key=%+v counts=%+v",
		proposal.Revision,
		work.Scope().Admission().Key,
		work.Scope().Admission().Counts,
	)
	assert.Diff(t, work.Scope().Admission().Counts, counts)
	decision := o.selection.Snapshot()
	stale, err := o.selection.Prepare(t.Context(), selection.PrepareRequest{
		Missing: o.cfg.missing,
		Reads:   &second.Reads, Preparation: &third.AdmissionPreparation,
		Generation: []store.Child{{Obj: o.store.GetObject(id), Origin: o.store.Policy(id).Origin}},
	})
	assert.NoError(t, err)
	_, committed, err := o.selection.Admit(t.Context(), selection.AdmissionOptions{
		Request: stale.Request, Stage: selection.Execution,
	})
	stale.Retire()
	assert.NoError(t, err)
	assert.Equal(t, committed, false)
	assert.Equal(t, o.selection.Snapshot() == decision, true)
	assert.Diff(t, work.Scope().Admission().Counts, counts)
}
