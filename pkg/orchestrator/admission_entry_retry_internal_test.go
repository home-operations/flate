package orchestrator

import (
	"context"
	"errors"
	"slices"
	"sync"
	"testing"

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
	assert.Diff(t, slices.Collect(manifest.References(current, manifest.ReferenceRelations)), beforeRefs)
	assert.Equal(t, caller.Valid(), true)
	next, committed, err := o.selection.Commit(t.Context(), proposal, selection.Execution)
	assert.NoError(t, err)
	assert.Equal(t, committed, true)
	assert.Equal(t, next != nil, true)
	assert.Equal(t, next.Revision > pendingRevision, true)
	assert.Equal(t, slices.Contains(next.Needed, id), true)
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
	next, err = o.selection.Prepare(
		t.Context(),
		selection.PrepareRequest{
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
