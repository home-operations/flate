package orchestrator

import (
	"context"
	"maps"
	"slices"

	"github.com/home-operations/flate/pkg/controllers/base"
	"github.com/home-operations/flate/pkg/controllers/helmrelease"
	"github.com/home-operations/flate/pkg/controllers/kustomization"
	resourcesetctrl "github.com/home-operations/flate/pkg/controllers/resourceset"
	sourcectrl "github.com/home-operations/flate/pkg/controllers/source"
	"github.com/home-operations/flate/pkg/loader"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/resourceset"
	"github.com/home-operations/flate/pkg/store"
)

// orchestratorExistence adapts the orchestrator's
// loader.ExistenceIndex + Store + WipeSecrets config into the
// depwait.ExistenceLookup interface that flows through controller
// Options into every Waiter. Bundled so controllers don't each
// reach into the orchestrator's private fields, and so future
// existence-related signals land on one type instead of plumbing
// new closures through three call layers.
type orchestratorExistence struct {
	idx         *loader.ExistenceIndex
	store       *store.Store
	wipeSecrets bool
}

func (e *orchestratorExistence) Promote(id manifest.NamedResource) bool {
	return e.idx.Promote(e.store, id, e.wipeSecrets)
}

func (e *orchestratorExistence) IsFileIndexed(id manifest.NamedResource) bool {
	_, ok := e.idx.Get(id)
	return ok
}

// Run starts every controller, blocks until the task service drains,
// then aggregates and returns any failures. The post-drain reporting
// + error-string assembly lives in finalize so Run reads as a clean
// start → drain → finalize sequence.
func (o *Orchestrator) Run(ctx context.Context) error {
	o.configureControllers()
	// Re-detect dependsOn cycles when render-emitted children land.
	// Bootstrap's one-shot pass only sees file-loaded resources; a
	// parent KS render that emits a child with a dependsOn pointing at
	// another freshly-emitted (or pre-existing) peer can introduce a
	// cycle invisible to the Bootstrap pass.
	//
	// Hot path: updateDependencyGraphFor touches only the changed id's
	// edges. The pre-Phase-2.6 implementation re-ran a full O(N+E) DFS
	// on every event — N events × O(N+E) at bootstrap turned 5k-object
	// repos into a quadratic cycle-detection storm. Incremental updates
	// keep each event O(reachable from new dst) in the healthy case and
	// fall back to a per-failed-node revalidation only when an edge is
	// removed.
	//
	// REGISTERED BEFORE the controllers: listeners fire in registration
	// order, so this records preflight failures synchronously BEFORE the
	// controllers' listeners spawn their reconcile goroutines. Without
	// the ordering the controller goroutine could start waiting on a peer
	// that's also waiting on it.
	unsubCycles := o.store.AddListener(store.EventObjectAdded, func(id manifest.NamedResource, _ any) {
		if !isReconcilableKind(id.Kind) {
			return
		}
		o.updateDependencyGraphFor(id)
	}, false)
	defer unsubCycles()

	return o.runDAG(ctx)
}

// configureControllers wires reconcile-shaping config (filter, parent
// resolver, existence + render-inflight bundles, preflight lookups)
// onto all three controllers. Called once at the top of Run before any
// controller Start, since Configure panics if invoked after dispatch
// begins.
func (o *Orchestrator) configureControllers() {
	o.src.Configure(sourcectrl.FetchOptions{
		Filter:              o.filter,
		AllowMissingSecrets: o.cfg.AllowMissingSecrets,
		Producers:           o.producers,
	})
	// parentResolver unifies the two sources of structural-parent
	// info: (1) the pre-built file-path prefix index for file-loaded
	// resources, and (2) the renderedSet for resources that arrive
	// via a KS render's emitRenderedChildren (populated at Run-time,
	// not at Bootstrap). Controllers query through this single seam.
	parentResolver := func(id manifest.NamedResource) (manifest.NamedResource, bool) {
		if parent, ok := o.parentOf[id]; ok {
			return parent, true
		}
		return o.rendered.ParentOf(id)
	}
	// existence bundles the file-existence lookups depwait needs:
	// Promote lazy-loads a file-indexed dep into the Store the
	// moment a depwait edge asks for it (covering the bjw-s parent-
	// patches pattern where a KS's substituteFrom CM lives inside
	// the same KS's spec.path); IsFileIndexed distinguishes
	// "render-only dep still in flight" (no file record → wait on
	// the per-dep ctx) from "file-indexed but promote failed" (file
	// record → fail fast). The orchestrator owns both signals; the
	// controllers and Waiter consume them via the single
	// depwait.ExistenceLookup interface.
	existence := &orchestratorExistence{
		idx:         o.existence,
		store:       o.store,
		wipeSecrets: o.cfg.WipeSecrets,
	}
	// selfProduces reports whether consumer's OWN render emits cm — the
	// graph-aware self-substitute signal collectDeps uses to drop a
	// self-produced postBuild.substituteFrom ConfigMap from the dep set.
	// Nil-safe: a nil index (no repoRoot) yields false, so the edge is
	// kept (the pre-index always-add behavior).
	selfProduces := func(cm, consumer manifest.NamedResource) bool {
		return slices.Contains(o.selfProduce.ProducedBy(cm), consumer)
	}
	// The reconcile-shaping config common to all three render controllers,
	// built once; each Configure adds its controller-specific fields.
	common := base.Options{
		Filter:           o.filter,
		ParentOf:         parentResolver,
		RenderTracker:    o.rendered,
		Existence:        existence,
		PreflightFailure: o.preflightFailure,
	}
	o.ksc.Configure(kustomization.Options{Options: common, SelfProduces: selfProduces})
	o.hrc.Configure(helmrelease.ReconcileOptions{
		Options:             common,
		AllowMissingSecrets: o.cfg.AllowMissingSecrets,
		Producers:           o.producers,
	})
	// The RS controller feeds each RawObject child it renders into
	// rsRawSink keyed by the RS's parent KS; render() commits the sink
	// into rsExtensions for the Result.Manifests grouping. DedupKey is
	// computed here so the sink stays orchestrator-internal.
	o.rsc.Configure(resourcesetctrl.Options{
		Options: common,
		RawSink: func(owner, parentKS manifest.NamedResource, doc map[string]any) {
			o.rsRawSink.Record(owner, parentKS, resourceset.DedupKey(doc), doc)
		},
	})
}

// Render is the structured embed-friendly entry point: Bootstrap +
// Run + collect everything an external caller needs to consume the
// reconcile result. CLI / Run() ergonomics remain unchanged; callers
// who want a single function that returns a typed Result use this.
//
// The returned Result is non-nil even when err is non-nil — failures
// during reconcile populate Result.Failed without aborting collection,
// so the caller sees both the partial output and the failure list.
// An error from Bootstrap (the load phase) is fatal and returns
// (nil, err); errors from Run yield (result, err).
//
// On any returned error, render defers Stop so embedders that receive
// (nil, err) — Bootstrap failure paths in particular — don't leak the
// staging cache tempdir and helm client until process exit. Stop is
// sync.Once-guarded so the deferred call composes safely with Run's
// own deferred Stop and any explicit caller Stop.
//
// Idempotent: a second call returns the cached Result/err pair from
// the first. The controllers' Configure hooks panic if invoked after
// Start (reconcile-shaping config is frozen once dispatch begins), so
// a re-Run would panic. Caching at this boundary lets embedders retry
// Render without restarting controllers — pair with Bootstrap's same
// guarantee guard above.
func (o *Orchestrator) Render(ctx context.Context) (*Result, error) {
	o.renderOnce.Do(func() {
		o.renderResult, o.renderErr = o.render(ctx)
	})
	return o.renderResult, o.renderErr
}

func (o *Orchestrator) render(ctx context.Context) (result *Result, err error) {
	defer func() {
		if err != nil {
			o.Stop()
		}
	}()
	if err = o.Bootstrap(ctx); err != nil {
		return nil, err
	}
	runErr := o.Run(ctx)
	// ResourceSets now expand in-DAG (the RS controller renders them as
	// first-class nodes), feeding each RawObject child into rsRawSink as
	// it goes. Commit the sink into rsExtensions so the Result.Manifests
	// merge below groups those non-Flux children under their owning
	// parent KS — what `flate build` surfaces as the RS's offline output.
	o.rsExtensions = o.rsRawSink.commit()
	res := &Result{
		Manifests: map[manifest.NamedResource][]map[string]any{},
		Failed:    map[manifest.NamedResource]store.StatusInfo{},
		Orphans:   map[manifest.NamedResource]string{},
		// The dependsOn graph is fully populated by now (Bootstrap's rebuild +
		// per-id ReplaceEdges during Run); snapshot it for blast-radius consumers.
		DependsOn: o.depGraph.Edges(),
		Blocked:   map[manifest.NamedResource][]manifest.NamedResource{},
		// Render advisories collected by producers during Run (HR stale values,
		// empty scan, …). Nil when none, sorted deterministically by the store.
		Warnings: o.store.Warnings(),
	}
	// Apply --skip-secrets / --skip-crds / --skip-kinds uniformly here
	// so embedders calling Render see consistent Result.Manifests
	// regardless of producing controller. helm.TemplateDocs pre-filters
	// HR output upstream, but Kustomization-rendered docs reach the
	// artifact unfiltered (the Store must hold the full set so
	// downstream valuesFrom / substituteFrom resolution finds Secret /
	// ConfigMap objects). The drop happens here, on the slice that
	// crosses the embed boundary — Store stays whole. Iter-15 #169
	// patched the CLI emit paths; this closes the same gap one layer
	// down for SDK consumers.
	skip := o.cfg.HelmOptions.SkipResourceKinds()
	for _, kind := range reconcilableKinds {
		for _, obj := range o.store.ListObjects(kind) {
			id := obj.Named()
			var mans []map[string]any
			if art, ok := o.store.GetArtifact(id).(store.RenderedArtifact); ok {
				mans = art.RenderedManifests()
			}
			// Append any ResourceSet-rendered non-Flux docs whose
			// owning KS is this one. The RS doc itself stays in the
			// parent's render output (kustomize emitted it); these
			// are its synthetic children that flate evaluates offline.
			if kind == manifest.KindKustomization {
				if ext := o.rsExtensions[id]; len(ext) > 0 {
					mans = append(mans, ext...)
				}
			}
			mans = manifest.DropKinds(mans, skip)
			if len(mans) > 0 {
				res.Manifests[id] = mans
			}
		}
	}
	// Same projection logResourceFailures + aggregateFailures use —
	// see sanitizeFailed for the contract. Centralizing in one
	// helper keeps the three readers in sync if the strip rule
	// changes.
	maps.Copy(res.Failed, sanitizeFailed(o.store.FailedResources()))
	maps.Copy(res.Orphans, o.orphans)
	// Tag the derived (dependency-blocked) failures so a consumer can collapse
	// the cascade under its root cause. A failure with no blockers is primary.
	for id := range res.Failed {
		if b := o.store.BlockedBy(id); len(b) > 0 {
			res.Blocked[id] = b
		}
	}
	return res, runErr
}

// Stop shuts the controllers down in reverse-construction order. Safe to call
// multiple times: each controller's Close is idempotent (drains the unsub slice
// once and nils it). Wrapped in sync.Once so the bookkeeping reads cleanly even
// if a caller's defer runs after Run's defer.
//
// The kustomize render cache is in-memory and holds no OS resources, so there
// is nothing to release here — it is freed with the Orchestrator.
func (o *Orchestrator) Stop() {
	o.stopOnce.Do(func() {
		o.rsc.Close()
		o.hrc.Close()
		o.ksc.Close()
		o.src.Close()
	})
}
