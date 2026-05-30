package orchestrator

import (
	"context"
	"errors"
	"log/slog"
	"maps"
	"runtime"

	"golang.org/x/sync/errgroup"

	"github.com/home-operations/flate/pkg/controllers/helmrelease"
	"github.com/home-operations/flate/pkg/controllers/kustomization"
	sourcectrl "github.com/home-operations/flate/pkg/controllers/source"
	"github.com/home-operations/flate/pkg/loader"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
	"github.com/home-operations/flate/pkg/task"
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

// orchestratorRenderInflight adapts task.Service into
// depwait.RenderInflight.
type orchestratorRenderInflight struct{ tasks *task.Service }

// QuiescenceCh delegates to task.Service.QuiescenceCh(0). The
// threshold is 0 because depwait callers reach this method while
// inside YieldQuiescent, which has already decremented the caller's
// own active slot — drain-to-zero means no productive task remains
// in the pool. Without YieldQuiescent's hop, two reconciles each
// blocked in depwait would pin the count at 2 and miss this signal
// entirely.
func (r *orchestratorRenderInflight) QuiescenceCh() <-chan struct{} {
	return r.tasks.QuiescenceCh(0)
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
		if id.Kind != manifest.KindKustomization && id.Kind != manifest.KindHelmRelease {
			return
		}
		o.updateDependencyGraphFor(id)
	}, false)
	defer unsubCycles()

	// Keep depwait's render-quiescence signal open while listener
	// flushes submit the bootstrap objects. Without this marker, a
	// Kustomization can start waiting on a render-emitted dependency
	// before the producer's replay event has been submitted and
	// incorrectly conclude that the render pool is drained.
	startupDone := make(chan struct{})
	o.tasks.Go(ctx, "orchestrator/startup", func(ctx context.Context) {
		select {
		case <-startupDone:
		case <-ctx.Done():
		}
	})
	// Start all three controllers in parallel + pre-warm every
	// GitRepository's bare mirror at the same time. Three benefits:
	//   1. Each Controller.Start runs AddListener(flush=true), which
	//      replays every bootstrap object through the listener under
	//      the store's write lock. The replays themselves still
	//      serialize on s.mu, but launching them concurrently removes
	//      goroutine-startup overhead and stays forward-compatible
	//      with sharded-store work (Phase 3.1).
	//   2. The three controllers have mutually exclusive Kind filters
	//      (source → GitRepository/OCIRepository/Bucket/External,
	//      ksc → Kustomization, hrc → HelmRelease), so the order in
	//      which their listeners land in the listener-set slice
	//      doesn't affect correctness — each filter ignores
	//      everything outside its own Kind.
	//   3. The cycle-detection listener registered above runs at
	//      slice position 0 and stays there: errgroup launches the
	//      controllers AFTER unsubCycles is in place, so future
	//      EventObjectAdded fires still hit cycle detection before
	//      KS/HR controllers' listeners — the precondition for the
	//      preflight-failure routing comment above.
	//
	// Mirror pre-warm runs as a fourth sibling: every GitRepository's
	// per-URL bare mirror is opened/fetched in a bounded errgroup so
	// the heavy network I/O overlaps with the lightweight controller
	// startup. The source controller's reconcile then sees a warm
	// mirror and OpenOrFetch returns instantly. Pre-warm errors are
	// logged here, not propagated — the source controller will retry
	// the same path during reconcile and produce the canonical
	// per-source status update.
	//
	// IMPORTANT: pass the unwrapped ctx, not the errgroup's gctx, into
	// every controller and the pre-warm. Controller listeners submit
	// reconcile bodies via task.Coalescer.Submit that propagate ctx
	// into long-running render goroutines; if those captured the
	// errgroup's gctx, they'd inherit cancellation the moment g.Wait
	// returns and abort with "context canceled" before any KS/HR could
	// finish. The errgroup here is a pure barrier, not a cancellation
	// scope.
	var g errgroup.Group
	g.Go(func() error { o.src.Start(ctx); return nil })
	g.Go(func() error { o.ksc.Start(ctx); return nil })
	g.Go(func() error { o.hrc.Start(ctx); return nil })
	g.Go(func() error { o.prewarmGitMirrors(ctx); return nil })
	_ = g.Wait()
	close(startupDone)
	defer o.Stop()

	return o.awaitDrain(ctx)
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
	renders := &orchestratorRenderInflight{tasks: o.tasks}
	o.ksc.Configure(kustomization.Options{
		Filter:           o.filter,
		ParentOf:         parentResolver,
		RenderTracker:    o.rendered,
		Existence:        existence,
		Renders:          renders,
		PreflightFailure: o.preflightFailure,
	})
	o.hrc.Configure(helmrelease.ReconcileOptions{
		Filter:              o.filter,
		ParentOf:            parentResolver,
		RenderTracker:       o.rendered,
		Existence:           existence,
		Renders:             renders,
		PreflightFailure:    o.preflightFailure,
		AllowMissingSecrets: o.cfg.AllowMissingSecrets,
	})
}

// awaitDrain races ctx.Done() against task drain so a Ctrl-C during a
// stuck fetch / render is observable within one task's natural
// cancellation latency rather than blocking forever on wgActive.Wait.
// Reconcile bodies still need to honor their own ctx to actually return
// promptly, but at minimum Run no longer hides the cancellation signal
// from finalize.
func (o *Orchestrator) awaitDrain(ctx context.Context) error {
	done := make(chan struct{})
	go func() {
		o.tasks.BlockTillDone()
		close(done)
	}()
	select {
	case <-done:
		return errors.Join(o.finalize(), ctx.Err())
	case <-ctx.Done():
		// Wait for in-flight tasks to wind down before finalizing so
		// the store snapshot is internally consistent, then preserve
		// the cancellation in the returned error. Queued bounded tasks
		// may never acquire a worker slot once ctx is canceled; a clean
		// partial snapshot must not look like a successful full run.
		<-done
		return errors.Join(o.finalize(), ctx.Err())
	}
}

// prewarmGitMirrors fires Prewarm against every discovered
// GitRepository in parallel. Called from Run inside the same errgroup
// that launches the controller Start calls, so the network I/O
// overlaps with controller startup. The per-URL mirror lock inside
// mirror.Cache.OpenOrFetch already serializes duplicate-URL pre-warms,
// so a bounded errgroup is sufficient — no per-URL coalescing needed
// here.
//
// Pre-warm failures are logged at debug level only. The source
// controller's reconcile of the same GitRepository will retry the
// fetch path immediately after Start returns and is the canonical
// reporter for per-source errors (auth, TLS, missing-secret). Logging
// at error level here would double-report transient network failures
// users see in the per-resource status output.
//
// No-op when:
//   - The orchestrator was built without a git fetcher (test path
//     that strips the default via WithFetcher(KindGitRepository, nil)).
//   - The git fetcher has no Mirrors configured.
//   - There are no GitRepository objects in the store.
func (o *Orchestrator) prewarmGitMirrors(ctx context.Context) {
	if o.gitFetcher == nil || o.gitFetcher.Mirrors == nil {
		return
	}
	repos := store.ListAs[*manifest.GitRepository](o.store, manifest.KindGitRepository)
	if len(repos) == 0 {
		return
	}
	limit := o.cfg.Concurrency
	if limit <= 0 {
		// Mirror the task.NewBounded default — I/O-bound work, cap at
		// 4x NumCPU so a tiny machine with many GitRepositories still
		// makes progress, and a huge fleet doesn't fork-bomb the file
		// descriptors / git transport pool.
		limit = runtime.NumCPU() * 4
	}
	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(limit)
	for _, repo := range repos {
		g.Go(func() error {
			if err := o.gitFetcher.Prewarm(gctx, repo); err != nil {
				slog.Debug("git: mirror prewarm failed (source controller will retry)",
					"git_repository", repo.Namespace+"/"+repo.Name,
					"url", repo.URL,
					"err", err)
			}
			return nil
		})
	}
	_ = g.Wait()
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
	// Post-Run RS expansion: re-render every ResourceSet now that
	// KS-controller-emitted RSIPs (kustomize-substituted dynamic
	// names like dragonfly-${APP}) are in the store. Discovery's
	// pre-Bootstrap renders see only literal-file RSIPs and miss
	// these. The output attributes non-Flux children to the owning
	// parent KS so `flate build` surfaces them.
	rsErr := o.expandResourceSetsPostRun(ctx)
	res := &Result{
		Manifests: map[manifest.NamedResource][]map[string]any{},
		Failed:    map[manifest.NamedResource]store.StatusInfo{},
		Orphans:   map[manifest.NamedResource]string{},
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
	for _, kind := range []string{manifest.KindKustomization, manifest.KindHelmRelease} {
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
	// Same projection logRecourceFailures + aggregateFailures use —
	// see sanitizeFailed for the contract. Centralizing in one
	// helper keeps the three readers in sync if the strip rule
	// changes.
	maps.Copy(res.Failed, sanitizeFailed(o.store.FailedResources()))
	maps.Copy(res.Orphans, o.orphans)
	return res, errors.Join(runErr, rsErr)
}

// Stop shuts the controllers down in reverse-construction order and
// releases the staging cache. Safe to call multiple times: each
// controller's Close is idempotent (drains the unsub slice once and
// nils it), and StagingCache.Close zeroes its stages map after the
// first cleanup. Wrapped in sync.Once so the bookkeeping reads
// cleanly even if a caller's defer runs after Run's defer.
//
// Embedders who call only New + Bootstrap (without Run) MUST call
// Stop themselves — the staging cache holds a tempdir that would
// otherwise leak until process exit.
func (o *Orchestrator) Stop() {
	o.stopOnce.Do(func() {
		o.hrc.Close()
		o.ksc.Close()
		o.src.Close()
		if o.staging != nil {
			_ = o.staging.Close()
		}
	})
}
