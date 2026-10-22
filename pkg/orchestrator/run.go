package orchestrator

import (
	"context"
	"errors"
	"maps"
	"slices"

	"github.com/home-operations/flate/pkg/change"
	"github.com/home-operations/flate/pkg/controllers/base"
	"github.com/home-operations/flate/pkg/controllers/helmrelease"
	"github.com/home-operations/flate/pkg/controllers/kustomization"
	sourcectrl "github.com/home-operations/flate/pkg/controllers/source"
	"github.com/home-operations/flate/pkg/loader"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/resourceset"
	"github.com/home-operations/flate/pkg/store"
)

// orchestratorExistence applies runtime admission to immutable discovery locations.
type orchestratorExistence struct {
	store       *store.Store
	index       *manifest.DiscoveryIndex
	wipeSecrets bool
	filter      *change.Filter
}

func (e *orchestratorExistence) Promote(id manifest.NamedResource) bool {
	for _, p := range e.store.ReferenceFacts(id, e.index).Productions {
		if loader.Promote(e.store, id, p, loader.PromoteOptions{WipeSecrets: e.wipeSecrets, Admit: e.admit}) {
			return true
		}
	}
	return false
}

func (e *orchestratorExistence) admit(obj manifest.BaseManifest) bool {
	info := manifest.KindOf(obj)
	return info.Tracked && (info.Group == manifest.SourceDomain || !info.Runnable) &&
		e.filter.ShouldReconcile(obj.Named())
}

func (e *orchestratorExistence) IsFileIndexed(id manifest.NamedResource) bool {
	if r, ok := e.index.Lookup(id); ok && r.Path != "" {
		return true
	}
	if id.Namespace == "" {
		return false
	}
	// Discovery retains bare identities until parsing applies the parent namespace.
	id.Namespace = ""
	r, ok := e.index.Lookup(id)
	return ok && r.Path != ""
}

// Run starts every controller, blocks until the task service drains,
// then aggregates and returns any failures. The post-drain reporting
// + error-string assembly lives in finalize so Run reads as a clean
// start → drain → finalize sequence.
func (o *Orchestrator) Run(ctx context.Context) (err error) {
	defer func() { err = errors.Join(err, o.Stop()) }()
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
		if !id.KindInfo().Render {
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
	}, o.cfg.SourceRetry)
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
		store:       o.store,
		index:       o.index,
		wipeSecrets: o.cfg.WipeSecrets,
		filter:      o.filter,
	}
	// The reconcile-shaping config common to all three render controllers,
	// built once; each Configure adds its controller-specific fields.
	common := base.Options{
		DependencySubstitutions: o.dependencySubstitutions,
		Filter:                  o.filter,
		Index:                   o.index,
		Existence:               existence,
		PreflightFailure:        o.preflightFailure,
		AllowMissingCRDs:        o.cfg.AllowMissingCRDs,
	}
	o.ksc.Configure(kustomization.Options{
		Options:        common,
		SubstituteFrom: o.substitutionSources, Substitute: o.cfg.Substitute,
		StrictSubstitutions: o.cfg.StrictSubstitutions,
	}, o.cfg.SourceRetry)
	o.hrc.Configure(helmrelease.ReconcileOptions{
		Options:             common,
		AllowMissingSecrets: o.cfg.AllowMissingSecrets,
		SubstituteFrom:      o.substitutionSources,
	})
	o.rsc.Configure(common)
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
// Render joins run teardown before returning, including on Bootstrap failure.
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

func (o *Orchestrator) render(ctx context.Context) (*Result, error) {
	if err := o.Bootstrap(ctx); err != nil {
		return nil, err
	}
	runErr := o.Run(ctx)
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
	// Completed inventory survives a failed successor; diagnostics remain attached to its input.
	failed := o.store.FailedResources()
	res.Manifests = o.collectManifests()
	// Same projection logResourceFailures + aggregateFailures use —
	// see sanitizeFailed for the contract. Centralizing in one
	// helper keeps the three readers in sync if the strip rule
	// changes.
	maps.Copy(res.Failed, sanitizeFailed(failed))
	maps.Copy(res.Orphans, o.orphans)
	// Tag the derived (dependency-blocked) failures so a consumer can collapse
	// the cascade under its root cause. A failure with no blockers is primary.
	for id, info := range res.Failed {
		if info.IsDependencyFailure() {
			res.Blocked[id] = info.BlockedBy()
		}
	}
	return res, runErr
}

// collectManifests filters completed output without changing stored documents needed by value consumers.
// Accepted completed output MUST remain available after a failed successor.
func (o *Orchestrator) collectManifests() map[manifest.NamedResource][]map[string]any {
	skip := o.cfg.HelmOptions.SkipResourceKinds()
	var objects []manifest.BaseManifest
	for _, info := range reconcilableKinds {
		objects = append(objects, o.store.ListObjects(info.Group, info.Kind)...)
	}
	objects = append(objects, o.store.ListObjects(manifest.FluxOperatorDomain, manifest.KindResourceSet)...)
	slices.SortFunc(objects, func(a, b manifest.BaseManifest) int { return a.Named().Compare(b.Named()) })
	out := map[manifest.NamedResource][]map[string]any{}
	for _, obj := range objects {
		id := obj.Named()
		parent := id
		_, parentAttributed := obj.(*manifest.ResourceSet)
		if parentAttributed {
			var ok bool
			parent, ok = o.parentOf(id)
			if !ok {
				continue
			}
		}
		_, attempt := o.store.GetEmission(id)
		attempt.Finish()
		selected := make(map[manifest.NamedResource]bool, len(attempt.Children))
		for _, child := range attempt.Children {
			if child.Doc != nil {
				target := child.Obj.Named()
				selected[target] = !parentAttributed || !manifest.KindOf(child.Obj).Render && o.store.EmissionOwner(target) == id
			}
		}
		for _, doc := range manifest.DropKinds(attempt.Manifests, skip) {
			target := resourceset.DedupKey(doc)
			apiVersion := manifest.DocAPIVersion(doc)
			identifiable := apiVersion != "" && target.Kind != "" && target.Name != "" && target.Group != manifest.KustomizeDomain
			if parentAttributed && identifiable && target.Namespace == "" && manifest.KindOf(&manifest.RawObject{APIVersion: apiVersion, Kind: target.Kind}).Namespaced() {
				target.Namespace = id.Namespace
			}
			if !parentAttributed || !identifiable || selected[target] {
				out[parent] = append(out[parent], doc)
			}
		}
	}
	return out
}

// Stop unsubscribes controllers and joins remote workers before releasing owned
// backing. Repeated calls return the same cleanup error; Store stays readable.
// Callers MUST cancel and join any active Run before Stop. An injected Cache
// and Network remain caller-owned.
func (o *Orchestrator) Stop() error {
	o.stopOnce.Do(func() {
		o.rsc.Close()
		o.hrc.Close()
		o.ksc.Close()
		o.src.Close()
		o.ksc.Trees.Close()
		o.inputs.Close()
		if o.ownNetwork {
			o.cfg.Network.Close()
		}
		if o.cfg.Cache == nil && o.cache != nil {
			o.stopErr = o.cache.Close()
		}
	})
	return o.stopErr
}
