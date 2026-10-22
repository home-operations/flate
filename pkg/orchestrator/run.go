package orchestrator

import (
	"context"
	"errors"
	"maps"
	"slices"

	"github.com/home-operations/flate/pkg/change"
	"github.com/home-operations/flate/pkg/controllers/base"
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

func (e *orchestratorExistence) Promote(ctx context.Context, reads *store.ReadSet, id manifest.NamedResource) bool {
	if obj := e.store.GetObject(id); obj != nil {
		return e.admit(obj)
	}
	productions := e.store.Productions(id, e.index)
	if reads != nil {
		productions = reads.Promotions(id, e.index)
	}
	for _, p := range productions {
		if loader.Promote(ctx, e.store, id, loader.PromoteOptions{Record: p, Reads: reads, WipeSecrets: e.wipeSecrets, Admit: e.admit}) {
			if reads != nil {
				reads.Productions(id, e.index)
			}
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

// Run starts the controllers and returns their settled failures.
func (o *Orchestrator) Run(ctx context.Context) (err error) {
	defer func() { err = errors.Join(err, o.Stop()) }()
	o.configureControllers()
	// Cycle failures MUST publish before scheduler admission of emitted objects.
	unsubCycles := o.store.AddListener(store.EventObjectAdded, func(id manifest.NamedResource, _ any) {
		if !id.KindInfo().Render {
			return
		}
		o.updateDependencyGraphFor(id)
	}, false)
	defer unsubCycles()

	return o.runDAG(ctx)
}

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
		SubstituteFrom:          o.substitutionSources,
		SkipKinds:               o.cfg.HelmOptions.SkipResourceKinds(),
		DependencySubstitutions: o.dependencySubstitutions,
		Filter:                  o.filter,
		Index:                   o.index,
		Existence:               existence,
		PreflightFailure:        o.preflightFailure,
		AllowMissingCRDs:        o.cfg.AllowMissingCRDs,
		AllowMissingSecrets:     o.cfg.AllowMissingSecrets,
	}
	o.ksc.Configure(kustomization.Options{
		Options:             common,
		Substitute:          o.cfg.Substitute,
		StrictSubstitutions: o.cfg.StrictSubstitutions,
	}, o.cfg.SourceRetry)
	o.hrc.Configure(common)
	o.rsc.Configure(common)
	o.agc.Configure(common)
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
// StartLifecycle (reconcile-shaping config is frozen once dispatch begins), so
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
		Failed:  map[manifest.NamedResource]store.StatusInfo{},
		Orphans: map[manifest.NamedResource]string{},
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
func (o *Orchestrator) collectManifests() []manifest.DocumentBatch {
	var objects []manifest.BaseManifest
	for info := range manifest.Kinds() {
		if info.Runnable {
			objects = append(objects, o.store.ListObjects(info.Group, info.Kind)...)
		}
	}
	slices.SortFunc(objects, func(a, b manifest.BaseManifest) int { return a.Named().Compare(b.Named()) })
	out := make([]manifest.DocumentBatch, 0, len(objects)+1)
	positions := make(map[manifest.NamedResource]int, len(objects))
	for _, obj := range objects {
		id := obj.Named()
		parent := id
		parentAttributed := !manifest.KindOf(obj).Render
		if parentAttributed {
			var ok bool
			parent, ok = o.parentOf(id)
			if !ok {
				if manifest.KindOf(obj).Parent {
					continue
				}
				parent = id
			}
			if o.store.GetObject(parent) == nil {
				continue
			}
		}
		effective, _, attempt := o.store.GetEmission(id)
		attempt.Finish()
		if effective == nil {
			continue
		}
		selected := make(map[manifest.NamedResource]bool, len(attempt.Children))
		for _, child := range attempt.Children {
			if child.Doc != nil {
				target := child.Obj.Named()
				selected[target] = !parentAttributed || !manifest.KindOf(child.Obj).Render && o.store.EmissionOwner(target) == id
			}
		}
		for _, doc := range attempt.Manifests {
			target := resourceset.DedupKey(doc)
			apiVersion := manifest.DocAPIVersion(doc)
			identifiable := apiVersion != "" && target.Kind != "" && target.Name != "" && target.Group != manifest.KustomizeDomain
			if parentAttributed && identifiable && target.Namespace == "" && manifest.KindOf(&manifest.RawObject{APIVersion: apiVersion, Kind: target.Kind}).Namespaced() {
				target.Namespace = id.Namespace
			}
			if !parentAttributed || !identifiable || selected[target] {
				position, ok := positions[parent]
				if !ok {
					position = len(out)
					positions[parent] = position
					out = append(out, manifest.DocumentBatch{Parent: o.store.GetObject(parent)})
				}
				out[position].Documents = append(out[position].Documents, doc)
			}
		}
	}
	var documents []map[string]any
	for _, doc := range o.documents {
		id := manifest.DocIdentity(doc)
		if o.store.EmissionOwner(id) == (manifest.NamedResource{}) && o.filter.ShouldReconcile(id) {
			documents = append(documents, doc)
		}
	}
	if len(documents) != 0 {
		out = append(out, manifest.DocumentBatch{Documents: documents})
	}
	return out
}

// Stop joins remote workers before releasing owned
// backing. Repeated calls return the same cleanup error; Store stays readable.
// Callers MUST cancel and join any active Run before Stop. An injected Cache
// and Network remain caller-owned.
func (o *Orchestrator) Stop() error {
	o.stopOnce.Do(func() {
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
