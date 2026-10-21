// Package resourceset reconciles flux-operator ResourceSets as
// first-class DAG nodes: wait on dependsOn / inputsFrom RSIPs / the
// structural parent KS, render the RS via the resourceset package, and
// emit every child through the shared emission transaction. A RS with a
// selector-only inputsFrom (no nameable RSIP to park on) asks the
// scheduler to re-run it at the structural fixpoint so it expands
// against the now-complete RSIP set.
package resourceset

import (
	"context"
	"slices"

	"github.com/home-operations/flate/pkg/controllers/base"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/resourceset"
	"github.com/home-operations/flate/pkg/store"
	"github.com/home-operations/flate/pkg/task"
)

// Controller orchestrates ResourceSet reconciliation. Reconcile-shaping
// state (Filter, Index, Existence) flows in via Configure exactly once before
// StartLifecycle — the same "config is read-only after StartLifecycle" invariant the KS
// and HR controllers carry, encoded in the embedded *base.Controller.
type Controller struct {
	*base.Controller

	// WipeSecrets controls whether Secret cleartext is wiped when parsing
	// emitted children (forwarded to base.Controller.Emit).
	WipeSecrets bool
}

// New constructs a ResourceSet controller.
func New(s *store.Store, t *task.Service, wipeSecrets bool) *Controller {
	return &Controller{
		Controller:  base.New(s, t, "resourceset"),
		WipeSecrets: wipeSecrets,
	}
}

// ReconcileNode runs id's reconcile under the dag engine, returning the
// blocked dependency set (nil = terminalized).
// The orchestrator's scheduler Dispatcher calls this for ResourceSet
// nodes. Reconciliation-disabled ResourceSets use the shared suspension gate.
func (c *Controller) ReconcileNode(ctx context.Context, id manifest.NamedResource, level manifest.DrainLevel) []manifest.NamedResource {
	return c.DispatchNode(ctx, id, level, c.reconcile)
}

func (c *Controller) reconcile(ctx context.Context, rs *manifest.ResourceSet, level manifest.DrainLevel) error {
	id := rs.Named()
	if err := c.PreflightError(id); err != nil {
		return err
	}
	// SetPendingUnlessReady (not a raw UpdateStatus): a no-op re-reconcile of
	// an already-Ready RS (re-emitted by its structural parent / a drain-rerun
	// that converged) must not transiently downgrade Ready→Pending, mirroring
	// the KS/HR pre-render progress writes (#657).
	c.SetPendingUnlessReady(id, "resolving inputs")

	deps := c.collectDeps(rs)
	if len(deps) > 0 {
		// RequireRefresh fuses the gate with the load-bearing re-read: the
		// structural parent may have re-emitted us with a substituted
		// name/namespace while we were waiting (a KS render baking
		// targetNamespace into a duplicate copy). Without the refresh the
		// first render would use the stale-spec snapshot. Mirrors the KS
		// controller (#102).
		fresh, ok, err := c.RequireRefresh[*manifest.ResourceSet](
			ctx, id, deps, level)
		if err != nil {
			return err
		}
		if ok {
			rs = fresh
		}
	}
	if err := c.PreflightError(id); err != nil {
		return err
	}

	resolve := resourceset.StoreResolver(c.Store)
	fp, handled, err := c.FingerprintDedup(id, resourceSetContent(rs, resolve))
	if handled {
		return err
	}

	_, attempt := c.Store.GetEmission(id)
	defer attempt.Finish()
	docs, err := resourceset.Render(rs, resolve)
	if err != nil {
		c.Store.UpdateStatus(id, store.StatusFailed, "", err.Error())
		return err
	}
	attempt.Children, attempt.Manifests = base.ParseChildren(id, docs, manifest.ParseDocOptions{WipeSecrets: c.WipeSecrets, DefaultNamespace: id.Namespace})
	attempt.Fingerprint, attempt.Complete = fp, true
	c.Emit(id, &attempt)
	return nil
}

// collectDeps gates explicit dependencies, named inputs and the structural parent.
// Authored dependsOn Ready=false still requests readiness here. Selector-only inputs
// use the drain rerun because their producers have no nameable gate.
func (c *Controller) collectDeps(rs *manifest.ResourceSet) []manifest.DependencyRef {
	parents := c.Store.Productions(rs.Named(), nil)
	capacity := len(rs.DependsOn) + len(rs.InputsFrom)
	deps := make([]manifest.DependencyRef, capacity)
	n := 0
	for ref := range manifest.References(rs) {
		if ref.Use == "dependsOn" || ref.Use == "inputsFrom" && ref.Name != "" {
			deps[n] = ref.DependencyRef
			n++
		}
	}
	deps = deps[:n]
	parents(func(p manifest.Production) bool {
		if p.Owner != (manifest.NamedResource{}) && p.Owner != rs.Named() {
			deps = append(deps, manifest.DependencyRef{NamedResource: p.Owner})
		}
		return true
	})
	return deps
}

// WantsDrainRerun reports whether the ResourceSet id has a selector-only
// inputsFrom (ref.Name == "") — the case where its matching RSIPs may be
// emitted by an unknown producer, so it has no nameable dependency to
// park on. Such a RS asks the scheduler to re-run it at the structural
// fixpoint (the orchestrator's dispatcher returns this as the dispatch rerun
// intent) so it re-expands against the now-complete RSIP set. Returns false
// when id is not a RS in the store.
func (c *Controller) WantsDrainRerun(id manifest.NamedResource) bool {
	rs, ok := c.Store.Get[*manifest.ResourceSet](id)
	if !ok || rs.Suspend {
		return false
	}
	for ref := range manifest.References(rs) {
		if ref.Use == "inputsFrom" && ref.Name == "" {
			return true
		}
	}
	return false
}

func resourceSetContent(rs *manifest.ResourceSet, resolve resourceset.ProviderResolver) any {
	providers := make(map[manifest.NamedResource]*manifest.ResourceSetInputProvider)
	for edge := range manifest.References(rs) {
		if edge.Use != "inputsFrom" {
			continue
		}
		matches, err := resolve(rs.InputsFrom[edge.Position], edge.NamedResource)
		if err != nil {
			return nil
		}
		for _, p := range matches {
			providers[p.Named()] = p
		}
	}
	ids := make([]manifest.NamedResource, 0, len(providers))
	for id := range providers {
		ids = append(ids, id)
	}
	slices.SortFunc(ids, manifest.NamedResource.Compare)
	var inputs []any
	for _, id := range ids {
		content, err := providers[id].Content()
		if err != nil {
			return nil
		}
		inputs = append(inputs, content)
	}
	return rs.Content(inputs)
}
