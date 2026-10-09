// Package resourceset reconciles flux-operator ResourceSets as
// first-class DAG nodes: wait on dependsOn / inputsFrom RSIPs / the
// structural parent KS, render the RS via the resourceset package, and
// emit every child through the standard two-pass emit path. A RS with a
// selector-only inputsFrom (no nameable RSIP to park on) asks the
// scheduler to re-run it at the structural fixpoint so it expands
// against the now-complete RSIP set.
package resourceset

import (
	"context"

	"github.com/home-operations/flate/pkg/controllers/base"
	"github.com/home-operations/flate/pkg/controllers/emit"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/resourceset"
	"github.com/home-operations/flate/pkg/store"
	"github.com/home-operations/flate/pkg/task"
)

// RawSink receives every non-Flux (RawObject) child a ResourceSet emits:
// the emitting RS (owner), the RS's structural-parent Kustomization
// (the grouping key), and the rendered doc. The orchestrator installs it
// to reproduce the legacy post-run output grouping — RS RawObject
// children surface under their parent KS in `flate build` output — using
// owner for deterministic cross-RS dedup. Nil is fine — no-op.
type RawSink func(owner, parentKS manifest.NamedResource, doc map[string]any)

// Controller orchestrates ResourceSet reconciliation. Reconcile-shaping
// state (Filter, ParentOf, …) flows in via Configure exactly once before
// Start — the same "config is read-only after Start" invariant the KS
// and HR controllers carry, encoded in the embedded *base.Controller.
type Controller struct {
	*base.Controller

	// WipeSecrets controls whether Secret cleartext is wiped when parsing
	// emitted children (forwarded to emit.Children).
	WipeSecrets bool

	// rawSink, when set, receives each RawObject child this RS emits so the
	// orchestrator can group it under the RS's parent KS for output.
	rawSink RawSink
}

// Options carries the post-bootstrap state the orchestrator wires onto the
// controller before Start. base.Options holds the config common to every
// render controller (Filter / ParentOf — here gating the RS render on its
// structural parent KS, which may re-emit the RS with a substituted
// name/namespace — RenderTracker / Existence / PreflightFailure); RawSink is
// ResourceSet-specific.
type Options struct {
	base.Options
	RawSink RawSink
}

// New constructs a ResourceSet controller.
func New(s *store.Store, t *task.Service, wipeSecrets bool) *Controller {
	return &Controller{
		Controller:  base.New(s, t, "resourceset"),
		WipeSecrets: wipeSecrets,
	}
}

// Configure installs the post-bootstrap state. Panics if called after
// Start — encodes the invariant that reconcile-shaping config is
// read-only once the controller is dispatching.
func (c *Controller) Configure(opts Options) {
	c.Controller.Configure(opts.Options)
	c.rawSink = opts.RawSink
}

// Start wires lifecycle state. The scheduler owns dispatch (via
// ReconcileNode) and the orchestrator's dagrun wires its own wake
// listeners, so Start registers no dispatch listener of its own.
func (c *Controller) Start(_ context.Context) {
	c.StartLifecycle()
}

// ReconcileNode runs id's reconcile under the dag engine, returning the
// blocked dependency set (nil = terminalized) and whether id ended Ready.
// The orchestrator's scheduler Dispatcher calls this for ResourceSet
// nodes. A ResourceSet has no Suspend field, so the suspended predicate
// is always false.
func (c *Controller) ReconcileNode(ctx context.Context, id manifest.NamedResource, drainLevel int) []manifest.NamedResource {
	return c.DispatchNode(ctx, id, drainLevel,
		func(*manifest.ResourceSet) bool { return false },
		c.reconcile)
}

func (c *Controller) reconcile(ctx context.Context, rs *manifest.ResourceSet) error {
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
			ctx, id, nil, deps,
			"", base.DepFailed(id)) // empty pendingMsg: status set above
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

	// A fingerprint hit MUST not replay emission: KeepEmitted / ReportRendered /
	// rawSink bookkeeping is cumulative and already complete. AddRendered
	// independently suppresses equal RawObject arrivals. Changed inputs require
	// a fresh render; fp is reused for SetArtifact.
	fp := resourceSetFingerprint(rs, c.Store)
	if handled, err := c.FingerprintDedup(id, fp, func([]map[string]any) {}); handled {
		return err
	}

	docs, err := resourceset.Render(rs, resourceset.StoreResolver(c.Store))
	if err != nil {
		c.Store.UpdateStatus(id, store.StatusFailed, err.Error())
		return err
	}
	c.emit(id, docs, true)

	c.Store.SetArtifact(id, &store.ResourceSetArtifact{Manifests: docs, Fingerprint: fp})
	return nil
}

// emit lands the RS-rendered children through the shared two-pass emit
// helper (so Flux-kind children reconcile / land in the store) and, on
// the fresh-render path, additionally routes RawObject children to the
// orchestrator's output sink keyed by the RS's parent KS. The sink is
// fed only when publishing (the dedup replay would double-feed it
// otherwise — the cached docs were already sunk by the render that set
// the artifact).
func (c *Controller) emit(id manifest.NamedResource, docs []map[string]any, publish bool) {
	children := emit.Children(c.Controller, c.WipeSecrets, id, docs, publish)
	if !publish || c.rawSink == nil {
		return
	}
	parentKS, ok := c.LookupParent(id)
	if !ok {
		return
	}
	// Route the RawObject (non-Flux) children to the orchestrator's output sink,
	// reusing emit.Children's parse — they group under the parent KS in the
	// build output, the same as the deleted post-run pass produced.
	for _, child := range children {
		if _, raw := child.Obj.(*manifest.RawObject); raw {
			c.rawSink(id, parentKS, child.Doc)
		}
	}
}

// collectDeps assembles the dependency refs whose readiness must precede
// this ResourceSet: explicit spec.dependsOn entries (carrying any CEL
// ReadyExpr), each named spec.inputsFrom RSIP, and the structural parent
// KS that renders us (so any parent-render-time spec mutations land
// first). A selector-only inputsFrom (ref.Name == "") adds no id edge —
// its matching RSIPs may be emitted by an unknown producer; the
// drain-rerun mechanism (WantsDrainRerun) re-expands the RS at the
// structural fixpoint instead.
func (c *Controller) collectDeps(rs *manifest.ResourceSet) []manifest.DependencyRef {
	parent, hasParent := c.LookupParent(rs.Named())
	capacity := len(rs.DependsOn) + len(rs.InputsFrom)
	if hasParent {
		capacity++
	}
	deps := make([]manifest.DependencyRef, capacity)
	for i, dep := range rs.DependsOn {
		ns := dep.Namespace
		if manifest.IsClusterScopedKind(dep.Kind) {
			ns = ""
		} else if ns == "" {
			ns = rs.Namespace
		}
		deps[i] = manifest.DependencyRef{
			Kind: dep.Kind, Namespace: ns, Name: dep.Name,
			ReadyExpr: dep.ReadyExpr,
		}
	}
	n := len(rs.DependsOn)
	for _, ref := range rs.InputsFrom {
		if ref.Name == "" {
			continue // selector-only: no nameable RSIP to park on
		}
		// InputProviderReference is same-namespace by spec — RSIPs live in
		// the ResourceSet's own namespace.
		deps[n].NamedResource = manifest.NamedResource{
			Kind:      manifest.KindResourceSetInputProvider,
			Namespace: rs.Namespace,
			Name:      ref.Name,
		}
		n++
	}
	if hasParent {
		deps[n].NamedResource = parent
		n++
	}
	return deps[:n]
}

// WantsDrainRerun reports whether the ResourceSet id has a selector-only
// inputsFrom (ref.Name == "") — the case where its matching RSIPs may be
// emitted by an unknown producer, so it has no nameable dependency to
// park on. Such a RS asks the scheduler to re-run it at the structural
// fixpoint (it is wired as the scheduler's rerun-at-drain predicate) so it
// re-expands against the now-complete RSIP set. Returns false when id is not
// a RS in the store.
func (c *Controller) WantsDrainRerun(id manifest.NamedResource) bool {
	rs, ok := c.Store.Get[*manifest.ResourceSet](id)
	if !ok {
		return false
	}
	for _, ref := range rs.InputsFrom {
		if ref.Name == "" {
			return true
		}
	}
	return false
}
