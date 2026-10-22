// Package resourceset reconciles flux-operator ResourceSets as
// first-class DAG nodes: wait on dependsOn / inputsFrom RSIPs / the
// structural parent KS, render the RS via the resourceset package, and
// emit every child through the shared emission transaction.
package resourceset

import (
	"context"

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

type execution struct {
	*Controller
	run base.Execution
}

func (c *Controller) newExecution(id manifest.NamedResource) *execution {
	execution := &execution{Controller: c}
	execution.run.Init(c.Controller, id)
	return execution
}

// New constructs a ResourceSet controller.
func New(s *store.Store, t *task.Service, wipeSecrets bool) *Controller {
	return &Controller{
		Controller:  base.New(s, t, "resourceset"),
		WipeSecrets: wipeSecrets,
	}
}

// ReconcileNode captures one ResourceSet execution for the scheduler.
func (c *Controller) ReconcileNode(ctx context.Context, id manifest.NamedResource, level manifest.DrainLevel) *base.Execution {
	execution := c.newExecution(id)
	execution.run.Dispatch(ctx, level, execution.reconcile)
	return &execution.run
}

func (c *execution) reconcile(ctx context.Context, rs *manifest.ResourceSet, level manifest.DrainLevel) error {
	id := rs.Named()
	if err := c.run.PreflightError(id); err != nil {
		return err
	}
	c.SetPendingUnlessReady(id, "resolving inputs")

	deps := c.collectDeps(rs)
	if len(deps) > 0 {
		err := c.run.Require(ctx, deps, level)
		if err != nil {
			return err
		}
	}
	if err := c.run.PreflightError(id); err != nil {
		return err
	}

	var attempt *store.Emission
	defer func() {
		if attempt != nil {
			attempt.Finish()
		}
	}()
	handled := false
	docs, warnings, err := resourceset.Render(rs, resourceset.StoreResolver(&c.run.Reads), func(content any) (bool, error) {
		fp, hit, err := c.run.FingerprintDedup(id, content)
		handled = hit
		if hit || err != nil {
			return hit, err
		}
		_, bound := c.run.Reads.GetEmission(id)
		attempt = &bound
		attempt.Fingerprint = fp
		return false, nil
	})
	for _, warning := range warnings {
		c.run.Reads.AddWarning(c.run.Reads.ID(), warning)
	}
	if err != nil {
		c.Store.UpdateStatus(id, store.StatusFailed, "", err.Error())
		return err
	}
	if handled {
		return nil
	}
	attempt.Children, attempt.Manifests = base.ParseChildren(id, docs, manifest.ParseDocOptions{WipeSecrets: c.WipeSecrets})
	for i, child := range attempt.Children {
		if child.Obj.Named().Namespace == "" && manifest.KindOf(child.Obj).Namespaced() {
			attempt.Children[i].Obj = child.Obj.WithNamespace(id.Namespace)
		}
	}
	attempt.Complete = true
	c.run.Emit(id, attempt)
	return nil
}

// collectDeps gates explicit dependencies, named inputs and the structural parent.
// Authored dependsOn Ready=false still requests readiness here.
func (c *execution) collectDeps(rs *manifest.ResourceSet) []manifest.DependencyRef {
	parents := c.run.Reads.Productions(rs.Named(), nil)
	capacity := len(rs.DependsOn) + len(rs.InputsFrom)
	for _, p := range parents {
		if p.Owner != (manifest.NamedResource{}) && p.Owner != rs.Named() {
			capacity++
		}
	}
	deps := make([]manifest.DependencyRef, capacity)
	n := 0
	for ref := range manifest.References(rs) {
		if ref.Use == "dependsOn" || ref.Use == "inputsFrom" && ref.Name != "" {
			deps[n] = ref.DependencyRef
			n++
		}
	}
	deps = deps[:n]
	for _, p := range parents {
		if p.Owner != (manifest.NamedResource{}) && p.Owner != rs.Named() {
			deps = append(deps, manifest.DependencyRef{NamedResource: p.Owner})
		}
	}
	return deps
}
