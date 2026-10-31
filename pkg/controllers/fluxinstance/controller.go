// Package fluxinstance publishes the operator's sync pair through ordinary emission.
package fluxinstance

import (
	"context"

	"github.com/home-operations/flate/pkg/controllers/base"
	"github.com/home-operations/flate/pkg/kustomize"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/store"
	"github.com/home-operations/flate/pkg/task"
)

// Controller reconciles FluxInstance sync declarations.
type Controller struct {
	*base.Controller
	Trees       *kustomize.TreeCache
	WipeSecrets bool
}

// New constructs a FluxInstance controller.
func New(s *store.Store, tasks *task.Service, trees *kustomize.TreeCache, wipeSecrets bool) *Controller {
	return &Controller{Controller: base.New(s, tasks), Trees: trees, WipeSecrets: wipeSecrets}
}

// Owns reports whether id belongs to the FluxInstance controller.
func (*Controller) Owns(id manifest.NamedResource) bool {
	return id.Group == manifest.FluxOperatorDomain && id.Kind == "FluxInstance"
}

type execution struct {
	*Controller
	run base.Execution
}

// ReconcileNode captures a FluxInstance sync execution for the scheduler.
func (c *Controller) ReconcileNode(ctx context.Context, id manifest.NamedResource, level manifest.DrainLevel) *base.Execution {
	ex := &execution{Controller: c}
	ex.run.Init(c.Controller, id)
	ex.run.Dispatch(ctx, level, ex, (*execution).reconcile)
	return &ex.run
}

func (c *execution) reconcile(ctx context.Context, obj *manifest.RawObject, level manifest.DrainLevel) error {
	obj, err := c.run.RequireRefresh(ctx, obj, manifest.GateInputs, level)
	if err != nil {
		return err
	}
	id := obj.Named()
	var attempt store.Emission
	defer attempt.Finish()
	fingerprint, handled, err := c.run.FingerprintDedup(id, obj, &attempt)
	if handled || err != nil {
		if err != nil {
			return err
		}
		return c.Trees.RecordRender(ctx, kustomize.RenderAcceptance{Owner: id, Receipt: attempt.Render, Records: attempt.Footprint(), DependencySubstitutions: c.DependencySubstitutions})
	}
	projection, err := obj.SyncProjection()
	if err != nil {
		return err
	}
	var docs []manifest.DocOccurrence
	if projection != nil {
		docs, attempt.Render, err = kustomize.Render(ctx, c.Trees, kustomize.RenderRequest{
			Documents: &kustomize.DocumentsInput{
				Documents: projection.Documents, Patches: projection.Patches, Namespace: projection.Namespace,
				Transformers: projection.Transformers, Subset: true,
			}, HeldSlot: c.Tasks, WipeSecrets: c.WipeSecrets, SkipKinds: c.SkipKinds,
			DependencySubstitutions: c.DependencySubstitutions,
		}, cache.RetryConfig{})
		if err != nil {
			return err
		}
	}
	attempt.Children, attempt.Manifests, attempt.Failures = c.ParseChildren(obj, docs, manifest.ParseDocOptions{WipeSecrets: c.WipeSecrets})
	attempt.Fingerprint, attempt.Complete = fingerprint, true
	if err := c.run.Emit(id, &attempt, nil); err != nil {
		return err
	}
	return c.Trees.RecordRender(ctx, kustomize.RenderAcceptance{Owner: id, Receipt: attempt.Render, Records: attempt.Footprint(), DependencySubstitutions: c.DependencySubstitutions})
}
