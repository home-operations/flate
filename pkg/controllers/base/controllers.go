package base

import (
	"context"
	"iter"
	"maps"
	"slices"

	"github.com/home-operations/flate/pkg/store"

	"github.com/home-operations/flate/pkg/schedule"

	"github.com/home-operations/flate/pkg/manifest"
)

type nodeController interface {
	Owns(manifest.NamedResource) bool
	CompleteOrphan(manifest.NamedResource, string) bool
	ReconcileNode(context.Context, manifest.NamedResource, manifest.DrainLevel, schedule.Result) *Execution
	PreviousRender(
		context.Context,
		manifest.BaseManifest,
		schedule.Result,
	) History
}

// History MUST retain its assessment through every possible preparation consumer.
type History struct {
	Assessment *Execution
	Records    iter.Seq2[manifest.NamedResource, manifest.DiscoveryRecord]
	Complete   bool
}

// Controllers binds objects to their controller for execution and render history.
// Bindings MUST remain immutable after dispatch begins.
type Controllers struct {
	Kustomization     nodeController
	HelmRelease       nodeController
	ResourceSet       nodeController
	Source            nodeController
	ArtifactGenerator nodeController
}

// ReconcileNode dispatches id to its owning controller.
func (c *Controllers) ReconcileNode(
	ctx context.Context,
	id manifest.NamedResource,
	level manifest.DrainLevel,
	previous schedule.Result,
) *Execution {
	if owner := c.owner(id); owner != nil {
		return owner.ReconcileNode(ctx, id, level, previous)
	}
	return nil
}

// PreviousRender returns accepted history for the immutable declaration candidate.
func (c *Controllers) PreviousRender(ctx context.Context, obj manifest.BaseManifest, previous schedule.Result) History {
	if obj != nil {
		if owner := c.owner(obj.Named()); owner != nil {
			return owner.PreviousRender(ctx, obj, previous)
		}
	}
	return History{}
}

// PreviousRender supplies no history until a concrete controller overrides it.
func (*Controller) PreviousRender(context.Context, manifest.BaseManifest, schedule.Result) History {
	return History{}
}

//nolint:ireturn // Heterogeneous controllers retain the consumer interface.
func (c *Controllers) owner(id manifest.NamedResource) nodeController {
	for _, owner := range [...]nodeController{c.Kustomization, c.HelmRelease, c.ResourceSet, c.Source, c.ArtifactGenerator} {
		if owner != nil && owner.Owns(id) {
			return owner
		}
	}
	return nil
}

// CompleteOrphans MUST run only after all reconcile bodies drain.
// Each owning controller retires its failure after the final ownership assessment.
func (c *Controllers) CompleteOrphans(orphans map[manifest.NamedResource]string, message string) {
	for _, id := range slices.SortedFunc(maps.Keys(orphans), manifest.NamedResource.Compare) {
		owner := c.owner(id)
		if owner == nil || !owner.CompleteOrphan(id, message) {
			delete(orphans, id)
		}
	}
}

// CompleteOrphan MUST run after task drain for an object owned by this controller.
func (c *Controller) CompleteOrphan(id manifest.NamedResource, message string) bool {
	if info, _ := c.Store.GetStatus(id); info.Status != store.StatusFailed {
		return false
	}
	c.Store.UpdateStatus(id, store.StatusInfo{Status: store.StatusReady, Message: message})
	return true
}
