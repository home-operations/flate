package base

import (
	"context"
	"iter"

	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

type nodeController interface {
	Owns(manifest.NamedResource) bool
	ReconcileNode(context.Context, manifest.NamedResource, manifest.DrainLevel) *Execution
	PreviousRender(context.Context, manifest.BaseManifest, *store.ReadSet) (iter.Seq2[manifest.NamedResource, manifest.DiscoveryRecord], bool)
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
func (c *Controllers) ReconcileNode(ctx context.Context, id manifest.NamedResource, level manifest.DrainLevel) *Execution {
	if owner := c.owner(id); owner != nil {
		return owner.ReconcileNode(ctx, id, level)
	}
	return nil
}

// PreviousRender returns accepted history for the immutable declaration candidate.
func (c *Controllers) PreviousRender(ctx context.Context, obj manifest.BaseManifest, reads *store.ReadSet) (iter.Seq2[manifest.NamedResource, manifest.DiscoveryRecord], bool) {
	if owner := c.owner(reads.ID()); owner != nil {
		return owner.PreviousRender(ctx, obj, reads)
	}
	return nil, false
}

// PreviousRender reports unknown history for controllers without a renderer.
func (*Controller) PreviousRender(context.Context, manifest.BaseManifest, *store.ReadSet) (iter.Seq2[manifest.NamedResource, manifest.DiscoveryRecord], bool) {
	return nil, false
}

func (c *Controllers) owner(id manifest.NamedResource) nodeController {
	for _, owner := range [...]nodeController{c.Kustomization, c.HelmRelease, c.ResourceSet, c.Source, c.ArtifactGenerator} {
		if owner != nil && owner.Owns(id) {
			return owner
		}
	}
	return nil
}
