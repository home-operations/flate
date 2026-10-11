package kustomization

import "github.com/home-operations/flate/pkg/manifest"

func (c *Controller) emitRenderedChildren(id manifest.NamedResource, docs []map[string]any, publish bool) {
	c.Emit(id, docs, c.WipeSecrets, publish)
}
