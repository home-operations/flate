package kustomization

import (
	"github.com/home-operations/flate/pkg/controllers/emit"
	"github.com/home-operations/flate/pkg/manifest"
)

// emitRenderedChildren parses the rendered docs and lands them in the
// store via the shared two-pass emission helper (see emit.Children for
// the full pass-ordering / publish-gate / SOPS-wipe rationale). KS and
// the ResourceSet controller share this byte-identical path.
func (c *Controller) emitRenderedChildren(id manifest.NamedResource, docs []map[string]any, publish bool) {
	emit.Children(c.Controller, c.WipeSecrets, id, docs, publish)
}
