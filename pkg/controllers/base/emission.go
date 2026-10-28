package base

import (
	"slices"

	"github.com/home-operations/flate/pkg/loader"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

// ParseChildren selects output and retains authoritative sanitized child documents.
// The returned objects and maps MUST remain immutable after publication.
func (c *Controller) ParseChildren(parent manifest.BaseManifest, docs []manifest.DocOccurrence, opts manifest.ParseDocOptions) ([]store.Child, []manifest.DocOccurrence, []manifest.DocOccurrence) {
	children, visible, failures := loader.ParseChildren(parent, docs, opts, c.SkipKinds)
	if c.Store != nil {
		for i, child := range children {
			if existing := c.Store.GetObject(child.Obj.Named()); existing != nil && manifest.Equal(existing, child.Obj) {
				children[i].Obj = existing
			}
		}
	}
	return children, visible, failures
}

// Emit extends changed-only selection before the single coherent publication.
func (c *Controller) Emit(id manifest.NamedResource, next *store.Emission, payloads map[manifest.NamedResource]*store.SourceArtifact) {
	children := next.Children
	cloned := false
	if next.Reads.Valid() {
		for i, child := range next.Children {
			if child.Obj == nil || child.Obj.Named() == id {
				continue
			}
			obj := manifest.MapDependsOn(child.Obj, c.DependencySubstitutions.Resolve, nil)
			if obj != child.Obj {
				if !cloned {
					next.Children = slices.Clone(children)
					cloned = true
				}
				next.Children[i].Obj = obj
			}
			c.KeepEmitted(id, next.Children[i], next.Reads)
		}
	}
	c.Store.RecordEmitted(id, next, payloads)
	if cloned && !next.Complete {
		next.Children = children
	}
	if next.Complete {
		if info, ok := c.Store.GetStatus(id); ok && info.Status == store.StatusPending {
			message := store.ApplyingStatus(len(next.Manifests))
			if info != message {
				c.Store.UpdateStatus(id, message)
			}
		}
	}
}
