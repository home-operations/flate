package base

import (
	"slices"

	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

// ParseChildren retains authoritative sanitized documents, including invalid typed output.
// The returned objects and maps MUST remain immutable after publication.
func ParseChildren(parent manifest.NamedResource, docs []map[string]any, opts manifest.ParseDocOptions) ([]store.Child, []map[string]any) {
	children := make([]store.Child, 0, len(docs))
	retained := false
	for i, doc := range docs {
		obj, authoritative, changed, err := manifest.ParseDoc(doc, opts)
		if changed {
			if !retained {
				docs = slices.Clone(docs)
				retained = true
			}
			docs[i] = authoritative
		}
		if err != nil || obj == nil || obj.Named() == parent {
			continue
		}
		children = append(children, store.Child{Obj: obj, Doc: authoritative})
	}
	return children, docs
}

// Emit extends changed-only selection before the single coherent publication.
func (c *Controller) Emit(id manifest.NamedResource, next *store.Emission) {
	original := next
	for i, child := range next.Children {
		if child.Obj == nil || child.Obj.Named() == id {
			continue
		}
		obj := manifest.MapDependsOn(child.Obj, c.dependencySubstitutions.Resolve, nil)
		if obj != child.Obj {
			if next == original {
				cloned := *next
				cloned.Children = slices.Clone(next.Children)
				next = &cloned
			}
			next.Children[i].Obj = obj
		}
		c.KeepEmitted(id, obj, next.Reads)
	}
	c.Store.RecordEmitted(id, next)
}
