package base

import (
	"slices"

	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

type emissionProducer interface {
	EmissionPolicy() manifest.EmissionPolicy
}

// ParseChildren selects output and retains authoritative sanitized child documents.
// Producer namespace policy overrides opts.DefaultNamespace; raw parents use that default.
// The returned objects and maps MUST remain immutable after publication.
func (c *Controller) ParseChildren(parent manifest.BaseManifest, docs []map[string]any, opts manifest.ParseDocOptions) ([]store.Child, []map[string]any) {
	var policy manifest.EmissionPolicy
	if producer, ok := parent.(emissionProducer); ok {
		policy = producer.EmissionPolicy()
		if policy.InternalNamespace {
			opts.DefaultNamespace = ""
		} else {
			opts.DefaultNamespace = policy.Namespace
		}
	}
	children := make([]store.Child, 0, len(docs))
	retained := false
	var visible []map[string]any
	for i, doc := range docs {
		output, child := policy.SelectDocument(doc, c.skipKinds)
		if !output {
			if visible == nil {
				visible = make([]map[string]any, i, len(docs))
				copy(visible, docs[:i])
			}
		}
		if !child {
			continue
		}
		obj, authoritative, changed, err := manifest.ParseDoc(doc, opts)
		if changed {
			if !retained {
				docs = slices.Clone(docs)
				retained = true
			}
			docs[i] = authoritative
		}
		if visible != nil && output {
			visible = append(visible, authoritative)
		}
		if err != nil || obj == nil {
			continue
		}
		if policy.InternalNamespace && obj.Named().Namespace == "" && manifest.KindOf(obj).Namespaced() {
			obj = obj.WithNamespace(policy.Namespace)
		}
		if obj.Named() == parent.Named() {
			continue
		}
		children = append(children, store.Child{Obj: obj, Doc: authoritative})
	}
	if visible != nil {
		docs = visible
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
	if next.Complete {
		if info, ok := c.Store.GetStatus(id); ok && info.Status == store.StatusPending {
			message := store.ApplyingStatus(len(next.Manifests))
			if info != message {
				c.Store.UpdateStatus(id, message)
			}
		}
	}
	c.Store.RecordEmitted(id, next)
}
