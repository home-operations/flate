package base

import (
	"slices"

	"github.com/home-operations/flate/pkg/loader"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

type emissionProducer interface {
	EmissionPolicy() manifest.EmissionPolicy
}

// ParseChildren selects output and retains authoritative sanitized child documents.
// Producer namespace policy overrides opts.DefaultNamespace; raw parents use that default.
// The returned objects and maps MUST remain immutable after publication.
func (c *Controller) ParseChildren(parent manifest.BaseManifest, docs []manifest.DocOccurrence, opts manifest.ParseDocOptions) ([]store.Child, []manifest.DocOccurrence, []manifest.DocOccurrence) {
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
	visible := docs
	filtered := false
	var failures []manifest.DocOccurrence
	for i, doc := range docs {
		output, admit := policy.SelectDocument(doc.Document, c.SkipKinds)
		outcome := doc
		changed := false
		if admit {
			var child store.Child
			child, outcome, changed = loader.Prepare(doc, opts)
			if obj := child.Obj; outcome.Err == nil && obj != nil {
				if policy.InternalNamespace && obj.Named().Namespace == "" && manifest.KindOf(obj).Namespaced() {
					child.Obj = obj.WithNamespace(policy.Namespace)
				}
				if child.Obj.Named() != parent.Named() {
					children = append(children, child)
				}
			}
		}
		if outcome.Err != nil {
			failures = append(failures, outcome)
		}
		output = output && outcome.Document != nil
		if !filtered && (!output || changed || doc.Err == nil && outcome.Err != nil) {
			visible = make([]manifest.DocOccurrence, 0, len(docs))
			visible = append(visible, docs[:i]...)
			filtered = true
		}
		if filtered && output {
			visible = append(visible, outcome)
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
