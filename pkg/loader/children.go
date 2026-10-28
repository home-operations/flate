package loader

import (
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

type emissionProducer interface {
	EmissionPolicy() manifest.EmissionPolicy
}

// ParseChildren selects output and retains authoritative sanitized child documents.
// Producer namespace policy overrides opts.DefaultNamespace; raw parents use that default.
// The returned objects and maps MUST remain immutable after publication.
func ParseChildren(parent manifest.BaseManifest, docs []manifest.DocOccurrence, opts manifest.ParseDocOptions, skipKinds []string) ([]store.Child, []manifest.DocOccurrence, []manifest.DocOccurrence) {
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
		output, admit := policy.SelectDocument(doc.Document, skipKinds)
		outcome := doc
		changed := false
		if admit {
			var child store.Child
			child, outcome, changed = Prepare(doc, opts)
			if obj := child.Obj; outcome.Err == nil && obj != nil {
				if policy.InternalNamespace && obj.Named().Namespace == "" && manifest.KindOf(obj).Namespaced() {
					child.Obj = obj.WithNamespace(policy.Namespace)
				}
				if policy.SelectChild(child.Obj) && child.Obj.Named() != parent.Named() {
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
