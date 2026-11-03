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
func ParseChildren(
	parent manifest.BaseManifest,
	docs []manifest.DocOccurrence,
	opts manifest.ParseDocOptions,
	skipKinds []string,
) ([]store.Child, []manifest.DocOccurrence, []manifest.DocOccurrence) {
	var policy manifest.EmissionPolicy
	if producer, ok := parent.(emissionProducer); ok {
		policy = producer.EmissionPolicy()
		if policy.InternalNamespace {
			opts.DefaultNamespace = ""
		} else {
			opts.DefaultNamespace = policy.Namespace
		}
	}
	parser := childDocumentParser{parent: parent, docs: docs, opts: &opts, skipKinds: skipKinds, policy: &policy}
	return parser.parse()
}

type childDocumentParser struct {
	parent    manifest.BaseManifest
	docs      []manifest.DocOccurrence
	opts      *manifest.ParseDocOptions
	skipKinds []string
	policy    *manifest.EmissionPolicy
}

func (p *childDocumentParser) parse() ([]store.Child, []manifest.DocOccurrence, []manifest.DocOccurrence) {
	children := make([]store.Child, 0, len(p.docs))
	visible := p.docs
	filtered := false
	var failures []manifest.DocOccurrence
	for i, doc := range p.docs {
		output, admit := p.policy.SelectDocument(doc.Document, p.skipKinds)
		outcome := doc
		changed := false
		if admit {
			var child store.Child
			child, outcome, changed = p.prepare(doc)
			if child.Obj != nil {
				children = append(children, child)
			}
		}
		if outcome.Err != nil {
			failures = append(failures, outcome)
		}
		output = output && outcome.Document != nil
		if !filtered && (!output || changed || doc.Err == nil && outcome.Err != nil) {
			visible = make([]manifest.DocOccurrence, 0, len(p.docs))
			visible = append(visible, p.docs[:i]...)
			filtered = true
		}
		if filtered && output {
			visible = append(visible, outcome)
		}
	}
	return children, visible, failures
}

func (p *childDocumentParser) prepare(doc manifest.DocOccurrence) (store.Child, manifest.DocOccurrence, bool) {
	child, outcome, changed := Prepare(doc, *p.opts)
	obj := child.Obj
	if outcome.Err != nil || obj == nil {
		return store.Child{}, outcome, changed
	}
	if p.policy.InternalNamespace && obj.Named().Namespace == "" && manifest.KindOf(obj).Namespaced() {
		child.Obj = obj.WithNamespace(p.policy.Namespace)
	}
	if child.Obj.Named() == p.parent.Named() {
		return store.Child{}, outcome, changed
	}
	return child, outcome, changed
}
