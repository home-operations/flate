package loader

import (
	"cmp"
	"slices"

	"github.com/home-operations/flate/internal/store"
	"github.com/home-operations/flate/pkg/manifest"
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
) store.ChildDocuments {
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

func (p *childDocumentParser) parse() store.ChildDocuments {
	children := make([]store.Child, 0, len(p.docs))
	visible := p.docs
	filtered := false
	var failures []manifest.DocOccurrence
	for i, doc := range p.docs {
		output, admit := p.policy.SelectDocument(doc.Document, p.skipKinds)
		var outcome manifest.DocOccurrence
		var changed bool
		var child store.Child
		if admit {
			child, outcome, changed = p.prepare(doc)
		} else {
			outcome, changed = manifest.SanitizeDocument(doc)
		}
		p.bindWarnings(&outcome)
		if child.Obj != nil && child.Obj.Named() != p.parent.Named() {
			child.Warnings = outcome.Warnings
			children = append(children, child)
		}
		p.docs[i] = outcome
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
	return store.ChildDocuments{Children: children, Visible: visible, Failures: failures}
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

func (p *childDocumentParser) bindWarnings(outcome *manifest.DocOccurrence) {
	if len(outcome.Warnings) != 0 {
		resource := manifest.DocIdentity(outcome.Document)
		if resource.Namespace == "" && resource.KindInfo().Namespaced() {
			resource.Namespace = cmp.Or(p.opts.DefaultNamespace, p.policy.Namespace)
		}
		for warningIndex := range outcome.Warnings {
			warning := outcome.Warnings[warningIndex]
			if warning.Resource != resource {
				outcome.Warnings = slices.Clone(outcome.Warnings)
				for j := range outcome.Warnings {
					outcome.Warnings[j].Resource = resource
				}
				break
			}
		}
	}
}
