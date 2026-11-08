package store

import (
	"slices"

	"github.com/home-operations/flate/pkg/manifest"
)

// MatchesContent compares captured content without granting current authority.
// Both captures MUST have finished recording and retain their observation payloads.
func (r *ReadSet) MatchesContent(previous *ReadSet, id manifest.NamedResource) bool {
	if r == nil || previous == nil || r.store != previous.store {
		return false
	}
	current, before := r.findInput(id), previous.findInput(id)
	if current == nil || before == nil {
		return false
	}
	const content = capturedObject | capturedArtifact | capturedProductions | capturedObjects
	required := current.derived & content
	if required == 0 && !current.declaration || before.derived&required != required {
		return false
	}
	if required&capturedObject != 0 && !reconcileEqual(current.view.child.input(), before.view.child.input()) {
		return false
	}
	if current.declaration && (!before.declaration ||
		!manifest.Equal(current.view.declaration(), before.view.declaration())) {
		return false
	}
	details, previousDetails := current.details.Load(), before.details.Load()
	if required&capturedArtifact != 0 &&
		(details.artifactRead&previousDetails.artifactRead != details.artifactRead ||
			!artifactReadEqual(details.artifact, previousDetails.artifact, details.artifactRead)) {
		return false
	}
	if required&capturedProductions != 0 {
		productions, previousProductions := current.productions.Load(), before.productions.Load()
		if productions.index != previousProductions.index ||
			!slices.Equal(productions.items, previousProductions.items) {
			return false
		}
	}
	return required&capturedObjects == 0 || slices.EqualFunc(details.objects, previousDetails.objects, manifest.Equal)
}

func (r *ReadSet) findInput(id manifest.NamedResource) *Input {
	if r.last != nil && r.last.Ref.NamedResource == id {
		return r.last
	}
	if r.byID != nil {
		return r.byID[id]
	}
	for input := r.inputs; input != nil; input = input.next {
		if input.Ref.NamedResource == id {
			return input
		}
	}
	return nil
}
