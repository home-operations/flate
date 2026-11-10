package depwait

import (
	"slices"

	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

// MissingPolicy MUST remain immutable after construction.
type MissingPolicy struct {
	kinds []string
}

// NewMissingPolicy MUST snapshot selections before concurrent assessment.
func NewMissingPolicy(kinds []string) *MissingPolicy {
	return &MissingPolicy{kinds: slices.Clone(kinds)}
}

// Missing MUST keep unreadability independent of absence permission.
//
//nolint:gocritic // References and captured facts remain independent value snapshots.
func (p *MissingPolicy) Missing(ref manifest.Reference, input store.ReferenceFacts) (bool, bool, bool) {
	data := ref.NamedResource.KindInfo().Data
	unreadable := data && ref.Use == "substituteFrom" && ref.Kind == manifest.KindSecret
	unreadable = unreadable && !ref.Optional && (input.Data == nil || !input.Data.HasKeys())
	producerAbsence := input.Private || input.Generated && !input.Offline
	allowed := ref.Optional || producerAbsence
	if p.AllowsKind(ref.Kind) {
		allowed = allowed || ref.NamedResource.IsCRD() || data && ref.Use == "valuesFrom"
	}
	return allowed, unreadable, producerAbsence
}

// AllowsKind MUST share the invocation selection with source acquisition.
func (p *MissingPolicy) AllowsKind(kind string) bool {
	return slices.Contains(p.kinds, kind)
}
