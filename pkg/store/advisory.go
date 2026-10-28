package store

import (
	"slices"

	"github.com/home-operations/flate/pkg/manifest"
)

// SuppliedOverride records repository keys that take precedence over supplied data.
type SuppliedOverride struct {
	Resource   manifest.NamedResource
	SupplyPath string
	Keys       []string
}

// AdvisoryEvidence retains accepted facts for cluster advisory publication.
type AdvisoryEvidence struct {
	Overrides []SuppliedOverride
}

// RecordSuppliedOverride retains a copied precedence fact in this execution.
func (r *ReadSet) RecordSuppliedOverride(override SuppliedOverride) {
	override.Keys = slices.Clone(override.Keys)
	r.advisories.Overrides = append(r.advisories.Overrides, override)
}

// CarryAdvisories copies precedence facts after the caller proves equal consumed inputs.
func (r *ReadSet) CarryAdvisories(previous *ReadSet) {
	if previous == nil {
		return
	}
	for _, override := range previous.advisories.Overrides {
		r.RecordSuppliedOverride(override)
	}
}

// AdvisoryEvidence returns defensive copies of current valid accepted facts.
func (s *Store) AdvisoryEvidence() AdvisoryEvidence {
	var out AdvisoryEvidence
	for _, reads := range s.assessments() {
		for _, override := range reads.advisories.Overrides {
			override.Keys = slices.Clone(override.Keys)
			out.Overrides = append(out.Overrides, override)
		}
	}
	return out
}
