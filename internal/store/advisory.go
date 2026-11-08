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
//
//nolint:gocritic // The SDK preserves this value input and its snapshot boundary.
func (r *ReadSet) RecordSuppliedOverride(override SuppliedOverride) {
	if slices.ContainsFunc(r.advisories.Overrides, func(current SuppliedOverride) bool {
		return current.Resource == override.Resource && current.SupplyPath == override.SupplyPath &&
			slices.Equal(current.Keys, override.Keys)
	}) {
		return
	}
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
