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
	Overrides    []SuppliedOverride
	AgentSources []manifest.NamedResource
}

// RecordSuppliedOverride retains a copied precedence fact in this execution.
func (r *ReadSet) RecordSuppliedOverride(override SuppliedOverride) {
	override.Keys = slices.Clone(override.Keys)
	r.advisories.Overrides = append(r.advisories.Overrides, override)
}

// RecordLocalSSHAgent retains successful acquisition through the local SSH agent.
func (r *ReadSet) RecordLocalSSHAgent(source manifest.NamedResource) {
	if !slices.Contains(r.advisories.AgentSources, source) {
		r.advisories.AgentSources = append(r.advisories.AgentSources, source)
	}
}

// CarryAdvisories copies source acquisition facts after the caller proves equal consumed inputs.
func (r *ReadSet) CarryAdvisories(previous *ReadSet) {
	if previous == nil {
		return
	}
	for _, override := range previous.advisories.Overrides {
		r.RecordSuppliedOverride(override)
	}
	for _, source := range previous.advisories.AgentSources {
		r.RecordLocalSSHAgent(source)
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
		out.AgentSources = append(out.AgentSources, reads.advisories.AgentSources...)
	}
	return out
}
