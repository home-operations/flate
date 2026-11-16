package depwait

import (
	"slices"

	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/selection"
	"github.com/home-operations/flate/pkg/store"
	"github.com/home-operations/flate/pkg/values"
)

// MissingPolicy MUST remain immutable after construction.
type MissingPolicy struct {
	kinds []string
}

// NewMissingPolicy MUST snapshot selections before concurrent assessment.
func NewMissingPolicy(kinds []string) *MissingPolicy {
	return &MissingPolicy{kinds: slices.Clone(kinds)}
}

// Admission MUST project values policy without importing values into selection.
func (p *MissingPolicy) Admission(ref *manifest.Reference, input *store.ReferenceFacts) selection.InputAdmission {
	if !ref.NamedResource.KindInfo().Data {
		if ref.Optional || p.allowsMissingDeclaration(ref.NamedResource) {
			return selection.OptionalInput
		}
		return selection.RequiredInput
	}
	result := values.Resolve(
		&values.ResolveRequest{Reference: *ref, Facts: *input, Permission: p, Demand: values.ObjectDemand},
	)
	if result.Present() {
		return selection.AvailableInput
	}
	if result.Admitted() {
		return selection.OptionalInput
	}
	return selection.RequiredInput
}

// AllowsKind MUST share the invocation selection with source acquisition.
func (p *MissingPolicy) AllowsKind(kind string) bool {
	return slices.Contains(p.kinds, kind)
}

func (p *MissingPolicy) allowsMissingDeclaration(id manifest.NamedResource) bool {
	return id.IsCRD() && p.AllowsKind(id.Kind)
}
