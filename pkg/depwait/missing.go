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
		if ref.Optional || ref.NamedResource.IsCRD() && p.AllowsKind(ref.Kind) {
			return selection.OptionalInput
		}
		return selection.RequiredInput
	}
	result, err := values.Classify(ref, input)
	if err == nil {
		switch result.State() {
		case values.Readable, values.Encrypted, values.MissingKey:
			return selection.AvailableInput
		case values.Absent, values.Produced:
			if result.AdmitMissing(p) || result.State() == values.Produced {
				return selection.OptionalInput
			}
		}
	}
	return selection.RequiredInput
}

// AllowsKind MUST share the invocation selection with source acquisition.
func (p *MissingPolicy) AllowsKind(kind string) bool {
	return slices.Contains(p.kinds, kind)
}
