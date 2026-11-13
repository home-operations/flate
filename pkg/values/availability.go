package values

import (
	"errors"
	"fmt"

	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

// State MUST describe one selected input without independent availability flags.
type State uint8

// States are mutually exclusive; zero MUST never grant readable data.
const (
	Readable State = iota + 1
	Absent
	Encrypted
	Produced
	MissingKey
)

// Disposition MUST preserve the captured cause when applying caller policy.
type Disposition uint8

// Effects MUST remain separate from the selected data state.
const (
	UseData Disposition = iota + 1
	Omit
	Unresolved
	Reject
)

// Result MUST retain the selected evidence, including readable siblings of encrypted keys.
type Result struct {
	warnings []manifest.Warning
	state    State
	id       manifest.NamedResource
	use      string
	optional bool
	data     manifest.Data
	value    string
	key      string
}

// Classify MUST resolve effective readable keys before interpreting unavailable evidence.
func Classify(ref *manifest.Reference, input *store.ReferenceFacts) (Result, error) {
	result := Result{
		warnings: input.Warnings,
		id:       ref.NamedResource,
		use:      ref.Use,
		optional: ref.Optional,
		data:     input.Data,
		key:      ref.Key,
	}
	if !ref.NamedResource.KindInfo().Data {
		return result, manifest.NewError(
			manifest.ErrInvalidValuesReference,
			nil,
			fmt.Sprintf("unsupported kind %s", ref.Kind),
		)
	}
	if ref.Key != "" && input.Data != nil {
		value, err := input.Data.Value(ref.Key)
		if err != nil {
			return result, err
		}
		if value.Found {
			result.state, result.value = Readable, value.Value
			return result, nil
		}
	}
	for i := range input.Warnings {
		warning := &input.Warnings[i]
		if warning.Category != manifest.WarnSOPSUnreadable {
			continue
		}
		key, err := unreadableWarning(warning, input.Data, ref.Key)
		if err != nil {
			return result, err
		}
		if key != "" && (result.key == "" || key < result.key) {
			result.key = key
		}
		if key != "" {
			result.state = Encrypted
		}
	}
	state := unresolvedState(input, ref.Key, result.state)
	if state == Absent && result.state == Encrypted {
		result.data = nil
	}
	result.state = state
	return result, nil
}

// ReportWarnings publishes classified evidence only through its consuming assessment.
func (r *Result) ReportWarnings(reads *store.ReadSet) {
	if r.state == Absent {
		return
	}
	for i := range r.warnings {
		warning := r.warnings[i]
		warning.Resource = r.id
		reads.AddWarning(warning)
	}
}

// State MUST be nonzero before a caller treats the result as classified.
func (r *Result) State() State { return r.state }

// Value MUST be consumed only when State reports Readable.
func (r *Result) Value() string { return r.value }

// Data MUST remain immutable, including readable siblings of unavailable keys.
//
//nolint:ireturn // The data owner preserves lazy decoding across manifest implementations.
func (r *Result) Data() manifest.Data { return r.data }

// Disposition MUST keep required missing keys strict regardless of permission.
func (r *Result) Disposition(permission missingPolicy) Disposition {
	if r.state == 0 {
		return Reject
	}
	if r.state == Readable {
		return UseData
	}
	if r.optional {
		return Omit
	}
	if r.use == substitutionUse {
		if permission.AllowsKind(r.id.Kind) && (r.state == Absent || r.state == Encrypted || r.state == Produced) {
			return Unresolved
		}
		if r.data != nil && r.data.HasKeys() {
			return UseData
		}
		if r.state == Encrypted || r.state == Produced || r.state == Absent && r.id.Kind == manifest.KindSecret {
			return Unresolved
		}
		return Reject
	}
	if r.state == Produced || r.state != MissingKey && permission.AllowsKind(r.id.Kind) {
		return Omit
	}
	return Reject
}

// Cause MUST distinguish unavailable keys from absent objects.
func (r *Result) Cause() string {
	switch r.state {
	case Readable:
		panic("values: readable data has no unavailable cause")
	case Absent:
		return "not found"
	case Encrypted:
		return fmt.Sprintf("key %q is SOPS-encrypted and unreadable; supply it with --supply", r.key)
	case Produced:
		return "is produced in the cluster with unknown data; supply it with --supply"
	case MissingKey:
		return fmt.Sprintf("key %q not found", r.key)
	default:
		panic("values: unavailable cause requires an unavailable result")
	}
}

// Failure MUST retain the domain category and Flux error chain.
func (r *Result) Failure() *manifest.Error {
	category := manifest.ErrInvalidValuesReference
	if r.state == Absent {
		category = manifest.ErrObjectNotFound
	}
	cause := r.Cause() + r.supplyGuidance()
	return manifest.NewError(category, errors.New(cause), fmt.Sprintf("%s %s %s/%s %s",
		r.use, r.id.Kind, r.id.Namespace, r.id.Name, cause))
}

// AdmitMissing MUST require matching kind permission for absent unresolved inputs.
func (r *Result) AdmitMissing(permission missingPolicy) bool {
	disposition := r.Disposition(permission)
	return disposition == Omit || disposition == Unresolved && (r.state != Absent || permission.AllowsKind(r.id.Kind))
}

func (r *Result) supplyGuidance() string {
	if r.state == Absent {
		return "; supply it with --supply"
	}
	return ""
}

func unreadableWarning(warning *manifest.Warning, data manifest.Data, requested string) (string, error) {
	first := ""
	for _, key := range warning.Detail {
		if requested != "" && key != requested {
			continue
		}
		if data != nil {
			value, err := data.Value(key)
			if err != nil {
				return "", err
			}
			if value.Found {
				continue
			}
		}
		if first == "" || key < first {
			first = key
		}
	}
	return first, nil
}

const substitutionUse = "substituteFrom"

func unresolvedState(input *store.ReferenceFacts, key string, state State) State {
	if state == Encrypted {
		private := input.Private && input.Prepared && !input.Accepted
		if private && (input.Data == nil || !input.Data.HasKeys()) {
			return Absent
		}
		return state
	}
	if key == "" && input.Data != nil {
		return Readable
	}
	if input.Original != nil {
		if key == "" {
			return Readable
		}
		return MissingKey
	}
	if input.Private || input.Generated {
		return Produced
	}
	if input.Data == nil || input.Offline {
		return Absent
	}
	if key == "" {
		return Readable
	}
	return MissingKey
}
