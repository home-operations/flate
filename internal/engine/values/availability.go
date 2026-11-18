package values

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/home-operations/flate/internal/engine/store"
	"github.com/home-operations/flate/pkg/manifest"
)

// Effect MUST distinguish unresolved results from each authorized operation.
type Effect uint8

// Effects are final decisions for one captured input.
const (
	Used Effect = iota + 1
	Omitted
	Deferred
	Failed
)

// Demand limits a decision to the material its consumer needs.
type Demand uint8

// Demands separate object existence from selected material.
const (
	ObjectDemand Demand = iota + 1
	SubstitutionDemand
	HelmDemand
	SourceDemand
)

// Settlement MUST capture terminal causes without retaining scheduler state.
type Settlement struct {
	Pending        []manifest.NamedResource
	Failure        error
	BlocksConsumer bool
}

// ResolveRequest MUST retain authored identity and the facts captured for one assessment.
type ResolveRequest struct {
	Reference       manifest.Reference
	Facts           store.ReferenceFacts
	Permission      missingPolicy
	Demand          Demand
	Settlement      Settlement
	Failure         error
	Ambient         bool
	Unavailable     error
	ProviderOnly    bool
	AnonymousDenied bool
}

// Result owns one captured decision. Its zero value never authorizes use.
type Result struct {
	ref          manifest.Reference
	effect       Effect
	err          error
	materialErr  error
	data         manifest.Data
	value        string
	variables    map[string]string
	unreadable   []string
	warnings     []manifest.Warning
	diagnostics  []manifest.Warning
	pending      []manifest.NamedResource
	cause        string
	present      bool
	admitted     bool
	materialized bool
	certain      bool
	required     bool
	knownKey     bool
	complete     bool
	objectAbsent bool
}

// Resolve MUST be the sole policy decision for captured consumer inputs.
func Resolve(request *ResolveRequest) Result {
	r := Result{ref: request.Reference, effect: Failed, data: request.Facts.Data, warnings: request.Facts.Warnings}
	r.resolve(request)
	required := !r.ref.Optional && !request.Facts.Private
	if required && r.effect == Failed {
		r.required = true
	} else if required && r.effect == Used {
		readable := r.materialized || len(r.variables) > len(r.unreadable)
		r.required = readable || r.present && !request.Facts.Generated && r.data == nil
	}
	return r
}

func (r *Result) resolve(request *ResolveRequest) {
	ref, input := &request.Reference, &request.Facts
	private := input.Private && input.Prepared && !input.Accepted
	if !private {
		declared := input.Declared || input.Generated
		r.present = declared || input.Original != nil || input.Data != nil
	}
	allowed := request.Permission != nil && request.Permission.AllowsKind(ref.Kind)
	r.admitted = r.present || ref.Optional || allowed
	if request.Failure != nil {
		r.err, r.materialErr = request.Failure, request.Failure
		return
	}
	if request.Demand == SourceDemand && request.Unavailable != nil {
		r.acquisition(request, private, allowed)
		return
	}
	if !ref.NamedResource.KindInfo().Data {
		r.err = manifest.NewError(manifest.ErrInvalidValuesReference, nil, fmt.Sprintf("unsupported kind %s", ref.Kind))
		r.materialErr = r.err
		return
	}
	demand := request.Demand
	if demand == 0 {
		demand = referenceDemand(ref.Use)
	}
	if private {
		r.data = nil
	}
	r.cause, r.complete = "not found", r.completeKeys(input)
	if err := r.capture(input, demand); err != nil {
		r.err, r.materialErr = err, err
		return
	}
	if r.settle(request, allowed, demand) {
		return
	}
	r.resolveDemand(request, demand, allowed)
}

func referenceDemand(use string) Demand {
	switch use {
	case substitutionUse:
		return SubstitutionDemand
	case "valuesFrom":
		return HelmDemand
	case "secretRef", "certSecretRef", "proxySecretRef":
		return SourceDemand
	default:
		return ObjectDemand
	}
}

func (r *Result) acquisition(request *ResolveRequest, private, allowed bool) {
	r.cause = request.Unavailable.Error()
	if errors.Is(request.Unavailable, context.Canceled) || errors.Is(request.Unavailable, context.DeadlineExceeded) ||
		errors.Is(request.Unavailable, manifest.ErrInput) {
		r.err, r.materialErr = request.Unavailable, request.Unavailable
		return
	}
	if len(request.Settlement.Pending) != 0 {
		r.effect, r.pending = Deferred, slices.Clone(request.Settlement.Pending)
		return
	}
	switch {
	case request.Facts.Generated && !private:
		r.effect = Omitted
	case request.Ambient:
		r.effect = Deferred
	case allowed, request.ProviderOnly && request.AnonymousDenied && r.ref.Name == "":
		r.effect = Omitted
	default:
		r.err = request.Unavailable
	}
}

func (r *Result) settle(request *ResolveRequest, allowed bool, demand Demand) bool {
	settlement := &request.Settlement
	unavailable := !r.materialized && !r.ref.Optional && !allowed
	if settlement.Failure != nil && demand == HelmDemand && !settlement.BlocksConsumer && unavailable {
		r.err = r.failure(!r.present)
		return true
	}
	permitted := r.ref.Optional || allowed
	if settlement.Failure != nil && (!r.materialized || settlement.BlocksConsumer) && !permitted {
		r.err = settlement.Failure
		return true
	}
	if len(settlement.Pending) != 0 {
		r.effect, r.pending = Deferred, slices.Clone(settlement.Pending)
		return true
	}
	return false
}

func (r *Result) resolveDemand(request *ResolveRequest, demand Demand, allowed bool) {
	if !r.present {
		switch {
		case r.ref.Optional, allowed:
			r.effect = Omitted
			if !r.ref.Optional {
				r.missingWarning(demand)
			}
		case demand == SourceDemand && request.Ambient:
			r.effect = Deferred
		case demand == ObjectDemand:
			r.objectAbsent = true
		default:
			r.err = r.failure(true)
		}
		return
	}
	switch demand {
	case ObjectDemand, SubstitutionDemand:
		r.effect = Used
	case HelmDemand:
		switch {
		case r.materialized:
			r.effect = Used
		case r.ref.Optional, r.knownKey:
			r.effect = Omitted
		case r.complete:
			r.err = r.failure(false)
		case request.Facts.Generated && request.Facts.Original == nil, allowed:
			r.effect = Omitted
		default:
			r.err = r.failure(false)
		}
	case SourceDemand:
		switch {
		case r.materialized:
			r.effect = Used
		case r.ref.Optional, allowed, request.Facts.Generated:
			r.effect = Omitted
		case request.Ambient:
			r.effect = Deferred
		default:
			r.err = r.failure(false)
		}
	}
}

func (r *Result) capture(input *store.ReferenceFacts, demand Demand) error {
	switch demand {
	case ObjectDemand:
		r.certain, r.materialized = r.present, r.data != nil
		return nil
	case SubstitutionDemand:
		return r.captureVariables(input)
	case HelmDemand, SourceDemand:
		return r.captureKey(input)
	}
	return nil
}

const producedCause = "is produced in the cluster with unknown data; supply a readable document"

func (r *Result) captureVariables(input *store.ReferenceFacts) error {
	if err := r.readVariables(&input.Keys); err != nil {
		return err
	}
	if r.present && input.Generated {
		for _, key := range input.Keys.Names {
			if _, found := r.variables[key]; !found {
				r.variables[key] = ""
				r.unreadable = append(r.unreadable, key)
			}
		}
	}
	slices.Sort(r.unreadable)
	r.unreadable = slices.Compact(r.unreadable)
	r.certain = r.present && r.data != nil && r.data.KeysKnown() && len(r.unreadable) == 0
	r.materialized = r.certain
	if input.Generated && input.Original == nil && !input.Keys.Complete {
		r.certain = false
	}
	switch {
	case len(r.unreadable) != 0:
		r.cause = fmt.Sprintf("key %q is unreadable offline; supply a readable document", r.unreadable[0])
	case input.Generated && !r.certain:
		r.cause = producedCause
	}
	return nil
}

func (r *Result) readVariables(keys *manifest.KeySet) error {
	data := r.data
	if data == nil {
		r.variables = make(map[string]string, len(keys.Names))
		return nil
	}
	var projection struct {
		capacity   int
		firstKey   string
		firstErr   error
		unreadable []string
		variables  map[string]string
	}
	projection.capacity, projection.unreadable = len(keys.Names), r.unreadable
	sequence := data.Keys()
	visit := func(key string) bool {
		if projection.variables == nil {
			projection.capacity++
			return true
		}
		value, err := data.Value(key)
		if err != nil {
			if projection.firstErr == nil || key < projection.firstKey {
				projection.firstKey, projection.firstErr = key, err
			}
			return true
		}
		projection.variables[key] = strings.ReplaceAll(value.Value, "\n", "")
		if value.Encrypted {
			projection.unreadable = append(projection.unreadable, key)
		}
		return true
	}
	if !keys.Complete {
		sequence(visit)
	}
	projection.variables = make(map[string]string, projection.capacity)
	r.variables = projection.variables
	sequence(visit)
	r.unreadable = projection.unreadable
	return projection.firstErr
}

func (r *Result) captureKey(input *store.ReferenceFacts) error {
	if r.data != nil {
		value, err := r.data.Value(r.ref.Key)
		if err != nil {
			return err
		}
		r.knownKey = value.Found
		if value.Found {
			r.selectValue(&value)
			return nil
		}
	}
	r.knownKey = r.present && input.Generated && input.Original == nil && slices.Contains(input.Keys.Names, r.ref.Key)
	switch {
	case !r.present:
		r.cause = "not found"
	case r.knownKey:
		r.unreadable = []string{r.ref.Key}
		r.cause = producedCause
	case r.complete:
		r.cause = fmt.Sprintf("key %q not found", r.ref.Key)
	case input.Generated:
		r.cause = producedCause
	default:
		r.cause = "has encrypted keys with unknown names; supply a readable document"
	}
	return nil
}

func (r *Result) selectValue(value *manifest.DataValue) {
	if value.Encrypted {
		r.unreadable = []string{r.ref.Key}
		r.cause = fmt.Sprintf("key %q is SOPS-encrypted and unreadable; supply a readable document", r.ref.Key)
		return
	}
	r.present, r.admitted = true, true
	r.value, r.materialized, r.certain = value.Value, true, true
}

func (r *Result) completeKeys(input *store.ReferenceFacts) bool {
	if input.Original != nil {
		return input.Original.KeysKnown()
	}
	if input.Generated {
		return input.Keys.Complete
	}
	return input.Keys.Complete || r.data != nil && r.data.KeysKnown()
}

func (r *Result) failure(absent bool) error {
	category, cause := manifest.ErrInvalidValuesReference, r.cause
	if absent {
		category = manifest.ErrObjectNotFound
		cause += "; supply a readable document"
	}
	return manifest.NewError(category, errors.New(cause), fmt.Sprintf("%s %s %s/%s %s",
		r.ref.Use, r.ref.Kind, r.ref.Namespace, r.ref.Name, cause))
}

func (r *Result) missingWarning(demand Demand) {
	var warning manifest.Warning
	switch demand {
	case SubstitutionDemand:
		warning = manifest.Warning{
			Category: manifest.WarnUnresolvedSubstitution,
			Message: fmt.Sprintf(
				"substituteFrom %s %q could not be read offline; its variables are unavailable, so their ${VAR} references may remain unsubstituted or resolve to defaults or empty values",
				r.ref.Kind,
				r.ref.Name,
			),
		}
	case HelmDemand:
		warning = manifest.Warning{
			Category: manifest.WarnOmittedValues,
			Message: fmt.Sprintf(
				"required valuesFrom %s/%s/%s omitted from offline render",
				r.ref.Kind,
				r.ref.Namespace,
				r.ref.Name,
			),
		}
	case ObjectDemand, SourceDemand:
		return
	}
	r.diagnostics = []manifest.Warning{warning}
}

// Effect MUST be consumed without deriving permission again.
func (r *Result) Effect() Effect { return r.effect }

// Err MUST preserve captured errors; object absence is formatted from immutable decision facts.
func (r *Result) Err() error {
	if r.objectAbsent {
		return r.failure(true)
	}
	return r.err
}

// MaterialError excludes unreadable data from malformed consumed material.
func (r *Result) MaterialError() error { return r.materialErr }

// ID retains the authored reference identity even when its data is absent.
func (r *Result) ID() manifest.NamedResource { return r.ref.NamedResource }

// Position retains the original index before declaration filtering.
func (r *Result) Position() int { return r.ref.Position }

// Reference MUST remain immutable after capture.
func (r *Result) Reference() manifest.Reference { return r.ref }

// Value never exposes ciphertext as readable data.
func (r *Result) Value() string { return r.value }

// UnreadableKeys borrows sorted names that MUST remain immutable.
func (r *Result) UnreadableKeys() []string { return r.unreadable }

// Pending borrows unsettled identities that MUST remain immutable.
func (r *Result) Pending() []manifest.NamedResource { return r.pending }

// Cause retains the actionable unavailable-material reason.
func (r *Result) Cause() string { return r.cause }

// Present includes qualifying production without inventing data.
func (r *Result) Present() bool { return r.present }

// Admitted projects invocation permission without waiting or warning publication.
func (r *Result) Admitted() bool { return r.admitted }

// Materialized answers only the captured demand, including object presence for ObjectDemand.
func (r *Result) Materialized() bool { return r.materialized }

// RequiresData distinguishes required material from satisfied unreadable projections.
func (r *Result) RequiresData() bool { return r.required }

// NeedsInput does not treat unknown key coverage as absent identity.
func (r *Result) NeedsInput() bool { return !r.present }

// CanPrepare requires certainty before speculative construction.
func (r *Result) CanPrepare() bool { return r.effect == Used && r.certain }

// StaticCertain never treats a projected empty value as a known source path.
func (r *Result) StaticCertain() bool { return r.effect == Used && r.certain }

// UnreadableInput retains required substitution provenance independently of warnings.
func (r *Result) UnreadableInput() bool {
	if r.ref.Use != substitutionUse || r.certain {
		return false
	}
	return r.effect == Used || r.effect == Omitted && !r.ref.Optional
}

// KnownKey distinguishes selected-key membership from readability.
func (r *Result) KnownKey() bool { return r.knownKey }

// KeysComplete permits a missing-key conclusion only for complete coverage.
func (r *Result) KeysComplete() bool { return r.complete }

// Data MUST remain immutable and preserves lazy selected-key decoding.
//
//nolint:ireturn // The data owner preserves lazy decoding across manifest implementations.
func (r *Result) Data() manifest.Data { return r.data }

// ReportWarnings MUST publish only through an accepted consuming assessment.
func (r *Result) ReportWarnings(reads *store.ReadSet) {
	if r.effect != Used && r.effect != Omitted {
		return
	}
	for i := range r.warnings {
		warning := r.warnings[i]
		warning.Resource = r.ref.NamedResource
		reads.AddWarning(warning)
	}
	for i := range r.diagnostics {
		warning := r.diagnostics[i]
		if r.ref.Use != substitutionUse {
			warning.Resource = reads.ID()
		}
		reads.AddWarning(warning)
	}
}

// OmittedIDs MUST preserve omission identity even for warning-free references.
func OmittedIDs(inputs []Result) []manifest.NamedResource {
	var ids []manifest.NamedResource
	for i := range inputs {
		if inputs[i].effect == Omitted {
			ids = append(ids, inputs[i].ref.NamedResource)
		}
	}
	slices.SortFunc(ids, manifest.NamedResource.Compare)
	return slices.Compact(ids)
}

const substitutionUse = "substituteFrom"
