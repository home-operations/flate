package manifest

import (
	"cmp"
	"slices"
)

// Warning is a categorized advisory collected alongside successful or failed runs.
// Owners record warnings on the Store for selection and diagnostic presentation.
type Warning struct {
	// Resource is the object the warning concerns. The zero value means the
	// warning is render-global (not tied to one object), e.g. an empty --path
	// scan or a --path/--path-orig misconfiguration.
	Resource NamedResource
	// Category is a stable machine-readable code (one of the Warn* constants).
	// Consumers switch/filter on this; it never carries free text.
	Category string
	// Message is a one-line human-readable summary.
	Message string
	// Detail is an optional structured payload, e.g. the dotted value key paths
	// a WarnStaleValues advisory lists.
	Detail []string
	// Count is the number of distinct advisory origins sharing this payload,
	// set by the collector. Repeated submissions by one origin count once.
	Count int
}

// Warning category codes. Stable identifiers a consumer filters on.
const (
	// WarnConflictingProducers: unequal accepted contributions share an identity.
	// Detail lists every contributor in deterministic settlement order.
	WarnConflictingProducers = "ConflictingProducers"
	// WarnMissingCRD: an absent CRD dependency was accepted for a final Ready
	// ResourceSet with an artifact. Message names its distinct dependents.
	WarnMissingCRD = "MissingCRD"
	// WarnStaleValues: a HelmRelease sets top-level values no chart template
	// references (#744). Detail = the unused keys.
	WarnStaleValues = "StaleValues"
	// WarnOmittedValues: a required HelmRelease valuesFrom reference was
	// unavailable offline, so the release rendered with the remaining values.
	WarnOmittedValues = "OmittedValues"
	// WarnIncompleteInputCoverage reports statically opaque Kustomize configuration inputs.
	WarnIncompleteInputCoverage = "IncompleteInputCoverage"
	// WarnEmptyScan: no Flux Kustomization/HelmRelease objects were found under
	// --path (likely a wrong path). Render-global.
	WarnEmptyScan = "EmptyScan"
	// WarnPathConfig: a --path / --path-orig misconfiguration (same root, or no
	// detected changes). Render-global.
	WarnPathConfig = "PathConfig"
	// WarnUnresolvedSubstitution: a spec.postBuild.substituteFrom Secret could
	// not be read offline (absent, or an ExternalSecret-synthesized empty
	// target), so the ${VAR}s it would supply render as the empty string —
	// dependent resources may then fail schema/template validation and drop
	// out of the render. Render-global (Message names the Secret) so the many
	// per-namespace KSes a cluster-config KS re-emits collapse to one line.
	WarnUnresolvedSubstitution = "UnresolvedSubstitution"
	// WarnBootstrapAliases reports that multiple sources alias the working tree.
	WarnBootstrapAliases = "BootstrapAliases"

	// WarnSourceRefUnavailable reports that a declared ref cannot be satisfied locally.
	WarnSourceRefUnavailable = "SourceRefUnavailable"

	// WarnInputParseFailed reports that an advisory input parse failed.
	WarnInputParseFailed = "InputParseFailed"

	// WarnInputReadFailed reports that an advisory input read failed.
	WarnInputReadFailed = "InputReadFailed"

	// WarnSubstitutionPrecedence reports that repository substitution data overrides supplied data.
	WarnSubstitutionPrecedence = "SubstitutionPrecedence"

	// WarnDuplicateDocument reports that a later document shadows the same identity.
	WarnDuplicateDocument = "DuplicateDocument"

	// WarnSkippedSubmodule reports that a Git tree omits a submodule.
	WarnSkippedSubmodule = "SkippedSubmodule"

	// WarnOfflineInputProvider reports that a dynamic provider has no offline inputs.
	WarnOfflineInputProvider = "OfflineInputProvider"

	// WarnOrphanedResource reports that an unowned resource was downgraded.
	WarnOrphanedResource = "OrphanedResource"

	// WarnBaselineStatusUnavailable reports that Git status required conservative comparison.
	WarnBaselineStatusUnavailable = "BaselineStatusUnavailable"

	// WarnDependencyCycle reports that a dependency cycle prevents reconciliation.
	WarnDependencyCycle = "DependencyCycle"

	// WarnDiffSuppressed reports that a comparison was withheld.
	WarnDiffSuppressed = "DiffSuppressed"

	// WarnRenderPersistenceDisabled reports that the build lacks persistent render identity.
	WarnRenderPersistenceDisabled = "RenderPersistenceDisabled"

	// WarnCacheMaintenance reports that cache maintenance encountered an advisory error.
	WarnCacheMaintenance = "CacheMaintenance"

	// WarnExternalSource reports that a source is outside the local tree.
	WarnExternalSource = "ExternalSource"

	// WarnUnrelatedFailure reports that an out-of-scope resource failed.
	WarnUnrelatedFailure = "UnrelatedFailure"
)

// CompareWarning orders warnings by category, resource, message and ordered detail.
func CompareWarning(a, b Warning) int {
	return cmp.Or(
		cmp.Compare(a.Category, b.Category),
		a.Resource.Compare(b.Resource),
		cmp.Compare(a.Message, b.Message),
		slices.Compare(a.Detail, b.Detail),
	)
}
