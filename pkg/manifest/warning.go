package manifest

import "cmp"

// Warning is a non-fatal render advisory: a user-actionable signal ABOUT the
// rendered output that flate surfaces at end-of-run and that a library consumer
// (e.g. konflate, which reads orchestrator.Result) can present in a PR diff.
//
// It is distinct from a failure (which blocks a resource and lands in
// Result.Failed) and from operational/diagnostic logs (submodule skips, cache
// resets, best-effort loader parse errors) which stay as plain slog lines — a
// Warning means "the render succeeded, but you probably want to look at this".
//
// Producers record Warnings on the Store; the orchestrator collects them into
// Result.Warnings. Consumers filter on the stable Category code rather than
// parsing Message.
type Warning struct {
	// Resource is the object the warning concerns. The zero value means the
	// warning is render-global (not tied to one object), e.g. an empty --path
	// scan or a --path/--path-orig misconfiguration.
	Resource NamedResource
	// Category is a stable machine-readable code (one of the Warn* constants).
	// Consumers switch/filter on this; it never carries free text.
	Category string
	// Message is a one-line human-readable summary for the footer.
	Message string
	// Detail is an optional structured payload — e.g. the dotted value key paths
	// a WarnStaleValues advisory lists.
	Detail []string
	// Count is the number of times an identical warning was emitted, set by the
	// collector's de-duplication (1 for a unique warning).
	Count int
}

// Warning category codes. Stable identifiers a consumer filters on.
const (
	// WarnStaleValues: a HelmRelease sets top-level values no chart template
	// references (#744). Detail = the unused keys.
	WarnStaleValues = "StaleValues"
	// WarnEmptyScan: no Flux Kustomization/HelmRelease objects were found under
	// --path (likely a wrong path). Render-global.
	WarnEmptyScan = "EmptyScan"
	// WarnPathConfig: a --path / --path-orig misconfiguration (same root, or no
	// detected changes). Render-global.
	WarnPathConfig = "PathConfig"
)

// CompareWarning orders warnings deterministically by category, then resource,
// then message — so the footer and Result.Warnings are stable across runs.
func CompareWarning(a, b Warning) int {
	return cmp.Or(
		cmp.Compare(a.Category, b.Category),
		a.Resource.Compare(b.Resource),
		cmp.Compare(a.Message, b.Message),
	)
}
