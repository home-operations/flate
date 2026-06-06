package kustomize

import (
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/values"
)

// Prepare runs the standard pre-render dance for a Kustomization so it
// is ready to feed into RenderFlux:
//
//  1. Clone ks so subsequent mutations don't touch the store-canonical
//     copy (the immutability contract every flate controller honors —
//     see pkg/manifest/doc.go).
//  2. Expand spec.postBuild.substituteFrom references against the
//     supplied values provider so ks.PostBuildSubstitute reflects the
//     merged result a render would consume.
//
// Embedders rendering a single Kustomization without standing up the
// orchestrator's KS controller call Prepare then RenderFlux.
//
// Unlike helm.Prepare, Prepare takes no *values.Cache — and that
// asymmetry is intentional, not an oversight. The helm valuesFrom path
// yaml.Unmarshals each ref into a nested map[string]any, so a
// platform-wide values CM referenced by N HRs is worth parsing once and
// memoizing. The substituteFrom path resolves only FLAT string vars
// (CM/Secret data -> map[string]string via decodeBag), does no
// yaml.Unmarshal, and the per-KS work that dominates its cost (the
// newline strip + varname-regex validation + UpdatePostBuildSubstitutions)
// runs regardless of any lookup cache. There is no parsed tree to memoize.
// BenchmarkExpandPostBuildSubstituteReference_SharedCM (pkg/values)
// measures the whole path at single-digit microseconds per KS even for a
// large shared CM — sub-millisecond aggregate across a big repo — so
// threading a cache here would add wiring for no measurable gain.
func Prepare(ks *manifest.Kustomization, provider values.Provider) (*manifest.Kustomization, error) {
	ks = ks.Clone()
	if err := values.ExpandPostBuildSubstituteReference(ks, provider); err != nil {
		return nil, err
	}
	return ks, nil
}
