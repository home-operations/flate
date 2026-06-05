package loader

import (
	"slices"
	"strings"

	"github.com/fluxcd/pkg/envsubst"

	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
	"github.com/home-operations/flate/pkg/values"
)

// ResolveDependsOnSubstitutions resolves bare ${VAR} references in
// Kustomization spec.dependsOn names/namespaces using the cluster's
// postBuild substitute values, mirroring how kustomize-controller
// substitutes a child KS's text (via its parent's postBuild) before the
// dependency is ever matched. flate builds its dependency graph from the
// raw file-parsed KS — where `dependsOn: 0-${CLUSTER_NAME}-config` (no
// default) survives ResolveEnvsubstDefaults and never matches the real
// `0-biohazard-config` — so without this pass the dependency is reported
// "not found".
//
// ${VAR:=default} forms are already collapsed at parse time
// (ResolveEnvsubstDefaults), so this pass only ever resolves *bare* vars,
// drawing values from the union of every discovered KS's
// spec.postBuild.substitute map. Two guards keep it from manufacturing a
// false dependency match:
//   - a var declared with conflicting values across Kustomizations (e.g.
//     per-cluster CLUSTER_NAME in a multi-cluster repo) is dropped from
//     the union and left literal;
//   - a var with no value leaves envsubst.Eval in its error path, so the
//     original name is kept verbatim rather than collapsed to empty.
//
// Run once, after the full KS set is discovered (so the union is complete
// and the conflict check is sound) and before the dependency graph is
// built. Idempotent: a KS's own name carries no var (templated names are
// skipped at load), so its store id is invariant, and a resolved name
// contains no ${ left to re-resolve.
func ResolveDependsOnSubstitutions(s *store.Store) {
	union := substituteUnion(s)
	if len(union) == 0 {
		return
	}
	mapping := func(name string) (string, bool) {
		v, ok := union[name]
		return v, ok
	}
	for _, ks := range store.ListAs[*manifest.Kustomization](s, manifest.KindKustomization) {
		resolved := resolveDeps(ks.DependsOn, mapping)
		if resolved == nil {
			continue
		}
		// Store immutability contract: clone, mutate the copy, replace.
		// The id is unchanged (own name has no var), so this is a
		// same-key replace.
		clone := ks.Clone()
		clone.DependsOn = resolved
		s.DeleteObject(ks.Named())
		s.AddObject(clone)
	}
}

// substituteUnion merges every Kustomization's postBuild.substitute map
// into one var→value lookup, dropping any key whose value conflicts
// across Kustomizations (left unresolved rather than guessed).
func substituteUnion(s *store.Store) map[string]string {
	union := map[string]string{}
	conflicting := map[string]struct{}{}
	for _, ks := range store.ListAs[*manifest.Kustomization](s, manifest.KindKustomization) {
		for k, v := range values.VarsMap(ks.PostBuildSubstitute) {
			if _, bad := conflicting[k]; bad {
				continue
			}
			if prev, seen := union[k]; seen && prev != v {
				delete(union, k)
				conflicting[k] = struct{}{}
				continue
			}
			union[k] = v
		}
	}
	return union
}

// resolveDeps resolves ${VAR} in each dependsOn name/namespace via
// mapping. Returns a fresh slice when anything changed, or nil when no
// reference needed resolution — so the caller only clones a KS it must.
// The input slice is never mutated.
func resolveDeps(deps []manifest.DependencyRef, mapping func(string) (string, bool)) []manifest.DependencyRef {
	var out []manifest.DependencyRef
	for i, dep := range deps {
		name := evalKeep(dep.Name, mapping)
		ns := evalKeep(dep.Namespace, mapping)
		if name == dep.Name && ns == dep.Namespace {
			continue
		}
		if out == nil {
			out = slices.Clone(deps)
		}
		out[i].Name = name
		out[i].Namespace = ns
	}
	return out
}

// evalKeep resolves ${VAR} in s via mapping, returning s unchanged when
// it has no template or when any referenced variable is unset
// (envsubst.Eval errors on a bare unset var) — so an unresolvable name is
// never collapsed to a partial or empty string.
func evalKeep(s string, mapping func(string) (string, bool)) string {
	if !strings.Contains(s, "${") {
		return s
	}
	out, err := envsubst.Eval(s, mapping)
	if err != nil {
		return s
	}
	return out
}
