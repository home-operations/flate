package manifest

import "regexp"

// envsubstDefaultRE matches POSIX-style parameter expansion patterns
// that carry an explicit default: `${VAR:=default}` and
// `${VAR:-default}`. Captures the default in group 2.
//
// `${VAR}` (no default) and `${VAR:?error}` (error on unset) are NOT
// matched — we have nothing to substitute and leave them as-is so
// downstream postBuild substitution can still fill them in.
//
// The default body permits any character except `}`, which matches
// kustomize-controller / envsubst behavior — nested expansions
// aren't a real concern in practice.
var envsubstDefaultRE = regexp.MustCompile(`\$\{[A-Za-z_][A-Za-z0-9_]*:[=-]([^}]+)\}`)

// ResolveEnvsubstDefaults applies envsubst-style defaults to s.
// `${VAR:=default}` and `${VAR:-default}` become `default`. Bare
// `${VAR}` (no default) is left untouched.
//
// Used by the parsers to pre-resolve defaults on flate-load-time
// fields (Kustomization spec.path, OCIRepository spec.ref.tag,
// dependsOn names, …) so flate can find local directories and
// remote refs even when the parent KS's postBuild.substitute hasn't
// supplied a value. Real Flux's reconcile would resolve the same
// pattern via postBuild substitution, just one phase later.
func ResolveEnvsubstDefaults(s string) string {
	if !maybeHasEnvsubst(s) {
		return s
	}
	return envsubstDefaultRE.ReplaceAllString(s, "$1")
}

// maybeHasEnvsubst is a cheap precheck — most strings have no `${`
// at all, and ReplaceAllString allocates even when nothing matches.
func maybeHasEnvsubst(s string) bool {
	for i := range len(s) - 1 {
		if s[i] == '$' && s[i+1] == '{' {
			return true
		}
	}
	return false
}

// envsubstReferenceRE matches any unresolved envsubst reference —
// `${VAR}`, `${VAR:?msg}`, etc. — that survived
// ResolveEnvsubstDefaults. The character class `[}:]` rules out
// random literal `${` followed by alphanumerics that aren't actually
// envsubst (rare but possible in shell/CEL strings).
var envsubstReferenceRE = regexp.MustCompile(`\$\{[A-Za-z_][A-Za-z0-9_]*[}:]`)

// HasEnvsubstReference reports whether s contains a `${VAR}` style
// reference that survived ResolveEnvsubstDefaults — i.e. a bare
// `${VAR}` with no default. Used by the file walker to detect
// template files whose metadata.name / namespace was meant to be
// substituted by a parent Kustomization's postBuild.substitute(From).
//
// Real Flux never sees such resources as CRs in-cluster: the K8s
// API would reject the `$` character in metadata.name, and Flux's
// reconcile model is "watch what's in the cluster." flate's
// file-driven discovery picks them up anyway and tries to reconcile,
// producing FAILED rows for resources that don't exist in any real
// sense. Skipping them at load time matches Flux's behavior — the
// substituted version emitted by the parent KS's render is the
// reconcilable one.
func HasEnvsubstReference(s string) bool {
	if !maybeHasEnvsubst(s) {
		return false
	}
	return envsubstReferenceRE.MatchString(s)
}
