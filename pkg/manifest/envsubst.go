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
	for i := 0; i < len(s)-1; i++ {
		if s[i] == '$' && s[i+1] == '{' {
			return true
		}
	}
	return false
}
