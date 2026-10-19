package manifest

import (
	"regexp"
	"strings"
)

// Bare references and error-on-unset expressions must remain deferred to postBuild.
var envsubstDefaultRE = regexp.MustCompile(`\$\{[A-Za-z_][A-Za-z0-9_]*:[=-]([^}]*)\}`)

func resolveEnvsubstDefaults(s string) string {
	if !maybeHasEnvsubst(s) {
		return s
	}
	return envsubstDefaultRE.ReplaceAllString(s, "$1")
}

// maybeHasEnvsubst is a cheap precheck — most strings have no `${`
// at all, and ReplaceAllString / MatchString allocate even when nothing
// matches.
func maybeHasEnvsubst(s string) bool {
	return strings.Contains(s, "${")
}

// envsubstReferenceRE matches any unresolved envsubst reference —
// `${VAR}`, `${VAR:?msg}`, etc. — that survived
// resolveEnvsubstDefaults. The character class `[}:]` rules out
// random literal `${` followed by alphanumerics that aren't actually
// envsubst (rare but possible in shell/CEL strings).
var envsubstReferenceRE = regexp.MustCompile(`\$\{[A-Za-z_][A-Za-z0-9_]*[}:]`)

// HasEnvsubstReference reports whether s contains a `${VAR}` style
// reference that survived resolveEnvsubstDefaults — i.e. a bare
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
