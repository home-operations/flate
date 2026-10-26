package manifest

import (
	"regexp"
	"strings"
)

func maybeHasEnvsubst(s string) bool {
	return strings.Contains(s, "${")
}

var envsubstReferenceRE = regexp.MustCompile(`\$\{[A-Za-z_][A-Za-z0-9_]*[}:]`)

// HasEnvsubstReference reports whether s contains envsubst reference syntax.
func HasEnvsubstReference(s string) bool {
	if !maybeHasEnvsubst(s) {
		return false
	}
	return envsubstReferenceRE.MatchString(s)
}
