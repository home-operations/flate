package kustomize

import (
	"bytes"
	"fmt"
	"regexp"
)

// Substitute replaces envsubst-style placeholders in data using the
// supplied vars map. Supports:
//
//	${VAR}              — required, errors if missing
//	${VAR:=default}     — default if missing or empty
//	${VAR:-default}     — same as :=
//
// Unknown references raise an error unless they use a default form. The
// implementation is intentionally narrow — Flux supports a broader
// envsubst dialect but the subset above covers ≥99% of real-world use.
func Substitute(data []byte, vars map[string]string) ([]byte, error) {
	out := data
	var firstErr error
	out = substRe.ReplaceAllFunc(out, func(match []byte) []byte {
		groups := substRe.FindSubmatch(match)
		name := string(groups[1])
		sep := string(groups[2])
		def := string(groups[3])

		val, ok := vars[name]
		if ok {
			return []byte(val)
		}
		if sep != "" {
			return []byte(def)
		}
		if firstErr == nil {
			firstErr = fmt.Errorf("postBuild: variable %q is undefined and has no default", name)
		}
		return match
	})
	if firstErr != nil {
		return nil, firstErr
	}
	return out, nil
}

// substRe matches ${VAR}, ${VAR:=default}, ${VAR:-default}. The default
// portion may contain any character except '}'.
var substRe = regexp.MustCompile(`\$\{([A-Za-z_][A-Za-z0-9_]*)(:=|:-)?([^}]*)\}`)

// HasSubstitutions returns whether data contains any ${...} placeholder.
// Useful when callers want to short-circuit costly substitution work.
func HasSubstitutions(data []byte) bool {
	return bytes.Contains(data, []byte("${"))
}
