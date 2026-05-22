package kustomize

import (
	"bytes"
	"errors"
	"fmt"
	"strings"

	"github.com/fluxcd/pkg/envsubst"
)

// Substitute replaces ${var} placeholders in data using the supplied
// vars map. Delegates to fluxcd/pkg/envsubst — the exact engine Flux
// source-controller uses — so behavior matches Flux bit-for-bit:
//
//   - $${VAR} passes through as literal ${VAR} (escape).
//   - ${VAR:-default}, ${VAR:=default}, ${VAR:+alt}, ${VAR:?msg}
//     handle the unset case per POSIX parameter expansion.
//   - Bash-only constructs like ${VAR[@]} or ${VAR%%:*} that aren't
//     recognized by envsubst are emitted literally, not erroneously
//     matched as bare variable references (a divergence the
//     previous regex-based implementation had).
//   - Undefined ${VAR} without a default returns a "postBuild:
//     variable %q is undefined and has no default" error, matching
//     the message flate has always surfaced.
func Substitute(data []byte, vars map[string]string) ([]byte, error) {
	out, err := envsubst.Eval(string(data), func(s string) (string, bool) {
		v, exists := vars[s]
		return v, exists
	})
	if err != nil {
		if name, ok := extractMissingVar(err); ok {
			return nil, fmt.Errorf("postBuild: variable %q is undefined and has no default", name)
		}
		return nil, fmt.Errorf("postBuild: %w", err)
	}
	return []byte(out), nil
}

// extractMissingVar pulls the variable name out of envsubst's
// "variable not set (strict mode): \"NAME\"" error so flate can
// surface the same diagnostic shape it always has.
func extractMissingVar(err error) (string, bool) {
	_, rest, found := strings.Cut(err.Error(), `(strict mode): "`)
	if !found {
		return "", false
	}
	name, _, ok := strings.Cut(rest, `"`)
	if !ok {
		return "", false
	}
	return name, true
}

// ErrSubstitution wraps any non-missing-var failure from the
// underlying envsubst engine (e.g. parse errors). Kept exported so
// callers can errors.Is against it if they need to distinguish
// envsubst failures from store / source errors in the future.
var ErrSubstitution = errors.New("substitution failed")

// HasSubstitutions returns whether data contains any ${...} placeholder.
// Useful when callers want to short-circuit costly substitution work.
func HasSubstitutions(data []byte) bool {
	return bytes.Contains(data, []byte("${"))
}
