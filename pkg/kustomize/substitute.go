package kustomize

import (
	"bytes"
	"errors"
	"fmt"

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
//   - Undefined ${VAR} without a default expands to the empty string,
//     matching kustomize-controller's default (strict mode is the
//     opt-in `StrictPostBuildSubstitutions` feature gate, off by
//     default). Returning the empty string with exists=true keeps
//     envsubst out of its strict-mode error path and lines flate up
//     with what real Flux renders against an incomplete substitute
//     map.
func Substitute(data []byte, vars map[string]string) ([]byte, error) {
	out, err := envsubst.Eval(string(data), func(s string) (string, bool) {
		return vars[s], true
	})
	if err != nil {
		return nil, fmt.Errorf("postBuild: %w", err)
	}
	return []byte(out), nil
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
