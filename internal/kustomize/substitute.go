package kustomize

import (
	"fmt"

	"github.com/fluxcd/pkg/envsubst"
)

// Substitute expands data with Flux's envsubst engine. Strict mode rejects
// undefined variables without defaults; otherwise they expand to empty strings.
func Substitute(data []byte, vars map[string]string, strict bool) ([]byte, error) {
	out, err := envsubst.Eval(string(data), func(s string) (string, bool) {
		if strict {
			value, exists := vars[s]
			return value, exists
		}
		// Non-strict mode must report every name as defined to avoid envsubst's strict error path, matching kustomize-controller.
		return vars[s], true
	})
	if err != nil {
		return nil, fmt.Errorf("postBuild: %w", err)
	}
	return []byte(out), nil
}
