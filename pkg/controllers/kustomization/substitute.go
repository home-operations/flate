package kustomization

import (
	"fmt"
	"strings"

	yaml "go.yaml.in/yaml/v4"

	"github.com/home-operations/flate/pkg/kustomize"
	"github.com/home-operations/flate/pkg/manifest"
)

// substituteDoc marshals a single manifest doc, runs envsubst over it,
// and unmarshals the result back. Per-doc substitution (rather than
// substitute-the-whole-blob) lets us honor Flux's
// "kustomize.toolkit.fluxcd.io/substitute: disabled" opt-out, which is
// scoped to individual resources. The marshal/unmarshal round-trip is
// load-bearing — it preserves Flux's YAML type-coercion semantics where
// `replicas: ${REPLICAS}` (plain scalar) round-trips through envsubst
// as int rather than string. Cheap pre-check on the decoded tree skips
// the round-trip for the (common) case of docs with no `${` anywhere.
func substituteDoc(doc map[string]any, vars map[string]string) (map[string]any, error) {
	if !manifest.AnyStringLeaf(doc, func(s string) bool { return strings.Contains(s, "${") }) {
		return doc, nil
	}
	raw, err := yaml.Marshal(doc)
	if err != nil {
		return nil, fmt.Errorf("substitute: marshal doc: %w", err)
	}
	out, err := kustomize.Substitute(raw, vars)
	if err != nil {
		return nil, err
	}
	var next map[string]any
	if err := yaml.Unmarshal(out, &next); err != nil {
		return nil, fmt.Errorf("substitute: unmarshal doc: %w", err)
	}
	return next, nil
}
