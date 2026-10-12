package kustomization

import (
	"encoding/json"
	"fmt"
	"math"
	"slices"
	"strings"

	yaml "go.yaml.in/yaml/v2"
	sigsyaml "sigs.k8s.io/yaml"

	"github.com/home-operations/flate/pkg/kustomize"
)

// Only expansion/escape text and scalars changed by JSON decoding need a
// round trip. Both map keys and values must be inspected.
func substituteDoc(doc map[string]any, vars map[string]string, strict bool) (map[string]any, error) {
	if !needsSubstitution(doc) {
		return doc, nil
	}
	// Direct YAML marshaling avoids an intermediate JSON decode with the same YAML 1.1 emitter.
	raw, err := yaml.Marshal(doc)
	if err != nil {
		return nil, fmt.Errorf("substitute: marshal doc: %w", err)
	}
	out, err := kustomize.Substitute(raw, vars, strict)
	if err != nil {
		return nil, err
	}
	data, err := sigsyaml.YAMLToJSON(out)
	if err != nil {
		return nil, fmt.Errorf("substitute: convert doc to JSON: %w", err)
	}
	var next map[string]any
	if err := json.Unmarshal(data, &next); err != nil {
		return nil, fmt.Errorf("substitute: unmarshal doc: %w", err)
	}
	return next, nil
}

// String-only walkers cannot detect scalars changed or rejected by JSON decoding.
func needsSubstitution(v any) bool {
	switch v := v.(type) {
	case string:
		return hasSubstitution(v)
	case float64:
		return math.IsNaN(v) || math.IsInf(v, 0) || v == 0 && math.Signbit(v)
	case int:
		return v < -(1<<53) || v > 1<<53
	case uint64:
		return v > 1<<53
	case map[string]any:
		for k, vv := range v {
			if hasSubstitution(k) || needsSubstitution(vv) {
				return true
			}
		}
	case []any:
		return slices.ContainsFunc(v, needsSubstitution)
	}
	return false
}

func hasSubstitution(s string) bool {
	for {
		i := strings.IndexByte(s, '$')
		if i < 0 || i+1 == len(s) {
			return false
		}
		if s[i+1] == '{' || s[i+1] == '$' {
			return true
		}
		s = s[i+1:]
	}
}
