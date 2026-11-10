package values

import (
	"math"
	"slices"
	"strings"

	kustomizev1 "github.com/fluxcd/kustomize-controller/api/v1"
	"github.com/fluxcd/pkg/envsubst/parse"
	yaml "go.yaml.in/yaml/v4"

	"github.com/home-operations/flate/pkg/manifest"
)

// SubstitutionActive requires a non-nil Kustomization with references already expanded.
func SubstitutionActive(ks *manifest.Kustomization) bool {
	return ks.PostBuild != nil && len(ks.PostBuild.Substitute) != 0 ||
		ks.SubstituteStrategy() == kustomizev1.SubstituteStrategyAlways
}

// NeedsSubstitution reports expansion syntax or scalars requiring the Flux JSON conversion.
func NeedsSubstitution(v any) bool {
	switch v := v.(type) {
	case string:
		return hasSubstitution(v)
	case float64:
		return math.IsNaN(v) || math.IsInf(v, 0) || v == 0 && math.Signbit(v)
	case int:
		return v < -(1<<floatMantissaBits) || v > 1<<floatMantissaBits
	case uint64:
		return v > 1<<floatMantissaBits
	case map[string]any:
		for k, vv := range v {
			if hasSubstitution(k) || NeedsSubstitution(vv) {
				return true
			}
		}
	case []any:
		return slices.ContainsFunc(v, NeedsSubstitution)
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

// SubstitutionDocument keeps YAML framing and document exclusions identical across callers.
func SubstitutionDocument(occurrence manifest.DocOccurrence, active bool) ([]byte, error) {
	doc := occurrence.Document
	if !active || occurrence.Err != nil || manifest.HasSubstituteDisabled(doc) || !NeedsSubstitution(doc) {
		return nil, nil //nolint:nilerr // Parse failures retain their existing outcome and never enter substitution.
	}
	//nolint:wrapcheck // Shaping wraps this deferred error.
	return yaml.Marshal(doc)
}

// SubstitutionNames derives effective names from Flux syntax, or nil for unproved syntax.
func SubstitutionNames(data []byte) map[string]struct{} {
	names := make(map[string]struct{})
	tree, err := parse.Parse(string(data))
	if err != nil || !collectSubstitutionNames(tree.Root, names) {
		return nil
	}
	return names
}

func collectSubstitutionNames(node parse.Node, names map[string]struct{}) bool {
	switch node := node.(type) {
	case *parse.TextNode:
		return true
	case *parse.ListNode:
		for _, child := range node.Nodes {
			if !collectSubstitutionNames(child, names) {
				return false
			}
		}
	case *parse.FuncNode:
		names[node.Param] = struct{}{}
		// The pinned evaluator reads every argument before looking up Param.
		for _, arg := range node.Args {
			if !collectSubstitutionNames(arg, names) {
				return false
			}
		}
	default:
		return false
	}
	return true
}

const (
	floatMantissaBits = 53
)
