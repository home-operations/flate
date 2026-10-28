package values

import (
	"math"
	"slices"
	"strings"

	"github.com/fluxcd/pkg/envsubst/parse"
	yaml "go.yaml.in/yaml/v3"

	"github.com/home-operations/flate/pkg/manifest"
)

// NeedsSubstitution reports expansion syntax or scalars requiring the Flux JSON conversion.
func NeedsSubstitution(v any) bool {
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

// SubstitutionNames derives effective names from Flux syntax, or nil for unproved syntax.
func SubstitutionNames(docs []manifest.DocOccurrence, active bool) map[string]struct{} {
	names := make(map[string]struct{})
	if !active {
		return names
	}
	for _, occurrence := range docs {
		doc := occurrence.Document
		if occurrence.Err != nil || manifest.HasSubstituteDisabled(doc) || !NeedsSubstitution(doc) {
			continue
		}
		raw, err := yaml.Marshal(doc)
		if err != nil {
			return nil
		}
		tree, err := parse.Parse(string(raw))
		if err != nil || !collectSubstitutionNames(tree.Root, names) {
			return nil
		}
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
