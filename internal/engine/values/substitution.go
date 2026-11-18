package values

import (
	"math"
	"unicode/utf8"

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

// ResolvedSubstitutionActive MUST preserve evaluation when satisfied inputs have no readable names.
func ResolvedSubstitutionActive(inputs []Result) bool {
	for i := range inputs {
		result := &inputs[i]
		if result.ref.Use == substitutionUse && result.effect == Used && !result.certain {
			return true
		}
	}
	return false
}

// UnreadableSubstitutions MUST discard evidence replaced by later or inline values.
func UnreadableSubstitutions(ks *manifest.Kustomization, inputs []Result) map[string]manifest.Reference {
	var unreadable map[string]manifest.Reference
	for i := range inputs {
		result := &inputs[i]
		if result.effect != Used || result.ref.Use != substitutionUse {
			continue
		}
		for name := range result.variables {
			delete(unreadable, name)
		}
		for _, name := range result.unreadable {
			if unreadable == nil {
				unreadable = make(map[string]manifest.Reference)
			}
			ref := result.ref
			ref.Key = name
			unreadable[name] = ref
		}
	}
	if ks.PostBuild != nil {
		for name := range ks.PostBuild.Substitute {
			delete(unreadable, name)
		}
	}
	return unreadable
}

// UnreadableValueReferences excludes absent objects and keys proved missing.
func UnreadableValueReferences(inputs []Result) []manifest.Reference {
	var references []manifest.Reference
	for i := range inputs {
		result := &inputs[i]
		omitted := result.ref.Use == "valuesFrom" && result.effect == Omitted
		if !omitted || !result.present {
			continue
		}
		unreadable := !result.materialized && (result.knownKey || !result.complete)
		if unreadable {
			references = append(references, result.ref)
		}
	}
	return references
}

type substitutionClass uint8

const (
	substitutionLiteral substitutionClass = iota
	substitutionInspect
	substitutionRequired
)

func needsSubstitution(v any) bool {
	return classifySubstitution(v) == substitutionRequired
}

func classifySubstitution(v any) substitutionClass {
	var required bool
	switch v := v.(type) {
	case nil, bool:
		return substitutionLiteral
	case string:
		return classifySubstitutionString(v)
	case float64:
		required = math.IsNaN(v) || math.IsInf(v, 0) || v == 0 && math.Signbit(v)
	case int:
		required = v < -(1<<floatMantissaBits) || v > 1<<floatMantissaBits
	case uint64:
		required = v > 1<<floatMantissaBits
	case map[string]any:
		class := substitutionLiteral
		for k, vv := range v {
			next := classifySubstitutionString(k)
			if next == substitutionRequired {
				return next
			}
			class |= next
			next = classifySubstitution(vv)
			if next == substitutionRequired {
				return next
			}
			class |= next
		}
		return class
	case []any:
		class := substitutionLiteral
		for _, vv := range v {
			next := classifySubstitution(vv)
			if next == substitutionRequired {
				return next
			}
			class |= next
		}
		return class
	default:
		// Other representations MUST retain the projected conversion decision.
		return substitutionInspect
	}
	if required {
		return substitutionRequired
	}
	return substitutionLiteral
}

func classifySubstitutionString(value string) substitutionClass {
	var highBits byte
	for i := range len(value) {
		c := value[i]
		highBits |= c
		if c == '$' && i+1 < len(value) && (value[i+1] == '{' || value[i+1] == '$') {
			return substitutionRequired
		}
	}
	if highBits >= utf8.RuneSelf && !utf8.ValidString(value) {
		return substitutionInspect
	}
	return substitutionLiteral
}

// SubstitutionDocument keeps YAML framing and document exclusions identical across callers.
//
//nolint:gocritic // DocOccurrence preserves an immutable value snapshot across this boundary.
func SubstitutionDocument(occurrence manifest.DocOccurrence, active bool) ([]byte, error) {
	doc := occurrence.Document
	if !active || occurrence.Err != nil || manifest.HasSubstituteDisabled(doc) {
		return nil, nil //nolint:nilerr // Parse failures retain their existing outcome and never enter substitution.
	}
	class := classifySubstitution(doc)
	if class == substitutionLiteral {
		return nil, nil
	}
	doc = occurrence.SubstitutionDocument()
	if class == substitutionInspect && !needsSubstitution(doc) {
		return nil, nil
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
