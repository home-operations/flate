package manifest

import (
	"slices"
	"strings"

	"k8s.io/apimachinery/pkg/api/validate/content"
	apivalidation "k8s.io/apimachinery/pkg/api/validation"
)

type artifactToken struct {
	literal string
	capture int
}

const artifactMatchBudget = 4096

func (r *RawObject) artifactTargets(candidates []NamedResource) []ProducerTarget {
	entries, _ := r.Spec["artifacts"].([]any)
	var targets []ProducerTarget
	var captures []string
	pattern, _ := r.Spec["pathPattern"].(string)
	if len(candidates) != 0 && pattern != "" {
		alias, path, ok := strings.Cut(pattern, "/")
		if ok && strings.HasPrefix(alias, "@") && len(alias) > 1 {
			_, captures, _ = artifactTokens(path, nil, true)
		}
	}
	for _, entry := range entries {
		m, _ := entry.(map[string]any)
		name, _ := m["name"].(string)
		if name == "" {
			continue
		}
		if !strings.ContainsAny(name, "{}") {
			targets = append(targets, ProducerTarget{Group: SourceDomain, Kind: KindExternalArtifact, Namespace: r.Namespace, Name: name})
			continue
		}
		if len(captures) == 0 {
			continue
		}
		tokens, _, ok := artifactTokens(name, captures, false)
		if !ok {
			continue
		}
		bound := make([]string, len(captures))
		for _, candidate := range candidates {
			if candidate.Group != SourceDomain || candidate.Kind != KindExternalArtifact || candidate.Namespace != r.Namespace || len(apivalidation.NameIsDNSSubdomain(candidate.Name, false)) != 0 {
				continue
			}
			clear(bound)
			budget := artifactMatchBudget
			if artifactMatch(tokens, candidate.Name, bound, &budget) {
				targets = append(targets, ProducerTarget{NamedResource: candidate})
			}
		}
	}
	slices.SortFunc(targets, func(a, b ProducerTarget) int { return a.Compare(b.NamedResource) })
	return slices.CompactFunc(targets, func(a, b ProducerTarget) bool { return a.NamedResource == b.NamedResource })
}

func artifactTokens(template string, captures []string, declare bool) ([]artifactToken, []string, bool) {
	var tokens []artifactToken
	for template != "" {
		i := strings.IndexAny(template, "{}")
		if i < 0 {
			tokens = append(tokens, artifactToken{literal: template})
			break
		}
		if template[i] != '{' {
			return nil, nil, false
		}
		if i != 0 {
			tokens = append(tokens, artifactToken{literal: template[:i]})
		}
		name, rest, ok := strings.Cut(template[i+1:], "}")
		if !ok || name == "" || strings.IndexFunc(name, func(r rune) bool {
			return r != '_' && (r < 'a' || r > 'z') && (r < 'A' || r > 'Z') && (r < '0' || r > '9')
		}) >= 0 || name[0] >= '0' && name[0] <= '9' {
			return nil, nil, false
		}
		index := slices.Index(captures, name)
		if declare {
			if index >= 0 || len(content.IsLabelKey(name)) != 0 {
				return nil, nil, false
			}
			index = len(captures)
			captures = append(captures, name)
		} else if index < 0 {
			return nil, nil, false
		}
		tokens = append(tokens, artifactToken{capture: index + 1})
		template = rest
	}
	return tokens, captures, true
}

// The shared budget bounds search after tokenization to O(4096 * (T + N + 63)).
// T is the token count and N is the candidate byte length; captures are at most 63 bytes.
func artifactMatch(tokens []artifactToken, name string, bound []string, budget *int) bool {
	if *budget == 0 {
		return false
	}
	*budget--
	if len(tokens) == 0 {
		return name == ""
	}
	if tail := tokens[len(tokens)-1].literal; tail != "" && !strings.HasSuffix(name, tail) {
		return false
	}
	token := tokens[0]
	if token.capture == 0 {
		rest, ok := strings.CutPrefix(name, token.literal)
		return ok && artifactMatch(tokens[1:], rest, bound, budget)
	}
	i := token.capture - 1
	if bound[i] != "" {
		rest, ok := strings.CutPrefix(name, bound[i])
		return ok && artifactMatch(tokens[1:], rest, bound, budget)
	}
	minimum, occurrences := 0, 1
	for _, next := range tokens[1:] {
		switch {
		case next.capture == 0:
			minimum += len(next.literal)
		case next.capture == token.capture:
			occurrences++
		case bound[next.capture-1] != "":
			minimum += len(bound[next.capture-1])
		default:
			minimum++
		}
	}
	for end := 1; end <= min(63, (len(name)-minimum)/occurrences); end++ {
		if *budget == 0 {
			break
		}
		*budget--
		value := name[:end]
		if len(content.IsLabelValue(value)) != 0 {
			continue
		}
		bound[i] = value
		if artifactMatch(tokens[1:], name[end:], bound, budget) {
			return true
		}
	}
	bound[i] = ""
	return false
}
