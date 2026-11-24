package artifactsyntax

import (
	"fmt"
	"regexp"
	"slices"
	"strings"

	"k8s.io/apimachinery/pkg/api/validate/content"
)

// ScanTemplate returns the capture at a grammar error so callers can report field-specific errors.
func ScanTemplate(template string, visit func(literal, capture string) error) (string, error) {
	rest := template
	for rest != "" {
		i := strings.IndexAny(rest, "{}")
		if i < 0 {
			return "", visit(rest, "")
		}
		offset := len(template) - len(rest) + i
		name, tail, ok := strings.Cut(rest[i+1:], "}")
		if rest[i] == '}' {
			return name, fmt.Errorf(
				"invalid template %q: capture ending at offset %d is missing opening brace",
				template,
				offset,
			)
		}
		if !ok {
			return name, fmt.Errorf(
				"invalid template %q: capture starting at offset %d is missing closing brace",
				template,
				offset,
			)
		}
		if name == "" {
			return name, fmt.Errorf("invalid template %q: empty capture variable", template)
		}
		if !validCapture(name) {
			return name, fmt.Errorf(
				"invalid template %q: capture variable %q must be a valid capture name matching [A-Za-z_][A-Za-z0-9_]*",
				template,
				name,
			)
		}
		if err := visit(rest[:i], name); err != nil {
			return name, err
		}
		rest = tail
	}
	return "", nil
}

// Token uses a one-based capture index so zero denotes a literal.
type Token struct {
	Literal string
	Capture int
}

// Tokens binds template captures in declaration order or against an existing capture list.
func Tokens(template string, captures []string, tokens []Token, declare bool) ([]Token, []string, error) {
	name, err := ScanTemplate(template, func(literal, capture string) error {
		if literal != "" {
			tokens = append(tokens, Token{Literal: literal})
		}
		if capture == "" {
			return nil
		}
		index := slices.Index(captures, capture)
		if declare {
			if index >= 0 || len(content.IsLabelKey(capture)) != 0 {
				return fmt.Errorf("invalid path capture %q", capture)
			}
			index = len(captures)
			captures = append(captures, capture)
		} else if index < 0 {
			return fmt.Errorf("undefined path capture %q", capture)
		}
		tokens = append(tokens, Token{Capture: index + 1})
		return nil
	})
	if err != nil {
		if declare {
			return nil, nil, fmt.Errorf("invalid or repeated pathPattern capture %q", name)
		}
		return nil, nil, err
	}
	return tokens, captures, nil
}

// CompilePathPattern matches whole directory paths and names their captures.
func CompilePathPattern(pattern string) (*regexp.Regexp, error) {
	var scratch [8]Token
	tokens, captures, err := Tokens(pattern, nil, scratch[:0], true)
	if err != nil {
		return nil, err
	}
	var expression strings.Builder
	expression.WriteByte('^')
	for _, token := range tokens {
		if token.Capture == 0 {
			expression.WriteString(regexp.QuoteMeta(token.Literal))
			continue
		}
		expression.WriteString("(?P<")
		expression.WriteString(captures[token.Capture-1])
		expression.WriteString(">[^/]+)")
	}
	expression.WriteByte('$')
	matcher, err := regexp.Compile(expression.String())
	if err != nil {
		return nil, fmt.Errorf("compile: %w", err)
	}
	return matcher, nil
}

func validCapture(name string) bool {
	return !(name == "" || strings.IndexFunc(name, func(r rune) bool {
		return r != '_' && (r < 'a' || r > 'z') && (r < 'A' || r > 'Z') && (r < '0' || r > '9')
	}) >= 0 || name[0] >= '0' && name[0] <= '9')
}
