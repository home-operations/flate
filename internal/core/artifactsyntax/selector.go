// Package artifactsyntax parses ArtifactGenerator selectors and capture templates.
package artifactsyntax

import "strings"

// Selector retains the authored text, with its alias and optional path.
type Selector struct {
	Text    string
	Alias   string
	Path    string
	Marked  bool
	HasPath bool
}

// ParseSelector interprets a selector without applying field-specific rules.
func ParseSelector(text string) Selector {
	selector := Selector{Text: text}
	value, marked := strings.CutPrefix(text, "@")
	if marked {
		selector.Marked = true
		selector.Alias, selector.Path, selector.HasPath = strings.Cut(value, "/")
	}
	return selector
}

// StagedFile reports whether a file selector reads from the composed output.
func (s Selector) StagedFile() bool {
	return s.HasPath && s.Alias == "artifact"
}
