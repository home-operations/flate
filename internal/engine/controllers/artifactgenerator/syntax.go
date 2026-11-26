package artifactgenerator

import (
	"fmt"
	"path/filepath"
	"strings"

	"github.com/home-operations/flate/internal/core/artifactsyntax"
	"github.com/home-operations/flate/internal/engine/source/safepath"
)

type selectorUse uint8

const (
	fileSelector selectorUse = iota
	copySourceSelector
	destinationSelector
	patternSelector
	revisionSelector
)

type selectorKey struct {
	use  selectorUse
	text string
}

type selectorResult struct {
	selector artifactsyntax.Selector
	globErr  error
}

type selectorResults struct {
	first              [revisionSelector + 1]selectorResult
	more               map[selectorKey]selectorResult
	expandedCopySource selectorResult
}

func (s *selectorResults) lookup(text string, use selectorUse) (selectorResult, bool) {
	if s == nil {
		return selectorResult{}, false
	}
	if result := s.first[use]; result.selector.Marked && result.selector.Text == text {
		return result, true
	}
	result, ok := s.more[selectorKey{use, text}]
	return result, ok
}

func (s *selectorResults) resolve(text string, use selectorUse, retain bool) (selectorResult, error) {
	var expanded *selectorResult
	if !retain && use == copySourceSelector && s != nil {
		expanded = &s.expandedCopySource
		if expanded.selector.Marked && expanded.selector.Text == text {
			return *expanded, nil
		}
	}
	if result, ok := s.lookup(text, use); ok {
		return result, nil
	}
	selector := artifactsyntax.ParseSelector(text)
	if err := validateSelector(selector, use); err != nil {
		return selectorResult{}, err
	}
	result := selectorResult{selector: selector}
	if use == copySourceSelector {
		// The glob limit applies only when evaluation reaches this copy.
		result.globErr = validateGlob(selector.Path)
	}
	switch {
	case expanded != nil:
		*expanded = result
	case !retain:
	case !s.first[use].selector.Marked:
		s.first[use] = result
	default:
		if s.more == nil {
			s.more = make(map[selectorKey]selectorResult)
		}
		s.more[selectorKey{use, text}] = result
	}
	return result, nil
}

func (s *selectorResults) parsedFile(text string) artifactsyntax.Selector {
	if result, ok := s.lookup(text, fileSelector); ok {
		return result.selector
	}
	return artifactsyntax.ParseSelector(text)
}

func (s *selectorResults) revision(text string) string {
	result, _ := s.lookup(text, revisionSelector)
	return result.selector.Alias
}

func validateSelector(selector artifactsyntax.Selector, use selectorUse) error {
	switch use {
	case fileSelector:
		if !selector.Marked {
			return fmt.Errorf("path must start with '@'")
		}
		if !selector.HasPath || selector.Alias == "" || selector.Path == "" {
			return fmt.Errorf("path must be in the format '@<alias>/<path>'")
		}
		return nil
	case revisionSelector:
		if !selector.Marked || selector.HasPath || len(selector.Alias) > maxAliasLength ||
			!aliasName.MatchString(selector.Alias) {
			return fmt.Errorf("invalid revision selector %q", selector.Text)
		}
		return nil
	case copySourceSelector, patternSelector:
		if !selector.Marked {
			return fmt.Errorf("source must start with '@'")
		}
		if !selector.HasPath {
			return fmt.Errorf("source format must be '@alias/pattern'")
		}
	case destinationSelector:
		if !selector.StagedFile() {
			return fmt.Errorf("destination must start with '@artifact/'")
		}
	}
	if selector.Path == "" {
		return nil
	}
	if use == destinationSelector {
		name := filepath.ToSlash(selector.Path)
		switch {
		case !filepath.IsLocal(selector.Path):
			return fmt.Errorf("destination path must stay within the artifact root")
		case name == "..",
			strings.HasPrefix(name, "../"),
			strings.Contains(name, "/../"),
			strings.HasSuffix(name, "/.."):
			return fmt.Errorf("destination path must not contain '..'")
		}
	}
	_, err := safepath.ArchiveName(selector.Path)
	return err
}
