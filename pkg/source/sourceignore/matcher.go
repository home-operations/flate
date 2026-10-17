// Package sourceignore builds Flux artifact-exclusion policy from in-tree
// .sourceignore files and caller-supplied spec.ignore patterns.
package sourceignore

import (
	"cmp"
	"errors"
	"fmt"
	"io/fs"
	"path"
	"path/filepath"
	"slices"
	"strings"

	flux "github.com/fluxcd/pkg/sourceignore"
	"github.com/fluxcd/pkg/sourceignore/gitignore"
)

// Matcher reports whether a path within a source tree is excluded from the
// artifact and retains the parsed ignore-file content. It is safe for concurrent reads.
type Matcher struct {
	matcher      gitignore.Matcher
	patterns     []gitignore.Pattern
	extra        []gitignore.Pattern
	withDefaults bool
	evidence     []policyFile
}

type policyFile struct {
	Path, Data string
	end        int
}

// NewFS builds Flux policy from a slash-named immutable filesystem.
// Nil files walks the input; a supplied list MUST contain every relative ignore file.
// Parent rules MUST precede nested rules, independent of filesystem listing order.
// Defaults apply only without parsed user patterns; extra patterns have final priority.
func NewFS(input fs.FS, files []string, extra string, withDefaults bool) (*Matcher, error) {
	m := &Matcher{withDefaults: withDefaults}
	depth := func(dir string) int {
		if dir == "." {
			return 0
		}
		return 1 + strings.Count(dir, "/")
	}
	compare := func(a, b string) int {
		a, b = path.Dir(a), path.Dir(b)
		return cmp.Or(cmp.Compare(depth(a), depth(b)), strings.Compare(a, b))
	}
	read := func(name string) error {
		data, err := fs.ReadFile(input, name)
		if errors.Is(err, fs.ErrNotExist) {
			return nil
		}
		if err != nil {
			return err
		}
		m.evidence = append(m.evidence, policyFile{Path: name, Data: string(data)})
		return nil
	}
	if files == nil {
		var walk func(string) error
		walk = func(dir string) error {
			entries, err := fs.ReadDir(input, dir)
			if err != nil {
				// fluxcd/pkg/sourceignore v0.19.0 LoadIgnorePatterns gives ignore-file read errors priority over listing errors.
				if readErr := read(path.Join(dir, flux.IgnoreFile)); readErr != nil {
					return readErr
				}
				return err
			}
			for _, entry := range entries {
				if entry.Name() == flux.IgnoreFile {
					if err := read(path.Join(dir, flux.IgnoreFile)); err != nil {
						return err
					}
					break
				}
			}
			for _, entry := range entries {
				if entry.IsDir() && entry.Name() != ".git" {
					if err := walk(path.Join(dir, entry.Name())); err != nil {
						return err
					}
				}
			}
			return nil
		}
		err := walk(".")
		if err != nil {
			return nil, fmt.Errorf("sourceignore load: %w", err)
		}
	} else {
		files = slices.Clone(files)
		slices.SortFunc(files, compare)
		for _, name := range files {
			if err := read(name); err != nil {
				return nil, fmt.Errorf("sourceignore load: %w", err)
			}
		}
	}
	slices.SortFunc(m.evidence, func(a, b policyFile) int {
		return compare(a.Path, b.Path)
	})
	for i, file := range m.evidence {
		var domain []string
		if dir := path.Dir(file.Path); dir != "." {
			domain = strings.Split(dir, "/")
		}
		patterns := flux.ReadPatterns(strings.NewReader(file.Data), domain)
		m.patterns = append(m.patterns, patterns...)
		m.evidence[i].end = len(m.patterns)
	}
	return m.WithExtra(extra), nil
}

// WithExtra replaces caller patterns while retaining the parsed ignore-file policy.
func (m *Matcher) WithExtra(extra string) *Matcher {
	next := *m
	next.extra = nil
	if strings.TrimSpace(extra) != "" {
		next.extra = flux.ReadPatterns(strings.NewReader(extra), nil)
	}
	next.matcher = next.buildMatcher(m.patterns)
	return &next
}

func (m *Matcher) buildMatcher(patterns []gitignore.Pattern) gitignore.Matcher {
	if len(m.extra) > 0 {
		patterns = slices.Concat(patterns, m.extra)
	}
	if m.withDefaults {
		if len(patterns) == 0 {
			return flux.NewDefaultMatcher(nil, nil)
		}
		patterns = append(flux.VCSPatterns(nil), patterns...)
	}
	return flux.NewMatcher(patterns)
}

// Evidence returns the ordered ignore-file paths and exact content parsed by NewFS.
func (m *Matcher) Evidence() []struct{ Path, Data string } {
	out := make([]struct{ Path, Data string }, len(m.evidence))
	for i, file := range m.evidence {
		out[i] = struct{ Path, Data string }{file.Path, file.Data}
	}
	return out
}

// Match reports whether rel (a path relative to the matcher's root, using the
// OS separator) is excluded. isDir distinguishes directory-only patterns.
func (m *Matcher) Match(rel string, isDir bool) bool {
	return m.matcher.Match(strings.Split(rel, string(filepath.Separator)), isDir)
}

// Select removes excluded regular-file names using captured policy.
// Non-nil files restrict policy to those ignore files without rereading them.
func Select[V any](policy *Matcher, candidates map[string]V, files []string) {
	matcher := policy.matcher
	if files != nil && slices.ContainsFunc(policy.evidence, func(file policyFile) bool {
		return !slices.Contains(files, file.Path)
	}) {
		var patterns []gitignore.Pattern
		start := 0
		for _, file := range policy.evidence {
			if slices.Contains(files, file.Path) {
				patterns = append(patterns, policy.patterns[start:file.end]...)
			}
			start = file.end
		}
		matcher = policy.buildMatcher(patterns)
	}
	for name := range candidates {
		if matcher.Match(strings.Split(name, "/"), false) {
			delete(candidates, name)
		}
	}
}

// PruneDir applies shared directory exclusions, including Helm directories during scans.
func PruneDir(name string, scan bool) bool {
	return name != "." && strings.HasPrefix(name, ".") || name == "node_modules" || scan && (name == "templates" || name == "crds")
}
