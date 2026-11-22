// Package sourceignore builds Flux artifact-exclusion policy from in-tree
// .sourceignore files and caller-supplied spec.ignore patterns.
package sourceignore

import (
	"cmp"
	"errors"
	"fmt"
	"io/fs"
	"path"
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
}

//nolint:gochecknoglobals // The default Flux patterns are immutable.
var defaultMatcher = flux.NewDefaultMatcher(nil, nil)

// NewFS builds Flux policy from a slash-named immutable filesystem.
// Nil files walks the input; a supplied list MUST contain every relative ignore file.
// Parent rules MUST precede nested rules, independent of filesystem listing order.
// Defaults apply only without parsed user patterns; extra patterns have final priority.
func NewFS(input fs.FS, files []string, extra string, withDefaults bool) (*Matcher, error) {
	m := &Matcher{withDefaults: withDefaults}
	if files == nil {
		err := m.loadPolicy(input, ".")
		if err != nil {
			return nil, fmt.Errorf("sourceignore load: %w", err)
		}
	} else {
		files = slices.Clone(files)
		slices.SortFunc(files, comparePolicyFiles)
		for _, name := range files {
			if err := m.readPolicy(input, name); err != nil {
				return nil, fmt.Errorf("sourceignore load: %w", err)
			}
		}
	}
	return m.compile(extra), nil
}

// NewRoot builds bucket policy from its root ignore object and declared extensions.
func NewRoot(data, extra string) *Matcher {
	m := &Matcher{evidence: []policyFile{{Path: flux.IgnoreFile, Data: data}}}
	return m.compile(extra)
}

func (m *Matcher) compile(extra string) *Matcher {
	slices.SortFunc(m.evidence, func(a, b policyFile) int {
		return comparePolicyFiles(a.Path, b.Path)
	})
	for _, file := range m.evidence {
		var domain []string
		if dir := path.Dir(file.Path); dir != "." {
			domain = strings.Split(dir, "/")
		}
		patterns := flux.ReadPatterns(strings.NewReader(file.Data), domain)
		m.patterns = append(m.patterns, patterns...)
	}
	return m.WithExtra(extra)
}

func policyDepth(dir string) int {
	if dir == "." {
		return 0
	}
	return 1 + strings.Count(dir, "/")
}

func comparePolicyFiles(a, b string) int {
	a, b = path.Dir(a), path.Dir(b)
	return cmp.Or(cmp.Compare(policyDepth(a), policyDepth(b)), strings.Compare(a, b))
}

func (m *Matcher) readPolicy(input fs.FS, name string) error {
	data, err := fs.ReadFile(input, name)
	if errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("read ignore file %q: %w", name, err)
	}
	m.evidence = append(m.evidence, policyFile{Path: name, Data: string(data)})
	return nil
}

func (m *Matcher) loadPolicy(input fs.FS, dir string) error {
	entries, err := fs.ReadDir(input, dir)
	if err != nil {
		// Flux gives ignore-file read errors priority over directory-listing errors.
		if readErr := m.readPolicy(input, path.Join(dir, flux.IgnoreFile)); readErr != nil {
			return readErr
		}
		return fmt.Errorf("list ignore directory %q: %w", dir, err)
	}
	for _, entry := range entries {
		if entry.Name() == flux.IgnoreFile {
			if readErr := m.readPolicy(input, path.Join(dir, flux.IgnoreFile)); readErr != nil {
				return readErr
			}
			break
		}
	}
	for _, entry := range entries {
		if !entry.IsDir() || entry.Name() == ".git" {
			continue
		}
		if walkErr := m.loadPolicy(input, path.Join(dir, entry.Name())); walkErr != nil {
			return walkErr
		}
	}
	return nil
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

//nolint:ireturn // Flux exposes matcher construction through the gitignore.Matcher interface.
func (m *Matcher) buildMatcher(patterns []gitignore.Pattern) gitignore.Matcher {
	if len(m.extra) > 0 {
		patterns = slices.Concat(patterns, m.extra)
	}
	if m.withDefaults {
		if len(patterns) == 0 {
			return defaultMatcher
		}
		patterns = append(flux.VCSPatterns(nil), patterns...)
	}
	return flux.NewMatcher(patterns)
}

// UsesDefaults reports whether the policy has no custom patterns.
func (m *Matcher) UsesDefaults() bool { return len(m.patterns) == 0 && len(m.extra) == 0 }

// Evidence returns the ordered ignore-file paths and exact content parsed by NewFS.
func (m *Matcher) Evidence() []struct{ Path, Data string } {
	out := make([]struct{ Path, Data string }, len(m.evidence))
	for i, file := range m.evidence {
		out[i] = struct{ Path, Data string }{file.Path, file.Data}
	}
	return out
}

// ReadPolicy returns the bytes already read for an ignore file.
func (m *Matcher) ReadPolicy(name string) (string, bool) {
	for _, file := range m.evidence {
		if file.Path == name {
			return file.Data, true
		}
	}
	return "", false
}

// Match reports whether rel (a path relative to the matcher's root, using the
// slash separator) is excluded. isDir distinguishes directory-only patterns.
func (m *Matcher) Match(rel string, isDir bool) bool {
	return m.matcher.Match(strings.Split(rel, "/"), isDir)
}

// Admit records one selected entry and the parents needed to reach it.
func (m *Matcher) Admit(kept map[string]bool, name string, directory bool) {
	if m != nil && m.Match(name, directory) {
		return
	}
	for parent := name; parent != "." && !kept[parent]; parent = path.Dir(parent) {
		kept[parent] = true
	}
}
