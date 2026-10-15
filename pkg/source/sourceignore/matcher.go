// Package sourceignore builds the file-exclusion matcher Flux's
// source-controller applies when it packages a GitRepository/OCIRepository
// artifact: VCS exclusions plus any in-tree .sourceignore files and
// caller-supplied spec.ignore patterns, with defaults selected by [New].
//
// It is a leaf package — it depends only on the vendored fluxcd/go-git
// ignore primitives, never on pkg/source or pkg/kustomize — so BOTH the
// source-fetch artifact filtering (pkg/source.ApplyIgnore) and the in-memory
// kustomize tree materialization (pkg/kustomize) can consume one matcher
// without an import cycle.
package sourceignore

import (
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

// RulesVersion must change when filtering rules change to invalidate source artifact caches.
const RulesVersion = "sourceignore-v1"

// Matcher reports whether a path within a source tree rooted at a fixed
// directory is excluded from the artifact. It is safe for concurrent reads.
type Matcher struct {
	matcher gitignore.Matcher
	domain  []string
}

// New builds a Matcher for the tree rooted at root.
//
// Flux default exclusions apply when withDefaults is true and no user patterns
// were parsed from files or extra. withDefaults always adds VCS patterns.
// Pass false for the Bucket flavor (in-tree .sourceignore + extra only).
// extra, when non-empty, appends caller patterns (a source's spec.ignore).
func New(root string, extra *string, withDefaults bool) (*Matcher, error) {
	abs, err := filepath.Abs(root)
	if err != nil {
		return nil, fmt.Errorf("sourceignore abs: %w", err)
	}
	domain := splitPath(abs)

	patterns, err := flux.LoadIgnorePatterns(abs, domain)
	if err != nil {
		return nil, fmt.Errorf("sourceignore load: %w", err)
	}
	return newMatcher(domain, patterns, extra, withDefaults), nil
}

// NewFS builds Flux policy from a slash-named immutable filesystem.
// Nil files walks the input; a supplied list MUST contain every relative ignore file.
// Parent rules MUST precede nested rules, independent of filesystem listing order.
func NewFS(input fs.FS, files []string, extra string, withDefaults bool) (*Matcher, error) {
	if files == nil {
		err := fs.WalkDir(input, ".", func(name string, d fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if d.IsDir() && d.Name() == ".git" {
				return fs.SkipDir
			}
			if d.Name() == flux.IgnoreFile {
				files = append(files, name)
			}
			return nil
		})
		if err != nil {
			return nil, fmt.Errorf("sourceignore load: %w", err)
		}
	}
	files = slices.Clone(files)
	slices.SortFunc(files, func(a, b string) int {
		a, b = path.Dir(a), path.Dir(b)
		var left, right []string
		if a != "." {
			left = strings.Split(a, "/")
		}
		if b != "." {
			right = strings.Split(b, "/")
		}
		return slices.Compare(left, right)
	})
	var patterns []gitignore.Pattern
	for _, name := range files {
		data, err := fs.ReadFile(input, name)
		if errors.Is(err, fs.ErrNotExist) {
			continue
		}
		if err != nil {
			return nil, fmt.Errorf("sourceignore load: %w", err)
		}
		var domain []string
		if dir := path.Dir(name); dir != "." {
			domain = strings.Split(dir, "/")
		}
		patterns = append(patterns, flux.ReadPatterns(strings.NewReader(string(data)), domain)...)
	}
	return newMatcher(nil, patterns, &extra, withDefaults), nil
}

func newMatcher(domain []string, patterns []gitignore.Pattern, extra *string, withDefaults bool) *Matcher {
	if extra != nil && strings.TrimSpace(*extra) != "" {
		patterns = append(patterns, flux.ReadPatterns(strings.NewReader(*extra), domain)...)
	}

	var matcher gitignore.Matcher
	if withDefaults && len(patterns) == 0 {
		matcher = flux.NewDefaultMatcher(patterns, domain)
	} else {
		if withDefaults {
			patterns = append(flux.VCSPatterns(domain), patterns...)
		}
		matcher = flux.NewMatcher(patterns)
	}
	return &Matcher{matcher: matcher, domain: domain}
}

// Match reports whether rel (a path relative to the matcher's root, using the
// OS separator) is excluded. isDir distinguishes directory-only patterns.
func (m *Matcher) Match(rel string, isDir bool) bool {
	segments := splitPath(rel)
	if len(m.domain) != 0 {
		segments = slices.Concat(m.domain, segments)
	}
	return m.matcher.Match(segments, isDir)
}

// splitPath breaks p into its components using the OS path separator.
func splitPath(p string) []string {
	return strings.Split(p, string(filepath.Separator))
}
