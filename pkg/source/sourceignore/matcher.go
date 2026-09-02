// Package sourceignore builds the file-exclusion matcher Flux's
// source-controller applies when it packages a GitRepository/OCIRepository
// artifact: its default patterns (.git/, .github/, *.jpg/png/zip, .sops.yaml,
// .flux.yaml, .goreleaser.yml, …) plus any in-tree .sourceignore files plus
// caller-supplied spec.ignore patterns.
//
// It is a leaf package — it depends only on the vendored fluxcd/go-git
// ignore primitives, never on pkg/source or pkg/kustomize — so BOTH the
// source-fetch artifact filtering (pkg/source.ApplyIgnore) and the in-memory
// kustomize tree materialization (pkg/kustomize) can consume one matcher
// without an import cycle.
package sourceignore

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strings"

	flux "github.com/fluxcd/pkg/sourceignore"
	"github.com/go-git/go-git/v5/plumbing/format/gitignore"
)

// Matcher reports whether a path within a source tree rooted at a fixed
// directory is excluded from the artifact. It is safe for concurrent reads.
type Matcher struct {
	matcher gitignore.Matcher
	domain  []string
}

// New builds a Matcher for the tree rooted at root.
//
// withDefaults adds Flux's default VCS + ExcludeExt/CI/Extra patterns — the
// GitRepository/OCIRepository behavior; pass false for the Bucket flavor
// (in-tree .sourceignore + extra only). extra, when non-empty, appends
// caller patterns (a source's spec.ignore).
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
	if extra != nil && strings.TrimSpace(*extra) != "" {
		patterns = append(patterns, flux.ReadPatterns(strings.NewReader(*extra), domain)...)
	}

	if withDefaults {
		return &Matcher{matcher: flux.NewDefaultMatcher(patterns, domain), domain: domain}, nil
	}
	return &Matcher{matcher: flux.NewMatcher(patterns), domain: domain}, nil
}

// Match reports whether rel (a path relative to the matcher's root, using the
// OS separator) is excluded. isDir distinguishes directory-only patterns.
func (m *Matcher) Match(rel string, isDir bool) bool {
	segments := slices.Concat(m.domain, splitPath(rel))
	return m.matcher.Match(segments, isDir)
}

// splitPath breaks p into its components using the OS path separator.
func splitPath(p string) []string {
	return strings.Split(p, string(filepath.Separator))
}

// Fingerprint digests every .sourceignore file under root — relative path and
// content, in LoadIgnorePatterns' traversal order — so work cached against a
// Matcher built from root can be keyed on the files that shaped it: an edit,
// addition, or removal of any .sourceignore changes the result. Directories
// named .git are skipped, as the loader skips them.
func Fingerprint(root string) (string, error) {
	h := sha256.New()
	err := filepath.WalkDir(root, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			if p != root && d.Name() == ".git" {
				return filepath.SkipDir
			}
			return nil
		}
		if d.Name() != flux.IgnoreFile {
			return nil
		}
		rel, err := filepath.Rel(root, p)
		if err != nil {
			return err
		}
		b, err := os.ReadFile(p) //nolint:gosec // p is a .sourceignore found by walking root
		if err != nil {
			return err
		}
		h.Write([]byte(filepath.ToSlash(rel)))
		h.Write([]byte{0})
		h.Write(b)
		h.Write([]byte{0})
		return nil
	})
	if err != nil {
		return "", fmt.Errorf("sourceignore fingerprint: %w", err)
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}
