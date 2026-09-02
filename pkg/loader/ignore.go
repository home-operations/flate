package loader

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"

	"github.com/fluxcd/pkg/sourceignore/gitignore"
)

// ignoreSet is the matched-rule set from a .krmignore (or .gitignore-style)
// file at the scan root.
type ignoreSet struct {
	matcher gitignore.Matcher
	// hasNegation is true when any pattern is a `!` re-include. The
	// walker's directory prune consults it: pruning an ignored
	// directory would prevent a deeper `!` pattern from re-including
	// files beneath it (per-file checks still filter everything the
	// walk visits), which is how source-controller's sourceignore
	// evaluates spec.ignore — see pkg/source/ignore.go.
	hasNegation bool
}

// loadIgnore reads <root>/.krmignore (or returns an empty set if not
// present). The grammar is gitignore: hash comments, blank lines, and one
// pattern per line. Patterns are interpreted relative to root and support
// the full gitignore glob syntax, including ** for zero-or-more path
// segments.
func loadIgnore(root string) (*ignoreSet, error) {
	f, err := os.Open(filepath.Join(root, ".krmignore")) //nolint:gosec // root is the cluster scan root
	if err != nil {
		if os.IsNotExist(err) {
			return &ignoreSet{}, nil
		}
		return &ignoreSet{}, err
	}
	defer func() { _ = f.Close() }()
	return parseIgnore(f, root)
}

// loadIgnoreFile reads the ignore file at path in place of
// <root>/.krmignore. The patterns are still anchored at root, so a file
// kept elsewhere in the repo (e.g. .krmignore.staging) reads exactly as
// it would at the scan root. Unlike loadIgnore, a missing file is an
// error: the caller named it explicitly.
func loadIgnoreFile(path, root string) (*ignoreSet, error) {
	f, err := os.Open(path) //nolint:gosec // path is a user-supplied ignore file
	if err != nil {
		return &ignoreSet{}, fmt.Errorf("loader: read krmignore %q: %w", path, err)
	}
	defer func() { _ = f.Close() }()
	return parseIgnore(f, root)
}

// parseIgnore builds the ignoreSet from one gitignore-grammar file,
// anchoring its patterns at root.
func parseIgnore(f *os.File, root string) (*ignoreSet, error) {
	out := &ignoreSet{}
	// domain is the root split into path segments, used by the gitignore
	// pattern parser to anchor absolute-style patterns correctly.
	domain := strings.Split(filepath.ToSlash(root), "/")
	var patterns []gitignore.Pattern
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		if strings.HasPrefix(line, "!") {
			out.hasNegation = true
		}
		patterns = append(patterns, gitignore.ParsePattern(line, domain))
	}
	if err := sc.Err(); err != nil {
		return out, err
	}
	if len(patterns) > 0 {
		out.matcher = gitignore.NewMatcher(patterns)
	}
	return out, nil
}

// matches reports whether path (an absolute path under root) should be
// ignored. isDir must be true when path is a directory; this is required so
// that trailing-slash patterns in .krmignore (e.g. "tmp/") — which the
// gitignore parser marks as dirOnly — are evaluated correctly. Passing false
// for a directory causes dirOnly patterns to silently never fire.
func (i *ignoreSet) matches(path, root string, isDir bool) bool {
	if i == nil || i.matcher == nil {
		return false
	}
	rel, err := filepath.Rel(root, path)
	if err != nil {
		return false
	}
	// Build a domain+relative segment slice: the gitignore matcher expects
	// [domain... rel_segments...] where domain matches what was passed to
	// ParsePattern. The segments are slash-separated path components.
	relSlash := filepath.ToSlash(rel)
	rootSlash := filepath.ToSlash(root)
	domain := strings.Split(rootSlash, "/")
	relParts := strings.Split(relSlash, "/")
	segments := slices.Concat(domain, relParts)
	return i.matcher.Match(segments, isDir)
}
