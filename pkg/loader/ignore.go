package loader

import (
	"bufio"
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
}

// loadIgnore reads <root>/.krmignore (or returns an empty set if not
// present). The grammar is gitignore: hash comments, blank lines, and one
// pattern per line. Patterns are interpreted relative to root and support
// the full gitignore glob syntax, including ** for zero-or-more path
// segments.
func loadIgnore(root string) (*ignoreSet, error) {
	out := &ignoreSet{}
	path := filepath.Join(root, ".krmignore")
	f, err := os.Open(path) //nolint:gosec // root is the cluster scan root
	if err != nil {
		if os.IsNotExist(err) {
			return out, nil
		}
		return out, err
	}
	defer func() { _ = f.Close() }()

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
