package loader

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"path/filepath"
	"slices"
	"strings"

	flux "github.com/fluxcd/pkg/sourceignore"
	"github.com/fluxcd/pkg/sourceignore/gitignore"
	"github.com/go-git/go-billy/v5/osfs"
	"github.com/go-git/go-git/v5/plumbing"

	"github.com/home-operations/flate/pkg/source/gittree"
)

type ignoreSet struct {
	matcher gitignore.Matcher
	// Ignored directories MUST remain reachable when descendants can be re-included.
	hasNegation bool
}

func (r documentReader) loadIgnore(root string) (*ignoreSet, error) {
	input, name := r.input(filepath.Join(root, ".krmignore"))
	f, err := input.Open(name)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return &ignoreSet{}, nil
		}
		return &ignoreSet{}, err
	}
	defer func() { _ = f.Close() }()
	return parseIgnore(f, root)
}

// Explicit overrides retain the scan root's domain and missing-file errors.
func (r documentReader) loadIgnoreFile(ctx context.Context, path, root string) (*ignoreSet, error) {
	abs, err := filepath.Abs(path)
	if err != nil {
		return &ignoreSet{}, fmt.Errorf("loader: read krmignore %q: %w", path, err)
	}
	input, name := r.input(abs)
	mounted := r.mountedRoot(abs)
	if mounted == "" {
		// Only the explicitly named option may admit a file outside source mounts.
		input, _, err = gittree.New(ctx, nil, plumbing.ZeroHash, gittree.ViewOptions{Mode: gittree.Direct, Worktree: osfs.New(filepath.Dir(abs), osfs.WithBoundOS())})
		if err != nil {
			return &ignoreSet{}, fmt.Errorf("loader: read krmignore %q: %w", path, err)
		}
		name = filepath.Base(abs)
	}
	f, err := input.Open(name)
	if err != nil {
		return &ignoreSet{}, fmt.Errorf("loader: read krmignore %q: %w", path, err)
	}
	defer func() { _ = f.Close() }()
	return parseIgnore(f, root)
}

func parseIgnore(f io.Reader, root string) (*ignoreSet, error) {
	out := &ignoreSet{}
	domain := strings.Split(filepath.ToSlash(root), "/")
	var normalized strings.Builder
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		if strings.HasPrefix(line, "!") {
			out.hasNegation = true
		}
		normalized.WriteString(line)
		normalized.WriteByte('\n')
	}
	if err := sc.Err(); err != nil {
		return out, err
	}
	patterns := flux.ReadPatterns(strings.NewReader(normalized.String()), domain)
	if len(patterns) > 0 {
		out.matcher = gitignore.NewMatcher(patterns)
	}
	return out, nil
}

// isDir MUST be true for directories so directory-only patterns match.
func (i *ignoreSet) matches(path, root string, isDir bool) bool {
	if i == nil || i.matcher == nil {
		return false
	}
	rel, err := filepath.Rel(root, path)
	if err != nil {
		return false
	}
	relSlash := filepath.ToSlash(rel)
	rootSlash := filepath.ToSlash(root)
	domain := strings.Split(rootSlash, "/")
	relParts := strings.Split(relSlash, "/")
	segments := slices.Concat(domain, relParts)
	return i.matcher.Match(segments, isDir)
}
