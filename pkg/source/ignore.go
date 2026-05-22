package source

import (
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"

	"github.com/fluxcd/pkg/sourceignore"
	"github.com/go-git/go-git/v5/plumbing/format/gitignore"
)

// ApplyIgnore deletes every file under root that matches the gitignore-
// style patterns in ignore, mirroring source-controller's spec.ignore
// behavior. Empty/nil ignore is a no-op.
//
// flate intentionally applies ONLY user-supplied patterns from
// spec.ignore — source-controller's default VCS/extension excludes are
// skipped so repos that never set spec.ignore see no behavior change.
// Files that real Flux would have excluded by default remain visible
// in the staged artifact; users wanting full fidelity should set
// spec.ignore explicitly.
func ApplyIgnore(root string, ignore *string) error {
	if ignore == nil || strings.TrimSpace(*ignore) == "" {
		return nil
	}
	abs, err := filepath.Abs(root)
	if err != nil {
		return fmt.Errorf("sourceignore abs: %w", err)
	}
	domain := strings.Split(abs, string(filepath.Separator))
	patterns := sourceignore.ReadPatterns(strings.NewReader(*ignore), domain)
	if len(patterns) == 0 {
		return nil
	}
	matcher := sourceignore.NewMatcher(patterns)
	return walkAndDelete(abs, domain, matcher)
}

func walkAndDelete(root string, domain []string, matcher gitignore.Matcher) error {
	var toRemove []string
	if err := filepath.WalkDir(root, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if p == root {
			return nil
		}
		rel, err := filepath.Rel(root, p)
		if err != nil {
			return err
		}
		// gitignore.Matcher expects path segments relative to the
		// scope root, combined with the absolute domain.
		segments := append(append([]string{}, domain...), strings.Split(rel, string(filepath.Separator))...)
		if matcher.Match(segments, d.IsDir()) {
			toRemove = append(toRemove, p)
			if d.IsDir() {
				return fs.SkipDir
			}
		}
		return nil
	}); err != nil {
		return fmt.Errorf("sourceignore walk: %w", err)
	}
	for _, p := range toRemove {
		if err := os.RemoveAll(p); err != nil {
			return fmt.Errorf("sourceignore remove %s: %w", p, err)
		}
	}
	return nil
}
