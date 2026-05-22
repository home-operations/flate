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

// ApplyIgnore deletes every file under root that matches the source-
// controller ignore matcher: VCS + Default excludes (.git/, .github/,
// *.jpg/png/zip, .sops.yaml, .flux.yaml, ...) PLUS any in-tree
// .sourceignore files PLUS the user-supplied spec.ignore patterns when
// non-nil. Mirrors source-controller's artifact-build behavior.
//
// nil spec.ignore is NOT a no-op — the VCS + Default patterns still
// apply, matching what real Flux ships in an artifact tarball.
func ApplyIgnore(root string, ignore *string) error {
	abs, err := filepath.Abs(root)
	if err != nil {
		return fmt.Errorf("sourceignore abs: %w", err)
	}
	domain := strings.Split(abs, string(filepath.Separator))

	// Default to VCS + extension excludes + any .sourceignore files
	// reachable from root, then layer user patterns on top.
	patterns, err := sourceignore.LoadIgnorePatterns(abs, domain)
	if err != nil {
		return fmt.Errorf("sourceignore load: %w", err)
	}
	if ignore != nil && strings.TrimSpace(*ignore) != "" {
		patterns = append(patterns, sourceignore.ReadPatterns(strings.NewReader(*ignore), domain)...)
	}
	return walkAndDelete(abs, domain, sourceignore.NewDefaultMatcher(patterns, domain))
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
