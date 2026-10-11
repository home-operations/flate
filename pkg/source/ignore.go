package source

import (
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"slices"

	"github.com/home-operations/flate/pkg/source/sourceignore"
)

// ApplyIgnore deletes every file under root that matches the source-
// controller ignore matcher, using the defaults rule defined by [sourceignore.New].
// Mirrors source-controller's GitRepository / OCIRepository artifact-build behavior.
//
// Bucket sources use ApplyIgnoreNoDefaults instead — see that function
// for the rationale.
func ApplyIgnore(root string, ignore *string) error {
	return applyIgnore(root, ignore, true)
}

func applyIgnore(root string, ignore *string, withDefaults bool) error {
	abs, err := filepath.Abs(root)
	if err != nil {
		return fmt.Errorf("sourceignore abs: %w", err)
	}
	matcher, err := sourceignore.New(abs, ignore, withDefaults)
	if err != nil {
		return err
	}
	return walkAndDelete(abs, matcher)
}

func walkAndDelete(root string, matcher *sourceignore.Matcher) error {
	// Decide per-file: walk every file, ask the matcher whether it
	// belongs in the artifact. Don't SkipDir on excluded directories —
	// that would prevent a deeper `!` re-include pattern from being
	// observed. With patterns like
	//
	//   /*
	//   !/charts/tekton-operator/
	//
	// the matcher correctly excludes `charts/` (match=true) but
	// re-includes `charts/tekton-operator/Chart.yaml` (match=false);
	// a SkipDir on `charts/` would wipe the latter alongside.
	//
	// Empty directories left after file removal are pruned in a
	// second bottom-up sweep so the artifact mirrors source-
	// controller's tarball shape.
	var toRemove []string
	if err := filepath.WalkDir(root, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if p == root || d.IsDir() {
			return nil
		}
		rel, err := filepath.Rel(root, p)
		if err != nil {
			return err
		}
		if matcher.Match(rel, false) {
			toRemove = append(toRemove, p)
		}
		return nil
	}); err != nil {
		return fmt.Errorf("sourceignore walk: %w", err)
	}
	for _, p := range toRemove {
		if err := os.Remove(p); err != nil {
			return fmt.Errorf("sourceignore remove %s: %w", p, err)
		}
	}
	return pruneEmptyDirs(root)
}

// pruneEmptyDirs removes directories that became empty after file
// deletion, bottom-up. Stops at root.
func pruneEmptyDirs(root string) error {
	var dirs []string
	if err := filepath.WalkDir(root, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() && p != root {
			dirs = append(dirs, p)
		}
		return nil
	}); err != nil {
		return fmt.Errorf("sourceignore prune walk: %w", err)
	}
	// Bottom-up by path length: deepest first.
	for _, v := range slices.Backward(dirs) {
		entries, err := os.ReadDir(v)
		if err != nil || len(entries) > 0 {
			continue
		}
		_ = os.Remove(v)
	}
	return nil
}
