package source

import (
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strings"

	"github.com/fluxcd/pkg/sourceignore"
	"github.com/go-git/go-git/v5/plumbing/format/gitignore"
)

// ApplyIgnore deletes every file under root that matches the source-
// controller ignore matcher: VCS + Default excludes (.git/, .github/,
// *.jpg/png/zip, .sops.yaml, .flux.yaml, ...) PLUS any in-tree
// .sourceignore files PLUS the user-supplied spec.ignore patterns when
// non-nil. Mirrors source-controller's GitRepository / OCIRepository
// artifact-build behavior.
//
// nil spec.ignore is NOT a no-op — the VCS + Default patterns still
// apply, matching what real Flux ships in a Git/OCI artifact tarball.
//
// Bucket sources use ApplyIgnoreNoDefaults instead — see that function
// for the rationale.
func ApplyIgnore(root string, ignore *string) error {
	return applyIgnore(root, ignore, true)
}

// ApplyIgnoreNoDefaults is the Bucket-flavored variant: it skips the
// VCS + Default exclude patterns and applies ONLY the in-tree
// .sourceignore plus the user-supplied spec.ignore. Mirrors
// fluxcd/source-controller/internal/controller/bucket_controller.go
// which uses sourceignore.NewMatcher (no defaults) rather than the
// NewDefaultMatcher that GitRepository / OCIRepository use.
//
// The rationale: Bucket has no VCS semantics. An object store can
// legitimately carry .git/-named keys, .jpg/.png images, .flux.yaml,
// etc., and source-controller delivers them as-is. Stripping them
// would diverge from what a cluster Bucket reconcile produces.
func ApplyIgnoreNoDefaults(root string, ignore *string) error {
	return applyIgnore(root, ignore, false)
}

func applyIgnore(root string, ignore *string, withDefaults bool) error {
	abs, err := filepath.Abs(root)
	if err != nil {
		return fmt.Errorf("sourceignore abs: %w", err)
	}
	domain := strings.Split(abs, string(filepath.Separator))

	patterns, err := sourceignore.LoadIgnorePatterns(abs, domain)
	if err != nil {
		return fmt.Errorf("sourceignore load: %w", err)
	}
	if ignore != nil && strings.TrimSpace(*ignore) != "" {
		patterns = append(patterns, sourceignore.ReadPatterns(strings.NewReader(*ignore), domain)...)
	}
	var matcher gitignore.Matcher
	if withDefaults {
		matcher = sourceignore.NewDefaultMatcher(patterns, domain)
	} else {
		matcher = sourceignore.NewMatcher(patterns)
	}
	return walkAndDelete(abs, domain, matcher)
}

func walkAndDelete(root string, domain []string, matcher gitignore.Matcher) error {
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
		segments := append(append([]string{}, domain...), strings.Split(rel, string(filepath.Separator))...)
		if matcher.Match(segments, false) {
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
