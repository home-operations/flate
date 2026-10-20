// Package kustpath resolves lexical discovery references.
package kustpath

import (
	"path"
	"path/filepath"
	"strings"
)

// Policy identifies the boundary required by a discovery reference.
type Policy uint8

// Directory, File, Transformer, Claim, Root and Dependency define discovery reference boundaries.
const (
	Directory   Policy = iota // Directory permits relative parent traversal.
	File                      // File confines relative references to their package.
	Transformer               // Transformer confines native references to the source.
	Claim                     // Claim uses source-relative slash coordinates.
	Root                      // Root checks an already resolved graph coordinate.
	Dependency                // Dependency permits absolute paths and relative parent traversal.
)

// Resolve joins a reference and applies its lexical boundary policy.
// Filesystem confinement and symlink resolution remain the source view's job.
func Resolve(base, ref string, policy Policy) (string, bool) {
	if policy == Directory || policy == Transformer || policy == Dependency {
		if strings.Contains(ref, "://") {
			return "", false
		}
		for _, prefix := range []string{"git::", "gh:", "git@", "github.com:", "github.com/"} {
			if strings.HasPrefix(strings.ToLower(ref), prefix) {
				return "", false
			}
		}
	}
	absolute := filepath.IsAbs(ref)
	if policy == Claim {
		absolute = path.IsAbs(ref)
	}
	if policy != Root && (ref == "" || policy != File && policy != Dependency && (absolute || strings.Contains(ref, "://"))) {
		return "", false
	}
	if (policy == File || policy == Dependency) && absolute {
		return filepath.Clean(ref), true
	}
	var resolved string
	switch policy {
	case Directory, File, Dependency:
		resolved = filepath.Join(base, strings.TrimPrefix(ref, "."+string(filepath.Separator)))
	case Transformer:
		resolved = path.Join(base, filepath.ToSlash(ref))
	case Claim:
		resolved = path.Join(base, ref)
	case Root:
		resolved = path.Clean(ref)
	}
	if policy == File {
		cleanBase := filepath.Clean(base)
		suffix, within := strings.CutPrefix(resolved, cleanBase)
		if resolved != cleanBase && (!within || !strings.HasPrefix(suffix, string(filepath.Separator))) {
			return "", false
		}
	}
	if (policy == Transformer || policy == Claim || policy == Root) &&
		(resolved == ".." || strings.HasPrefix(resolved, "../") || policy != Root && resolved == ".") {
		return "", false
	}
	return resolved, true
}
