package kustpath

import (
	"path/filepath"
	"runtime"
	"strings"
	"unicode"
)

const windows = "windows"

// HostRelation compares the location of two lexical paths.
type HostRelation uint8

// Host relations describe the base path relative to the target path.
const (
	HostDisjoint HostRelation = iota
	HostEqual
	HostAncestor
	HostDescendant
)

// Overlaps reports whether either path contains the other.
func (r HostRelation) Overlaps() bool { return r != HostDisjoint }

// Contains reports whether the base contains or equals the target.
func (r HostRelation) Contains() bool { return r == HostEqual || r == HostAncestor }

// RelateHost compares lexical host paths. Empty paths denote missing evidence.
func RelateHost(base, target string) HostRelation {
	if base == "" || target == "" {
		return HostDisjoint
	}
	base, target = filepath.Clean(base), filepath.Clean(target)
	if base == target {
		return HostEqual
	}
	if runtime.GOOS == windows && strings.EqualFold(base, target) {
		return HostEqual
	}
	switch {
	case hostDescends(base, target):
		return HostAncestor
	case hostDescends(target, base):
		return HostDescendant
	default:
		return HostDisjoint
	}
}

func hostDescends(base, target string) bool {
	var rel string
	switch {
	case runtime.GOOS == windows:
		var err error
		rel, err = filepath.Rel(base, target)
		if err != nil {
			return false
		}
	case base == ".":
		return filepath.IsLocal(target)
	case base == string(filepath.Separator):
		return strings.HasPrefix(target, base)
	default:
		if len(target) <= len(base) || !strings.HasPrefix(target, base) || target[len(base)] != filepath.Separator {
			return false
		}
		rel = target[len(base)+1:]
	}
	return rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator))
}

// HostKey normalizes host lookup keys. Repository-relative Git paths MUST keep their spelling.
func HostKey(path string) string {
	path = filepath.ToSlash(filepath.Clean(path))
	if runtime.GOOS != windows {
		return path
	}
	// Use the same Unicode folding classes as filepath.Rel on Windows.
	return strings.Map(func(r rune) rune {
		folded := r
		for next := unicode.SimpleFold(r); next != r; next = unicode.SimpleFold(next) {
			folded = min(folded, next)
		}
		return folded
	}, path)
}
