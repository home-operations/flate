package manifest

import (
	"iter"
	"path/filepath"
	"slices"
	"strings"

	"github.com/home-operations/flate/internal/kustpath"
)

// KSClaim pairs a Flux Kustomization id with one of its slash-terminated,
// repo-relative claimed-path prefixes. A KS claims its spec.path plus one
// prefix per spec.components entry and per on-disk `components:` entry; the
// same ID across multiple prefixes is intentional (the deepest claim wins
// in a longest-prefix lookup).
type KSClaim struct {
	ID     NamedResource
	Prefix string
}

// RenderRoots yields normalized source-relative render roots and components.
// An empty root denotes the source artifact root.
func RenderRoots(obj BaseManifest) iter.Seq[string] {
	return func(yield func(string) bool) {
		if ks, ok := obj.(*Kustomization); ok {
			ks.renderRoots(nil, yield)
		}
	}
}

func (k *Kustomization) renderRoots(components []string, yield func(string) bool) {
	if k.IsFragment() {
		return
	}
	base := NormalizeClaimBase(k.Path)
	if !yield(base) {
		return
	}
	for _, refs := range [2][]string{k.Components, components} {
		for _, ref := range refs {
			resolved, ok := kustpath.Resolve(base, ref, kustpath.Claim)
			if !ok {
				continue
			}
			if !yield(resolved) {
				return
			}
		}
	}
}

// BuildKSClaims constructs longest-prefix-first claims from authored paths and
// already-decoded components. Kustomizations without a path or source claim nothing.
func BuildKSClaims(kss []*Kustomization, components map[NamedResource][]string) []KSClaim {
	claims := make([]KSClaim, 0, len(kss))
	for _, ks := range kss {
		id := ks.Named()
		ks.renderRoots(components[id], func(root string) bool {
			prefix := root
			if prefix != "" {
				prefix += "/"
			}
			claims = append(claims, KSClaim{ID: id, Prefix: prefix})
			return true
		})
	}
	slices.SortStableFunc(claims, func(a, b KSClaim) int { return len(b.Prefix) - len(a.Prefix) })
	return claims
}

// StrictParent returns the deepest claimant other than self. selfPrefixes excludes
// peers that share a claimed path, or their dependency gates would deadlock.
// Claims must be ordered longest-prefix-first by BuildKSClaims.
func StrictParent(claims []KSClaim, file string, self NamedResource, selfPrefixes map[string]struct{}) (NamedResource, bool) {
	for _, c := range claims {
		if c.ID == self {
			continue
		}
		if _, peer := selfPrefixes[c.Prefix]; peer {
			continue
		}
		if strings.HasPrefix(file, c.Prefix) {
			return c.ID, true
		}
	}
	return NamedResource{}, false
}

// NormalizeClaimBase returns a repo-relative base with no trailing slash.
// Leading parent segments are clamped at the artifact root. Interior parent
// segments must remain unresolved because SecureJoin follows symlinks first.
func NormalizeClaimBase(p string) string {
	p = strings.Trim(filepath.ToSlash(p), "/")
	for {
		part, rest, _ := strings.Cut(p, "/")
		if part != "." && part != ".." {
			break
		}
		p = strings.TrimLeft(rest, "/")
	}
	if !strings.Contains(p, "//") && !strings.Contains(p, "/./") && !strings.HasSuffix(p, "/.") {
		return p
	}
	var b strings.Builder
	for part := range strings.SplitSeq(p, "/") {
		if part == "" || part == "." {
			continue
		}
		if b.Len() > 0 {
			b.WriteByte('/')
		}
		b.WriteString(part)
	}
	return b.String()
}
