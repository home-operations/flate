package manifest

import (
	"path"
	"path/filepath"
	"slices"
	"strings"
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

// BuildKSClaims constructs longest-prefix-first claims from authored paths and
// already-decoded components. Kustomizations without a path or source claim nothing.
func BuildKSClaims(kss []*Kustomization, components map[NamedResource][]string) []KSClaim {
	claims := make([]KSClaim, 0, len(kss))
	add := func(id NamedResource, base, ref string) {
		if ref == "" || strings.Contains(ref, "://") || filepath.IsAbs(ref) {
			return
		}
		resolved := path.Clean(path.Join(base, ref))
		if resolved == "." || strings.HasPrefix(resolved, "..") {
			return
		}
		claims = append(claims, KSClaim{ID: id, Prefix: resolved + "/"})
	}
	for _, ks := range kss {
		if ks.Path == "" && ks.SourceName == "" {
			continue
		}
		id := ks.Named()
		base := NormalizeClaimBase(ks.Path)
		prefix := base + "/"
		if base == "" {
			prefix = ""
		}
		claims = append(claims, KSClaim{ID: id, Prefix: prefix})
		for _, comp := range ks.Components {
			add(id, base, comp)
		}
		for _, comp := range components[id] {
			add(id, base, comp)
		}
	}
	slices.SortStableFunc(claims, func(a, b KSClaim) int { return len(b.Prefix) - len(a.Prefix) })
	return claims
}

// StrictParent returns the deepest claimant other than self. selfPrefixes excludes
// peers that share a claimed path, or their dependency gates would deadlock.
// Claims must be ordered longest-prefix-first by BuildKSClaims.
func StrictParent(claims []KSClaim, file string, self NamedResource, selfPrefixes map[string]struct{}) (NamedResource, bool) {
	file = filepath.ToSlash(file)
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

// NormalizeClaimBase turns a Kustomization spec.path into a clean,
// slash-separated, repo-relative base with no trailing slash. ToSlash is
// applied so Windows-style spec.path values (rare, but unconstrained by the
// Flux CRD) normalize to the same shape as discovery file coordinates. A leading slash
// is dropped: kustomize-controller resolves spec.path with SecureJoin, which
// treats an absolute path as relative to the artifact root, so
// `/kubernetes/apps` claims the same files as `./kubernetes/apps`.
func NormalizeClaimBase(p string) string {
	p = filepath.ToSlash(p)
	p = strings.TrimPrefix(p, "./")
	p = strings.TrimLeft(p, "/")
	return strings.TrimSuffix(p, "/")
}
