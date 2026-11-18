package manifest

import (
	"iter"
	"path/filepath"
	"slices"
	"strings"

	"github.com/home-operations/flate/internal/core/kustpath"
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

// SourceRoots yields source-relative paths declared for rendering or entry application.
func SourceRoots(obj BaseManifest) iter.Seq[string] {
	return func(yield func(string) bool) {
		if object, ok := obj.(*RawObject); ok {
			if _, root, applies := object.SourcePath(); applies {
				yield(NormalizeClaimBase(root))
			}
			return
		}
		for root := range RenderRoots(obj) {
			if !yield(root) {
				return
			}
		}
	}
}

// RenderRoots yields source-relative render roots and unresolved component evidence.
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
			if ref == "" {
				continue
			}
			root := ref
			if ref != "" && !filepath.IsAbs(ref) && !strings.Contains(ref, "://") {
				root = base + "/" + ref
				if base == "" {
					root = ref
				}
			}
			if !yield(root) {
				return
			}
		}
	}
}

// BuildKSClaims MUST use components from the admitted source view.
func BuildKSClaims(kss []*Kustomization, components map[NamedResource][]string) []KSClaim {
	claims := make([]KSClaim, 0, len(kss))
	for _, ks := range kss {
		if _, _, applies := ks.SourcePath(); !applies {
			continue
		}
		claims = ks.appendClaims(claims, ks.Path, components[ks.Named()])
	}
	slices.SortStableFunc(claims, func(a, b KSClaim) int { return len(b.Prefix) - len(a.Prefix) })
	return claims
}

func (k *Kustomization) appendClaims(claims []KSClaim, path string, components []string) []KSClaim {
	declaration := *k
	declaration.Path = path
	base := true
	declaration.renderRoots(components, func(root string) bool {
		prefix := root
		if !base {
			var ok bool
			prefix, ok = kustpath.Resolve("", root, kustpath.Claim)
			if !ok {
				return true
			}
		}
		base = false
		if prefix != "" {
			prefix += "/"
		}
		claims = append(claims, KSClaim{ID: k.Named(), Prefix: prefix})
		return true
	})
	return claims
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
