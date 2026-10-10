package loader

import (
	"path/filepath"
	"slices"
	"strings"

	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

// Claims builds canonical local-tree claims, including committed source coordinates.
// Post-render reporting may call this on a fresh Loader to include late Kustomizations.
func (l *Loader) Claims(repoRoot string) []manifest.KSClaim {
	return l.claims(repoRoot, true)
}

func (l *Loader) claims(repoRoot string, localOnly bool) []manifest.KSClaim {
	if l.documents == nil && repoRoot != "" {
		l.documents = documentReader{}
	}
	kss := l.Store.ListAs[*manifest.Kustomization](manifest.KindKustomization)
	local := kss[:0]
	var pinned []*manifest.Kustomization
	var components map[manifest.NamedResource][]string
	for _, ks := range kss {
		root, pin, external := SourceView(l.Store, ks, repoRoot)
		if localOnly && external {
			continue
		}
		if pin {
			pinned = append(pinned, ks)
		} else {
			local = append(local, ks)
		}
		if root != "" {
			if k, _ := l.documents.read(filepath.Join(root, manifest.NormalizeClaimBase(ks.Path))); k != nil && len(k.Components) != 0 {
				if components == nil {
					components = map[manifest.NamedResource][]string{}
				}
				components[ks.Named()] = k.Components
			}
		}
	}
	claims := manifest.BuildKSClaims(local, components)
	for _, ks := range pinned {
		root, _, _ := SourceView(l.Store, ks, repoRoot)
		base, err := filepath.Rel(repoRoot, root)
		if err != nil {
			continue
		}
		for _, c := range manifest.BuildKSClaims([]*manifest.Kustomization{ks}, components) {
			claims = append(claims, c, manifest.KSClaim{ID: c.ID, Prefix: filepath.ToSlash(filepath.Join(base, c.Prefix)) + "/"})
		}
	}
	if len(pinned) != 0 {
		slices.SortStableFunc(claims, func(a, b manifest.KSClaim) int { return len(b.Prefix) - len(a.Prefix) })
	}
	return claims
}

// SourceView classifies a Kustomization's local source root, committed pin and
// external ownership. Missing source objects retain bootstrap alias semantics.
func SourceView(s *store.Store, ks *manifest.Kustomization, repoRoot string) (string, bool, bool) {
	ref := manifest.NamedResource{Kind: ks.SourceKind, Namespace: ks.SourceNamespace, Name: ks.SourceName}
	if ks.SourceName == "" || ref == manifest.BootstrapSourceID {
		return repoRoot, false, false
	}
	if art, ok := s.GetArtifact(ref).(*store.SourceArtifact); ok {
		if art.LocalRoot != "" && art.LocalRoot == repoRoot {
			return art.LocalPath, true, false
		}
		if art.LocalPath == repoRoot {
			return repoRoot, false, false
		}
	}
	return repoRoot, false, s.GetObject(ref) != nil
}

func longestStrictParent(prefixes []manifest.KSClaim, file string, self manifest.NamedResource, selfPrefixes map[string]struct{}) (manifest.NamedResource, bool) {
	slashFile := filepath.ToSlash(file)
	for _, p := range prefixes {
		if p.ID == self {
			continue
		}
		if _, claimed := selfPrefixes[p.Prefix]; claimed {
			continue
		}
		if strings.HasPrefix(slashFile, p.Prefix) {
			return p.ID, true
		}
	}
	return manifest.NamedResource{}, false
}
