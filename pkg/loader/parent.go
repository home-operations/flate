package loader

import (
	"path/filepath"
	"slices"

	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

// Claims builds final-store claims for reporting, including external sources
// and committed source coordinates. Discovery MUST use Loader.Claims instead.
func Claims(s *store.Store, repoRoot string) []manifest.KSClaim {
	return buildClaims(s, repoRoot, documentReader{}, s.ListAs[*manifest.Kustomization](manifest.KindKustomization))
}

// Claims reuses discovery's source-view documents while the Loader is mutable.
func (l *Loader) Claims(repoRoot string) []manifest.KSClaim {
	if l.documents == nil {
		l.documents = documentReader{}
	}
	return buildClaims(l.Store, repoRoot, l.documents, localKustomizations(l.Store, repoRoot))
}

func localKustomizations(s *store.Store, repoRoot string) []*manifest.Kustomization {
	return slices.DeleteFunc(s.ListAs[*manifest.Kustomization](manifest.KindKustomization), func(ks *manifest.Kustomization) bool {
		_, _, external := SourceView(s, ks, repoRoot)
		return external
	})
}

func buildClaims(s *store.Store, repoRoot string, documents documentReader, kss []*manifest.Kustomization) []manifest.KSClaim {
	local := kss[:0]
	var pinned []*manifest.Kustomization
	var components map[manifest.NamedResource][]string
	for _, ks := range kss {
		root, pin, _ := SourceView(s, ks, repoRoot)
		if pin {
			pinned = append(pinned, ks)
		} else {
			local = append(local, ks)
		}
		if root != "" {
			if k, _ := documents.read(filepath.Join(root, manifest.NormalizeClaimBase(ks.Path))); k != nil && len(k.Components) != 0 {
				if components == nil {
					components = map[manifest.NamedResource][]string{}
				}
				components[ks.Named()] = k.Components
			}
		}
	}
	claims := manifest.BuildKSClaims(local, components)
	for _, ks := range pinned {
		root, _, _ := SourceView(s, ks, repoRoot)
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
// external ownership. Missing Git source objects retain bootstrap alias semantics.
// External-sourced Kustomizations MUST NOT claim unrelated working-tree files.
func SourceView(s *store.Store, ks *manifest.Kustomization, repoRoot string) (root string, pinned, external bool) {
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
	return repoRoot, false, s.GetObject(ref) != nil || ks.SourceKind == manifest.KindOCIRepository
}
