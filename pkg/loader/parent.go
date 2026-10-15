package loader

import (
	"io/fs"
	"path/filepath"
	"slices"
	"strings"

	securejoin "github.com/cyphar/filepath-securejoin"

	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

// Claims builds final-store claims for reporting, including external sources
// and committed source coordinates. Discovery MUST use Loader.Claims instead.
func Claims(s *store.Store, repoRoot string) []manifest.KSClaim {
	reader := documentReader{cache: map[string]document{}}
	reader.mountSources(s)
	return buildClaims(s, repoRoot, reader, s.ListAs[*manifest.Kustomization](manifest.KindKustomization))
}

// Claims reuses discovery's source-view documents while the Loader is mutable.
func (l *Loader) Claims(repoRoot string) []manifest.KSClaim {
	l.mountSources()
	return buildClaims(l.Store, repoRoot, l.documents, localKustomizations(l.Store, repoRoot))
}

func localKustomizations(s *store.Store, repoRoot string) []*manifest.Kustomization {
	return slices.DeleteFunc(s.ListAs[*manifest.Kustomization](manifest.KindKustomization), func(ks *manifest.Kustomization) bool {
		_, _, external := SourceView(s, ks, repoRoot)
		return external
	})
}

func buildClaims(s *store.Store, repoRoot string, documents documentReader, kss []*manifest.Kustomization) []manifest.KSClaim {
	var qualified []*manifest.Kustomization
	var components map[manifest.NamedResource][]string
	for i, ks := range kss {
		root, _, external := SourceView(s, ks, repoRoot)
		if external {
			continue
		}
		base := documents.claimBase(root, ks.Path)
		if base != manifest.NormalizeClaimBase(ks.Path) {
			c := *ks
			c.Path = base
			if c.Path == "" {
				c.Path = "."
			}
			ks = &c
			kss[i] = ks
		}
		if root != "" && root != repoRoot {
			qualified = append(qualified, ks)
		}
		if root != "" {
			if k, _ := documents.read(filepath.Join(root, base)); k != nil && len(k.Components) != 0 {
				if components == nil {
					components = map[manifest.NamedResource][]string{}
				}
				components[ks.Named()] = k.Components
			}
		}
	}
	claims := manifest.BuildKSClaims(kss, components)
	for _, ks := range qualified {
		root, _, _ := SourceView(s, ks, repoRoot)
		base, err := filepath.Rel(repoRoot, root)
		if err != nil {
			continue
		}
		for _, c := range manifest.BuildKSClaims([]*manifest.Kustomization{ks}, components) {
			claims = append(claims, manifest.KSClaim{ID: c.ID, Prefix: filepath.ToSlash(filepath.Join(base, c.Prefix)) + "/"})
		}
	}
	if len(qualified) != 0 {
		slices.SortStableFunc(claims, func(a, b manifest.KSClaim) int { return len(b.Prefix) - len(a.Prefix) })
	}
	return claims
}

// Failed resolution retains the lexical claim; reconciliation reports path errors.
func (r documentReader) claimBase(root, p string) string {
	base := manifest.NormalizeClaimBase(p)
	if root == "" || !strings.Contains(base, "/../") && !strings.HasSuffix(base, "/..") {
		return base
	}
	resolved, err := securejoin.SecureJoinVFS(root, base, r)
	if err != nil {
		return base
	}
	rel, err := filepath.Rel(root, resolved)
	if err != nil {
		return base
	}
	return manifest.NormalizeClaimBase(rel)
}

// Lstat keeps SecureJoin inside the mounted source filesystem.
func (r documentReader) Lstat(file string) (fs.FileInfo, error) {
	input, name := r.input(filepath.Clean(file))
	return fs.Lstat(input, name)
}

// Readlink uses the same mounted source view as Lstat.
func (r documentReader) Readlink(file string) (string, error) {
	input, name := r.input(filepath.Clean(file))
	return fs.ReadLink(input, name)
}

// SourceView classifies a Kustomization's admitted source coordinate, committed
// pin and external ownership. Mutable aliases retain empty Key. Unavailable
// sources MUST NOT borrow bootstrap bytes or claim unrelated working-tree files.
func SourceView(s *store.Store, ks *manifest.Kustomization, repoRoot string) (root string, pinned, external bool) {
	ref := ks.SourceID()
	if ref.Name == "" || ref == manifest.BootstrapSourceID {
		return repoRoot, false, false
	}
	if art, ok := s.GetArtifact(ref).(*store.SourceArtifact); ok && art.FS != nil {
		if art.LocalPath == repoRoot {
			return repoRoot, false, false
		}
		if art.LocalRoot != "" && art.LocalRoot == repoRoot {
			// Derived input keys pin selected bootstrap bytes, never authored refs.
			return art.LocalPath, art.Key != "" && !art.InputAlias, false
		}
	}
	return repoRoot, false, s.GetObject(ref) != nil || ref.Kind == manifest.KindOCIRepository
}
