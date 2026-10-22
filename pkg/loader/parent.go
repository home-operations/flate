package loader

import (
	"io/fs"
	"path/filepath"
	"slices"
	"strings"

	securejoin "github.com/cyphar/filepath-securejoin"

	"github.com/home-operations/flate/internal/kustomizeinput"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source/safepath"
	"github.com/home-operations/flate/pkg/store"
)

// Claims builds final-store claims for reporting, including external sources
// and committed source coordinates. Discovery MUST use Loader.Claims instead.
func Claims(s *store.Store, repoRoot string) []manifest.KSClaim {
	reader := documentReader{cache: map[string]document{}}
	reader.mountSources(s)
	return buildClaims(s, repoRoot, reader, s.ListAs[*manifest.Kustomization](manifest.FluxKustomizeDomain, manifest.KindKustomization))
}

// Claims reuses discovery's source-view documents while the Loader is mutable.
func (l *Loader) Claims(repoRoot string) []manifest.KSClaim {
	l.mountSources()
	var kss []*manifest.Kustomization
	for _, child := range l.Kustomizations() {
		ks := child.Obj.(*manifest.Kustomization)
		if _, _, external := SourceView(l.Store, ks.SourceID(), repoRoot); !external {
			kss = append(kss, ks)
		}
	}
	return buildClaims(l.Store, repoRoot, l.documents, kss)
}

func localKustomizations(s *store.Store, repoRoot string) []*manifest.Kustomization {
	return slices.DeleteFunc(s.ListAs[*manifest.Kustomization](manifest.FluxKustomizeDomain, manifest.KindKustomization), func(ks *manifest.Kustomization) bool {
		_, _, external := SourceView(s, ks.SourceID(), repoRoot)
		return external
	})
}

func buildClaims(s *store.Store, repoRoot string, documents documentReader, kss []*manifest.Kustomization) []manifest.KSClaim {
	var qualified []*manifest.Kustomization
	var components map[manifest.NamedResource][]string
	for i, ks := range kss {
		root, _, external := SourceView(s, ks.SourceID(), repoRoot)
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
			if origin := documents.localBase(root, repoRoot); origin != "." {
				c := *ks
				c.Path = filepath.ToSlash(filepath.Join(origin, base))
				kss[i] = &c
			}
		}
		if root != "" {
			var refs []string
			spec, _ := documents.read(filepath.Join(root, base))
			for ref := range kustomizeinput.Kustomization(spec) {
				if ref.Role == kustomizeinput.Component {
					refs = append(refs, ref.Path)
				}
			}
			if len(refs) != 0 {
				if components == nil {
					components = map[manifest.NamedResource][]string{}
				}
				components[ks.Named()] = refs
			}
		}
	}
	claims := manifest.BuildKSClaims(kss, components)
	for _, ks := range qualified {
		root, _, _ := SourceView(s, ks.SourceID(), repoRoot)
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

// SourceView classifies an admitted source coordinate, committed
// pin and external ownership. Mutable aliases retain empty Key. Unavailable
// sources MUST NOT borrow bootstrap bytes or claim unrelated working-tree files.
func SourceView(s *store.Store, ref manifest.NamedResource, repoRoot string) (root string, pinned, external bool) {
	if ref.Name == "" || ref == manifest.BootstrapSourceID {
		return repoRoot, false, false
	}
	if art, ok := s.GetArtifact(ref).(*store.SourceArtifact); ok && art.FS != nil {
		if art.LocalPath == repoRoot {
			return repoRoot, false, false
		}
		if art.LocalRoot != "" && (art.LocalRoot == repoRoot || art.InputAlias && safepath.Contains(repoRoot, art.LocalRoot)) {
			// Derived input keys pin selected bootstrap bytes, never authored refs.
			return art.LocalPath, art.Key != "" && !art.InputAlias, false
		}
	}
	if s.GetObject(ref) != nil {
		return repoRoot, false, true
	}
	obj, _ := ref.DecodeIdentity()
	_, entry := obj.(interface {
		BootstrapAlias(string) (manifest.BaseManifest, *manifest.GitRepository)
	})
	return repoRoot, false, entry
}

// Input aliases map source-relative readers and claims to their physical origin.
func (r documentReader) localBase(root, repoRoot string) string {
	if art := r.artifacts[root]; art != nil && art.InputAlias {
		base, _ := filepath.Rel(repoRoot, r.localPath(root))
		return filepath.ToSlash(base)
	}
	return "."
}

func (r documentReader) localPath(file string) string {
	root := r.mountedRoot(file)
	if art := r.artifacts[root]; art != nil && art.InputAlias {
		rel, _ := filepath.Rel(root, file)
		return filepath.Join(art.LocalRoot, rel)
	}
	return file
}
