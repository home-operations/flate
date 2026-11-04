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

// Claims builds reporting claims from caller-owned candidates, including external
// sources and committed source coordinates. Discovery MUST use Loader.Claims instead.
func Claims(s *store.Store, repoRoot string, candidates []*manifest.Kustomization) []manifest.KSClaim {
	reader := documentReader{cache: map[string]document{}}
	reader.mountSources(s)
	var claimBuf [16]ownerClaims
	built := new(reader).buildClaims(repoRoot, candidates, func(id manifest.NamedResource) sourceScope {
		root, pinned, external := SourceView(s, id, repoRoot)
		return sourceScope{
			root:     root,
			pinned:   pinned,
			external: external,
			origin:   new(reader).localBase(root, repoRoot),
		}
	}, claimBuf[:0])
	claims, _ := projectClaims(len(built), func(i int) ownerClaims { return built[i] })
	return claims
}

// Claims reuses discovery's source-view documents while the Loader is mutable.
func (l *Loader) Claims(repoRoot string) []manifest.KSClaim {
	l.refreshSources()
	l.refreshClaims(repoRoot, l.kustomizations())
	claims, borrowed := l.claimView(repoRoot, func(input scopeInput) bool {
		r, _ := l.Index.Lookup(input.owner.Named())
		return l.documents.cache[r.Path].eligible != fileInactive
	})
	if borrowed {
		return slices.Clone(claims)
	}
	return claims
}

type ownerClaims struct {
	claims, aliases []manifest.KSClaim
}

// Borrowed projections MUST be cloned before exposing cached input storage.
func projectClaims(countOwners int, ownerAt func(int) ownerClaims) ([]manifest.KSClaim, bool) {
	count := 0
	var first []manifest.KSClaim
	for i := range countOwners {
		owner := ownerAt(i)
		count += len(owner.claims) + len(owner.aliases)
		if len(first) == 0 {
			first = owner.claims
			if len(first) == 0 {
				first = owner.aliases
			}
		}
	}
	if count == len(first) {
		return slices.Clip(first), true
	}
	claims := make([]manifest.KSClaim, 0, count)
	for i := range countOwners {
		owner := ownerAt(i)
		claims = append(claims, owner.claims...)
	}
	for i := range countOwners {
		owner := ownerAt(i)
		claims = append(claims, owner.aliases...)
	}
	slices.SortStableFunc(claims, func(a, b manifest.KSClaim) int { return len(b.Prefix) - len(a.Prefix) })
	return claims, false
}

// Owners are distinct and ordered by identity; captured occurrences derive separately.
func (r *documentReader) buildClaims(
	repoRoot string,
	kss []*manifest.Kustomization,
	source func(manifest.NamedResource) sourceScope,
	out []ownerClaims,
) []ownerClaims {
	dr := *r
	if cap(out) < len(kss) {
		out = make([]ownerClaims, len(kss))
	} else {
		out = out[:len(kss)]
		clear(out)
	}
	qualified, components := dr.qualifyClaims(repoRoot, kss, source)
	claims := manifest.BuildKSClaims(kss, components)
	slices.SortStableFunc(claims, func(a, b manifest.KSClaim) int { return a.ID.Compare(b.ID) })
	next := 0
	for i, ks := range kss {
		first := next
		for next < len(claims) && claims[next].ID == ks.Named() {
			next++
		}
		out[i].claims = claims[first:next:next]
	}
	for _, view := range qualified {
		base, err := filepath.Rel(repoRoot, view.root)
		if err != nil {
			continue
		}
		aliases := manifest.BuildKSClaims([]*manifest.Kustomization{view.ks}, components)
		for i := range aliases {
			aliases[i].Prefix = filepath.ToSlash(filepath.Join(base, aliases[i].Prefix)) + "/"
		}
		out[view.index].aliases = aliases
	}
	return out
}

type qualifiedClaim struct {
	ks    *manifest.Kustomization
	root  string
	index int
}

func (r *documentReader) qualifyClaims(
	repoRoot string,
	kss []*manifest.Kustomization,
	source func(manifest.NamedResource) sourceScope,
) ([]qualifiedClaim, map[manifest.NamedResource][]string) {
	var qualified []qualifiedClaim
	var components map[manifest.NamedResource][]string
	for i, ks := range kss {
		view := source(ks.SourceID())
		root := view.root
		if view.external {
			continue
		}
		base := new(*r).claimBase(root, ks.Path)
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
			qualified = append(qualified, qualifiedClaim{ks, root, i})
			if origin := view.origin; origin != "." {
				c := *ks
				c.Path = filepath.ToSlash(filepath.Join(origin, base))
				kss[i] = &c
			}
		}
		components = r.claimComponents(ks, root, base, components)
	}
	return qualified, components
}

func (r *documentReader) claimComponents(
	ks *manifest.Kustomization,
	root, base string,
	components map[manifest.NamedResource][]string,
) map[manifest.NamedResource][]string {
	if root == "" {
		return components
	}
	var refs []string
	spec, _ := r.read(filepath.Join(root, base))
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
	return components
}

// Failed resolution retains the lexical claim; reconciliation reports path errors.
func (r *documentReader) claimBase(root, p string) string {
	dr := *r
	base := manifest.NormalizeClaimBase(p)
	if root == "" || !strings.Contains(base, "/../") && !strings.HasSuffix(base, "/..") {
		return base
	}
	resolved, err := securejoin.SecureJoinVFS(root, base, dr)
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
//
//nolint:gocritic // SecureJoin consumes filesystem methods from documentReader values.
func (r documentReader) Lstat(file string) (fs.FileInfo, error) {
	input, name := new(r).input(filepath.Clean(file))
	return fs.Lstat(input, name) //nolint:wrapcheck // SecureJoin requires original filesystem errors.
}

// Readlink uses the same mounted source view as Lstat.
//
//nolint:gocritic // SecureJoin consumes filesystem methods from documentReader values.
func (r documentReader) Readlink(file string) (string, error) {
	input, name := new(r).input(filepath.Clean(file))
	return fs.ReadLink(input, name) //nolint:wrapcheck // SecureJoin requires original filesystem errors.
}

type sourceStore interface {
	GetObject(manifest.NamedResource) manifest.BaseManifest
	GetArtifact(manifest.NamedResource, store.ArtifactRead) store.Artifact
}

// SourceView classifies an admitted source coordinate, committed pin and external ownership.
// Unavailable sources MUST NOT borrow bootstrap bytes or claim unrelated working-tree files.
func SourceView(s sourceStore, ref manifest.NamedResource, repoRoot string) (string, bool, bool) {
	if ref.Name == "" || ref == manifest.BootstrapSourceID {
		return repoRoot, false, false
	}
	art, _ := s.GetArtifact(ref, store.ArtifactContent).(*store.SourceArtifact)
	return sourceView(ref, repoRoot, art, s.GetObject(ref))
}

func sourceView(
	ref manifest.NamedResource,
	repoRoot string,
	art *store.SourceArtifact,
	object manifest.BaseManifest,
) (string, bool, bool) {
	if art != nil && art.FS != nil {
		if art.LocalPath == repoRoot {
			return repoRoot, false, false
		}
		if art.LocalRoot != "" &&
			(art.LocalRoot == repoRoot || art.InputAlias && safepath.Contains(repoRoot, art.LocalRoot)) {
			// Derived input keys pin selected bootstrap bytes, never authored refs.
			return art.LocalPath, art.Content != "" && !art.InputAlias, false
		}
	}
	if object != nil {
		return repoRoot, false, true
	}
	return repoRoot, false, ref.CanBindLocal()
}

// Input aliases map source-relative readers and claims to their physical origin.
func (r *documentReader) localBase(root, repoRoot string) string {
	dr := *r
	if origin := dr.artifacts[root].OriginPath(root); origin != root {
		base, _ := filepath.Rel(repoRoot, origin)
		return filepath.ToSlash(base)
	}
	return "."
}

func (r *documentReader) localPath(file string) string {
	dr := *r
	return dr.artifacts[new(dr).mountedRoot(file)].OriginPath(file)
}
