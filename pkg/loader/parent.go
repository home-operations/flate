package loader

import (
	"io/fs"
	"path/filepath"
	"slices"
	"strings"

	securejoin "github.com/cyphar/filepath-securejoin"

	"github.com/home-operations/flate/internal/kustomizeinput"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/selection"
	"github.com/home-operations/flate/pkg/source/safepath"
	"github.com/home-operations/flate/pkg/store"
)

// Claims builds reporting claims from caller-owned candidates, including external
// sources and committed source coordinates. Discovery MUST use Loader.Claims instead.
func Claims(s *store.Store, repoRoot string, candidates []*manifest.Kustomization) []manifest.KSClaim {
	candidates = slices.DeleteFunc(
		candidates,
		func(ks *manifest.Kustomization) bool { return !s.Policy(ks.Named()).Root },
	)
	reader := documentReader{cache: map[string]document{}}
	reader.mountSources(s, nil)
	var claimBuf [16]ownerClaims
	built := new(reader).buildClaims(repoRoot, candidates, func(id manifest.NamedResource) sourceScope {
		location := SourceView(s, id, repoRoot)
		return sourceScope{
			root:     location.Path,
			pinned:   location.Pinned,
			external: location.External,
			origin:   new(reader).localBase(location.Path, repoRoot),
		}
	}, claimBuf[:0])
	claims, _ := projectClaims(len(built), func(i int) ownerClaims { return built[i] })
	return claims
}

// Claims reuses discovery's source-view documents while the Loader is mutable.
func (l *Loader) Claims(repoRoot string) []manifest.KSClaim {
	l.refreshSources(nil)
	l.refreshClaims(repoRoot, l.kustomizations())
	claims, borrowed := l.claimView(repoRoot, func(input scopeInput) bool {
		r, _ := l.Index.Lookup(input.owner.Named())
		return l.documents.cache[r.Path].eligible != selection.RootInactive
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
	qualification := dr.qualifyClaims(repoRoot, kss, source)
	claims := manifest.BuildKSClaims(kss, qualification.components)
	slices.SortStableFunc(claims, func(a, b manifest.KSClaim) int { return a.ID.Compare(b.ID) })
	next := 0
	for i, ks := range kss {
		first := next
		for next < len(claims) && claims[next].ID == ks.Named() {
			next++
		}
		out[i].claims = claims[first:next:next]
	}
	for _, view := range qualification.qualified {
		base, err := filepath.Rel(repoRoot, view.root)
		if err != nil {
			continue
		}
		aliases := manifest.BuildKSClaims([]*manifest.Kustomization{view.ks}, qualification.components)
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

type claimQualification struct {
	qualified  []qualifiedClaim
	components map[manifest.NamedResource][]string
}

func (r *documentReader) qualifyClaims(
	repoRoot string,
	kss []*manifest.Kustomization,
	source func(manifest.NamedResource) sourceScope,
) claimQualification {
	var qualified []qualifiedClaim
	var components map[manifest.NamedResource][]string
	for i, ks := range kss {
		view := source(ks.EffectiveSourceID())
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
	return claimQualification{qualified: qualified, components: components}
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
	spec := r.read(filepath.Join(root, base)).body
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
	inputFile := new(r).input(filepath.Clean(file))
	return fs.Lstat(inputFile.fs, inputFile.name) //nolint:wrapcheck // SecureJoin requires original filesystem errors.
}

// Readlink uses the same mounted source view as Lstat.
//
//nolint:gocritic // SecureJoin consumes filesystem methods from documentReader values.
func (r documentReader) Readlink(file string) (string, error) {
	input := new(r).input(filepath.Clean(file))
	return fs.ReadLink(input.fs, input.name) //nolint:wrapcheck // SecureJoin requires original filesystem errors.
}

type sourceStore interface {
	GetObject(manifest.NamedResource) manifest.BaseManifest
	GetArtifact(manifest.NamedResource, store.ArtifactRead) store.Artifact
}

// SourceLocation binds a source path to its pin and ownership.
type SourceLocation struct {
	Path             string
	Pinned, External bool
}

// SourceView classifies an admitted source coordinate, committed pin and external ownership.
// Unavailable sources MUST NOT borrow bootstrap bytes or claim unrelated working-tree files.
func SourceView(s sourceStore, ref manifest.NamedResource, repoRoot string) SourceLocation {
	if ref.Name == "" || ref == manifest.BootstrapSourceID {
		return SourceLocation{Path: repoRoot, Pinned: false, External: false}
	}
	art, _ := s.GetArtifact(ref, store.ArtifactContent).(*store.SourceArtifact)
	return sourceView(ref, repoRoot, art, s.GetObject(ref))
}

func sourceView(
	ref manifest.NamedResource,
	repoRoot string,
	art *store.SourceArtifact,
	object manifest.BaseManifest,
) SourceLocation {
	if art != nil && art.FS != nil {
		if art.LocalPath == repoRoot {
			return SourceLocation{Path: repoRoot, Pinned: false, External: false}
		}
		if art.LocalRoot != "" &&
			(art.LocalRoot == repoRoot || art.InputAlias && safepath.Contains(repoRoot, art.LocalRoot)) {
			// Derived input keys pin selected bootstrap bytes, never authored refs.
			return SourceLocation{Path: art.LocalPath, Pinned: art.Content != "" && !art.InputAlias, External: false}
		}
	}
	if object != nil {
		return SourceLocation{Path: repoRoot, Pinned: false, External: true}
	}
	return SourceLocation{Path: repoRoot, Pinned: false, External: ref.CanBindLocal()}
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

// AppliesToEntry reports whether a resolved application reads the admitted entry.
func AppliesToEntry(
	s sourceStore,
	application *manifest.EntryApplication,
	input *store.SourceArtifact,
	entry string,
) bool {
	if input == nil {
		return false
	}
	source, _ := s.GetArtifact(application.Source, store.ArtifactContent).(*store.SourceArtifact)
	if source == nil {
		return false
	}
	location2 := SourceView(s, application.Source, input.LocalPath)
	if location2.Pinned || location2.External {
		return false
	}
	target := filepath.Join(location2.Path, manifest.NormalizeClaimBase(application.Path))
	return safepath.Contains(location2.Path, target) && sameOriginPath(input, source, entry, target)
}

func sameOriginPath(input, source *store.SourceArtifact, entry, target string) bool {
	if input != nil {
		entry = input.OriginPath(entry)
	}
	if source != nil {
		target = source.OriginPath(target)
	}
	return entry == target
}
