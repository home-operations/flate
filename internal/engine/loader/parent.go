package loader

import (
	"cmp"
	"fmt"
	"io/fs"
	"path/filepath"
	"slices"
	"strings"

	securejoin "github.com/cyphar/filepath-securejoin"

	"github.com/home-operations/flate/internal/engine/kustomize/kustomizeinput"
	"github.com/home-operations/flate/internal/engine/selection"
	"github.com/home-operations/flate/internal/engine/source/safepath"
	"github.com/home-operations/flate/internal/engine/store"
	"github.com/home-operations/flate/pkg/manifest"
)

// Claims captures source-qualified claim evidence without publishing declarations.
func Claims(
	s *store.Store, repoRoot string, candidates []*manifest.Kustomization, indexed *manifest.DiscoveryIndex,
) *manifest.DiscoveryIndex {
	candidates = slices.DeleteFunc(
		candidates,
		func(ks *manifest.Kustomization) bool { return !s.Policy(ks.Named()).Root },
	)
	reader := documentReader{cache: map[string]document{}}
	reader.mountSources(s, nil, nil)
	var claimBuf [16]ownerClaims
	built := new(reader).buildClaims(repoRoot, candidates, func(id manifest.NamedResource) sourceScope {
		location := SourceView(s, id, repoRoot)
		return sourceScope{
			root: location.Path, pinned: location.Pinned, external: location.External,
			origin: new(reader).localBase(location.Path, repoRoot),
		}
	}, claimBuf[:0])
	index := &manifest.DiscoveryIndex{
		Claims:     map[string][]manifest.NamedResource{},
		Records:    map[manifest.NamedResource]manifest.DiscoveryRecord{},
		Components: new(reader).claimComponentIndex(),
	}
	for i, owner := range candidates {
		id := owner.Named()
		record, _ := indexed.Lookup(id)
		record.Declaration = owner
		index.Records[id] = record
		source := owner.EffectiveSourceID()
		if evidence, ok := indexed.Lookup(source); ok {
			index.Records[source] = evidence
		}
		for _, claims := range [2][]manifest.KSClaim{built[i].claims, built[i].aliases} {
			for _, claim := range claims {
				index.Claims[claim.Prefix] = appendUniqueProducer(index.Claims[claim.Prefix], claim.ID)
			}
		}
	}
	return index
}

func (l *Loader) claims(repoRoot string) []manifest.KSClaim {
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
		if view.replace {
			out[view.index].claims = nil
		}
		out[view.index].aliases = aliases
	}
	return out
}

type qualifiedClaim struct {
	ks      *manifest.Kustomization
	root    string
	index   int
	replace bool
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
			replace := filepath.Join(repoRoot, view.origin) == filepath.Clean(root)
			qualified = append(qualified, qualifiedClaim{ks: ks, root: root, index: i, replace: replace})
			if origin := view.origin; origin != "." && !replace {
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
	object manifest.BaseManifest,
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
		components[object.Named()] = refs
	}
	return components
}

func (r *documentReader) claimComponentIndex() map[string][]string {
	var components map[string][]string
	for path := range r.cache {
		body := r.cache[path].body
		if body == nil || len(body.Components) == 0 {
			continue
		}
		if components == nil {
			components = make(map[string][]string)
		}
		components[path] = body.Components
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
	info, err := fs.Lstat(inputFile.fs, inputFile.name)
	if err != nil {
		return info, fmt.Errorf("document lstat: %w", err)
	}
	return info, nil
}

// Readlink uses the same mounted source view as Lstat.
//
//nolint:gocritic // SecureJoin consumes filesystem methods from documentReader values.
func (r documentReader) Readlink(file string) (string, error) {
	input := new(r).input(filepath.Clean(file))
	target, err := fs.ReadLink(input.fs, input.name)
	if err != nil {
		return target, fmt.Errorf("document readlink: %w", err)
	}
	return target, nil
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

// SourceView projects the accepted material into loader coordinates.
func SourceView(s sourceStore, ref manifest.NamedResource, repoRoot string) SourceLocation {
	art, _ := s.GetArtifact(ref, store.ArtifactContent).(*store.SourceArtifact)
	if art != nil && art.FS != nil {
		return sourceLocation(art, repoRoot, false)
	}
	declared := s.GetObject(ref) != nil
	return sourceLocation(nil, repoRoot, declared || ref != manifest.BootstrapSourceID && ref.KindInfo().SourceArtifact)
}

func sourceLocation(art *store.SourceArtifact, repoRoot string, declared bool) SourceLocation {
	if art != nil && art.FS != nil {
		path := art.LocalPath
		if art.Provider == store.ProviderInput && repoRoot != "" && safepath.Contains(path, repoRoot) {
			path = repoRoot
		}
		return SourceLocation{
			Path:     path,
			Pinned:   art.Provider == store.ProviderCommit,
			External: art.Provider == store.ProviderFetch,
		}
	}
	if declared {
		return SourceLocation{External: true}
	}
	return SourceLocation{Path: repoRoot}
}

func (r *documentReader) localBase(root, repoRoot string) string {
	dr := *r
	artifact := dr.artifacts[root]
	if artifact != nil && (artifact.OriginPath(root) != root || artifact.Extent == store.ExtentComposed) {
		base, _ := artifact.IndexPath(repoRoot, ".")
		return base
	}
	return "."
}

func (r *documentReader) localPath(file string) string {
	if len(r.artifacts) == 0 {
		return file
	}
	return r.artifacts[r.mountedRoot(file)].OriginPath(file)
}

// WorkingTreePath uses the admitted mounts before the next mount refresh.
func (l *Loader) WorkingTreePath(file string) string {
	return cmp.Or(ProjectWorkingTreePath(file, l.documents.workingTree), file)
}

// ProjectWorkingTreePath returns an empty path when no admitted mount contains file.
func ProjectWorkingTreePath(file string, mounts map[string]*store.SourceArtifact) string {
	if mount := mounts[selectMount(file, "", mounts)]; mount != nil {
		return mount.OriginPath(file)
	}
	return ""
}
