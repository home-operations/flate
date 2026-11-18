package selection

import (
	"path/filepath"
	"slices"

	"github.com/home-operations/flate/internal/engine/store"
	"github.com/home-operations/flate/pkg/manifest"
)

type rootReader interface {
	Lookup(manifest.NamedResource) (manifest.BaseManifest, manifest.DeclarationPolicy)
	GetDeclaration(manifest.NamedResource) manifest.BaseManifest
	GetArtifact(manifest.NamedResource, store.ArtifactRead) store.Artifact
	SourceAdmission(manifest.NamedResource, manifest.NamedResource) bool
	Productions(manifest.NamedResource, *manifest.DiscoveryIndex) []manifest.Production
	ListProductionCandidates(
		string,
		string,
		*manifest.DiscoveryIndex,
		map[manifest.NamedResource]bool,
	) []manifest.BaseManifest
}

type rootOwnerReader interface {
	RootOwners() *[]RootCoveringOwner
}

type prospectiveRootReader interface {
	rootReplacement(manifest.NamedResource) (map[manifest.NamedResource]manifest.DiscoveryRecord, bool)
	rootOwnerStorage(manifest.NamedResource) []RootCoveringOwner
}

type capturedRootEvidence struct {
	physical   RootEvidence
	support    RootEvidence
	supported  bool
	owners     [1]RootCoveringOwner
	ownersUsed bool
}

//nolint:gochecknoglobals // The immutable empty index excludes discovered ancestry from live support.
var currentRootIndex manifest.DiscoveryIndex

// CaptureRoot MUST borrow immutable discovery and production facts.
//
//nolint:gocritic // Capture owns its evidence while borrowing immutable discovery and production facts.
func CaptureRoot(facts RootEvidence, objects rootReader, index *manifest.DiscoveryIndex) RootEvidence {
	id := facts.Identity
	facts.Captured = true
	facts.CoveringOwners = nil
	facts.Scope.Records, facts.Scope.Claims = index.Records, index.Claims
	if !facts.Scope.NativeEntry {
		object, policy := objects.Lookup(id)
		facts.Declaration = objects.GetDeclaration(id)
		facts.Supplied = !facts.Incoming && object != nil && policy.Origin == manifest.SuppliedDeclaration &&
			facts.Declaration == object && manifest.AdmitsRoot(object)
		facts.Productions = objects.Productions(id, index)
		facts.LiveProductions = objects.Productions(id, &currentRootIndex)
	}
	if facts.Record.Path == "" && facts.Record.Builder == "" {
		facts.Scope.Candidate = facts.Scope.Candidate || facts.Declaration != nil
		facts.Scope.NamespaceEligible = facts.Scope.NamespaceEligible ||
			facts.Declaration != nil && manifest.AdmitsRoot(facts.Declaration)
	}
	captureRootSources(&facts, objects)
	if Root(facts).NeedsOwnerSupport {
		captureRootOwners(&facts, objects, index)
	}
	if reads, ok := objects.(*store.ReadSet); ok {
		facts.Stale = facts.Stale || !reads.Valid()
	}
	return facts
}

func captureRootSources(facts *RootEvidence, objects rootReader) {
	source := manifest.BootstrapSourceID
	for i := range facts.Productions {
		production := &facts.Productions[i]
		if production.Source == (manifest.NamedResource{}) {
			continue
		}
		input, _ := objects.GetArtifact(production.Source, store.ArtifactContent).(*store.SourceArtifact)
		if input != nil && facts.Occurrence.Declaration.Source != "" &&
			input.LocalPath != facts.Occurrence.Declaration.Source &&
			input.OriginPath(input.LocalPath) != facts.Occurrence.Declaration.Source {
			continue
		}
		source = production.Source
		break
	}
	for i, candidate := range [...]manifest.NamedResource{source, manifest.BootstrapSourceID} {
		if i != 0 && candidate == facts.Sources[0].Source {
			facts.Sources[i] = facts.Sources[0]
			continue
		}
		input, _ := objects.GetArtifact(candidate, store.ArtifactContent).(*store.SourceArtifact)
		support := RootSourceSupport{
			Source: candidate, Allowed: objects.SourceAdmission(facts.Identity, candidate), Available: input != nil,
		}
		if input != nil {
			support.Path, support.Origin = input.LocalPath, input.OriginPath(input.LocalPath)
		}
		facts.Sources[i] = support
	}
}

func rootOwnerFact(object manifest.BaseManifest, objects rootReader) RootCoveringOwner {
	fact := RootCoveringOwner{Object: object}
	if object == nil {
		return fact
	}
	fact.Identity = object.Named()
	source, path, applies := object.SourcePath()
	if !applies {
		return fact
	}
	fact.Source = source
	input, _ := objects.GetArtifact(source, store.ArtifactContent).(*store.SourceArtifact)
	if input != nil {
		fact.Input, fact.Root = input, input.OriginPath(input.LocalPath)
		fact.Path = input.OriginPath(filepath.Join(input.LocalPath, path))
		if !containsPath(fact.Root, fact.Path) {
			fact.Path = ""
		}
	}
	return fact
}

func captureRootOwners(facts *RootEvidence, objects rootReader, index *manifest.DiscoveryIndex) {
	var owners []RootCoveringOwner
	if facts.Record.Path != "" {
		owners = rootOwnerCandidates(objects, index, facts.Sources[1].Path)
	}
	if reader, ok := objects.(prospectiveRootReader); ok {
		facts.CoveringOwners = reader.rootOwnerStorage(facts.Identity)
	}
	var materializers [1]manifest.NamedResource
	pending := facts.rootMaterializers(materializers[:0])
	claims := index
	if len(owners) != 0 {
		claims = owners[0].claims
	}
	facts.Scope.Records, facts.Scope.Claims = claims.Records, claims.Claims
	for i := range owners {
		owner := &owners[i]
		if rootClaimed(facts, owner, claims) && !slices.Contains(pending, owner.Identity) {
			pending = append(pending, owner.Identity)
		}
	}
	for cursor := 0; cursor < len(pending); cursor++ {
		id := pending[cursor]
		candidate := facts.captureOwner(id, objects, index, owners)
		candidate.covering = rootClaimed(facts, &candidate, claims)
		facts.CoveringOwners = append(facts.CoveringOwners, candidate)
		if !facts.Identity.KindInfo().Parent {
			continue
		}
		for i := range candidate.Productions {
			ancestor := candidate.Productions[i].Materializer(id)
			if ancestor != (manifest.NamedResource{}) && ancestor != facts.Identity &&
				!slices.Contains(pending, ancestor) {
				pending = append(pending, ancestor)
			}
		}
	}
	slices.SortFunc(facts.CoveringOwners, func(a, b RootCoveringOwner) int { return a.Identity.Compare(b.Identity) })
}

func rootOwnerCandidates(objects rootReader, index *manifest.DiscoveryIndex, root string) []RootCoveringOwner {
	cache, cached := objects.(rootOwnerReader)
	if cached && *cache.RootOwners() != nil {
		return *cache.RootOwners()
	}
	owners := []RootCoveringOwner{}
	for info := range manifest.Kinds() {
		if !info.Structural {
			continue
		}
		for _, object := range objects.ListProductionCandidates(info.Group, info.Kind, index, nil) {
			owners = append(owners, rootOwnerFact(object, objects))
		}
	}
	captureOwnerClaims(owners, index, root)
	if cached {
		*cache.RootOwners() = owners
	}
	return owners
}

func captureOwnerClaims(owners []RootCoveringOwner, index *manifest.DiscoveryIndex, root string) {
	var storage [16]manifest.ClaimInput
	inputs := storage[:0]
	for i := range owners {
		owner := &owners[i]
		if owner.Input == nil || owner.Path == "" {
			continue
		}
		base, err := filepath.Rel(owner.Root, owner.Path)
		if err != nil || !filepath.IsLocal(base) {
			continue
		}
		pinned := owner.Source != manifest.BootstrapSourceID && owner.Input.LocalPath != root
		pinned = pinned && owner.Input.Content != "" && !owner.Input.InputAlias
		inputs = append(inputs, manifest.ClaimInput{
			Declaration: owner.Object,
			Base:        filepath.ToSlash(base), Root: owner.Root, IndexRoot: root,
			Components: index.Components[filepath.Join(owner.Input.LocalPath, base)], Pinned: pinned,
		})
	}
	claims := index.CaptureClaims(inputs)
	for i := range owners {
		owners[i].claims = claims
		owners[i].claimRoot = root
	}
}

func rootClaimed(facts *RootEvidence, owner *RootCoveringOwner, index *manifest.DiscoveryIndex) bool {
	coordinate, known := rootClaimCoordinate(facts, owner, owner.claimRoot)
	return known && claimContains(index, coordinate, facts.Identity, owner)
}

func rootClaimCoordinate(facts *RootEvidence, owner *RootCoveringOwner, root string) (string, bool) {
	if owner.Input == nil || owner.Path == "" || facts.Record.Path == "" {
		return "", false
	}
	source := facts.DeclarationSource
	if source == "" {
		source = facts.Occurrence.Declaration.Source
	}
	if source == "" {
		source = facts.Sources[0].Origin
	}
	if source != owner.Input.LocalPath && source != owner.Root {
		return "", false
	}
	relative, err := filepath.Rel(owner.Root, owner.Input.OriginPath(facts.Record.Path))
	if err != nil || !filepath.IsLocal(relative) {
		return "", false
	}
	return owner.Input.IndexPath(root, filepath.ToSlash(relative))
}

func claimContains(
	index *manifest.DiscoveryIndex,
	coordinate string,
	subject manifest.NamedResource,
	owner *RootCoveringOwner,
) bool {
	for group := range index.ClaimOwners(coordinate, owner.Source, subject, nil) {
		if slices.Contains(group, owner.Identity) {
			return true
		}
	}
	return false
}

func (f *RootEvidence) captureOwner(
	id manifest.NamedResource,
	objects rootReader,
	index *manifest.DiscoveryIndex,
	owners []RootCoveringOwner,
) RootCoveringOwner {
	prospective, preparing := objects.(prospectiveRootReader)
	candidate := RootCoveringOwner{Identity: id}
	if i := slices.IndexFunc(owners, func(owner RootCoveringOwner) bool { return owner.Identity == id }); i >= 0 {
		candidate = owners[i]
	}
	if f.Identity.KindInfo().Parent || candidate.Object != nil {
		candidate.Productions = objects.Productions(id, index)
	}
	if f.Occurrence.Declaration != (OccurrenceID{}) &&
		slices.ContainsFunc(f.Productions, func(p manifest.Production) bool { return p.Owner == id }) {
		candidate.Output, _ = objects.GetArtifact(id, store.ArtifactContent).(*store.Emission)
		if preparing {
			candidate.Applications, candidate.Replaced = prospective.rootReplacement(id)
		}
	}
	return candidate
}

func (f *RootEvidence) rootMaterializers(pending []manifest.NamedResource) []manifest.NamedResource {
	for i := range f.Productions {
		production := &f.Productions[i]
		for _, owner := range [...]manifest.NamedResource{production.Materializer(f.Identity), production.Owner} {
			if preparedRootBuilder(f, owner) {
				continue
			}
			if owner != (manifest.NamedResource{}) && owner != f.Identity && !slices.Contains(pending, owner) {
				pending = append(pending, owner)
			}
		}
	}
	return pending
}
