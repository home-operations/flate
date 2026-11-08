package loader

import (
	"path/filepath"
	"slices"

	"github.com/home-operations/flate/internal/selection"
	"github.com/home-operations/flate/internal/store"
	"github.com/home-operations/flate/pkg/manifest"
)

//nolint:revive // Capture keeps the reader and both indexed/current production views explicit.
func rootSupport(
	evidence *selection.RootEvidence,
	objects admissionReader,
	index, currentOnly *manifest.DiscoveryIndex,
	sourceRoot string,
) {
	id := evidence.Identity
	evidence.Captured = true
	if !evidence.Scope.NativeEntry {
		object, policy := objects.Lookup(id)
		evidence.Declaration = objects.GetDeclaration(id)
		evidence.Supplied = object != nil && policy.Origin == manifest.SuppliedDeclaration &&
			evidence.Declaration == object && manifest.AdmitsRoot(object)
	}
	if evidence.Record.Path == "" && evidence.Record.Builder == "" {
		evidence.Scope.Candidate = evidence.Scope.Candidate || evidence.Declaration != nil
		evidence.Scope.NamespaceEligible = evidence.Scope.NamespaceEligible ||
			evidence.Declaration != nil && manifest.AdmitsRoot(evidence.Declaration)
	}
	if !evidence.Scope.NativeEntry {
		evidence.Productions = objects.Productions(id, index)
		evidence.LiveProductions = objects.Productions(id, currentOnly)
	}
	evidence.Scope.Records, evidence.Scope.Claims = index.Records, index.Claims
	if evidence.Scope.Application {
		location := SourceView(objects, evidence.Scope.ApplicationSource, sourceRoot)
		evidence.Scope.LocalApplication = !location.Pinned && !location.External
		target := filepath.Join(location.Path, evidence.Scope.ApplicationPath)
		input, _ := objects.GetArtifact(evidence.Scope.ApplicationSource, store.ArtifactContent).(*store.SourceArtifact)
		if input != nil {
			target = input.OriginPath(target)
		}
		coordinate, _ := filepath.Rel(sourceRoot, target)
		evidence.Scope.Claim = filepath.ToSlash(coordinate) + "/"
	}
	captureRootSources(evidence, objects)
}

func completeRootSupport(
	evidence *selection.RootEvidence,
	objects admissionReader,
	index *manifest.DiscoveryIndex,
) {
	if !selection.Root(*evidence).NeedsOwnerSupport {
		return
	}
	var owners *rootOwnerInventory
	if evidence.Record.Path != "" {
		owners = rootCoveringFacts(objects, index)
	}
	captureRootOwners(evidence, objects, index, owners)
}

func coveringFact(
	id manifest.NamedResource,
	object manifest.BaseManifest,
	objects admissionReader,
) selection.RootCoveringOwner {
	fact := selection.RootCoveringOwner{Identity: id, Object: object}
	if object == nil {
		return fact
	}
	source, path, applies := object.SourcePath()
	if !applies {
		return fact
	}
	if source == (manifest.NamedResource{}) || source.Name == "" {
		source = manifest.BootstrapSourceID
	}
	fact.Source = source
	input, _ := objects.GetArtifact(source, store.ArtifactContent).(*store.SourceArtifact)
	if input != nil {
		fact.Input = input
		fact.Root = input.OriginPath(input.LocalPath)
		fact.Path = input.OriginPath(filepath.Join(input.LocalPath, path))
	}
	return fact
}

func captureRootOwners(
	evidence *selection.RootEvidence,
	objects admissionReader,
	index *manifest.DiscoveryIndex,
	owners *rootOwnerInventory,
) {
	pending := owners.candidates(evidence)
	for cursor := 0; cursor < len(pending); cursor++ {
		owner := pending[cursor]
		fact := selection.RootCoveringOwner{Identity: owner}
		if owners != nil {
			if known, ok := owners.identities[owner]; ok {
				fact = known
			}
		}
		if evidence.Identity.KindInfo().Parent || fact.Object != nil {
			fact.Productions = objects.Productions(owner, index)
		}
		if evidence.Occurrence.Declaration != (selection.OccurrenceID{}) &&
			slices.ContainsFunc(evidence.Productions, func(p manifest.Production) bool { return p.Owner == owner }) {
			fact.Output, _ = objects.GetArtifact(owner, store.ArtifactContent).(*store.Emission)
		}
		evidence.CoveringOwners = append(evidence.CoveringOwners, fact)
		if !evidence.Identity.KindInfo().Parent {
			continue
		}
		for i := range fact.Productions {
			production := &fact.Productions[i]
			ancestor := production.Materializer(owner)
			if ancestor != (manifest.NamedResource{}) && ancestor != evidence.Identity &&
				!slices.Contains(pending, ancestor) {
				pending = append(pending, ancestor)
			}
		}
	}
	slices.SortFunc(
		evidence.CoveringOwners,
		func(a, b selection.RootCoveringOwner) int { return a.Identity.Compare(b.Identity) },
	)
}

func captureRootSources(evidence *selection.RootEvidence, objects admissionReader) {
	id := evidence.Identity
	source := manifest.BootstrapSourceID
	for i := range evidence.Productions {
		production := &evidence.Productions[i]
		if production.Source == (manifest.NamedResource{}) {
			continue
		}
		input, _ := objects.GetArtifact(production.Source, store.ArtifactContent).(*store.SourceArtifact)
		if input != nil && evidence.Occurrence.Declaration.Source != "" &&
			input.LocalPath != evidence.Occurrence.Declaration.Source &&
			input.OriginPath(input.LocalPath) != evidence.Occurrence.Declaration.Source {
			continue
		}
		source = production.Source
		break
	}
	for i, candidate := range [2]manifest.NamedResource{source, manifest.BootstrapSourceID} {
		if i != 0 && candidate == evidence.Sources[0].Source {
			evidence.Sources[i] = evidence.Sources[0]
			continue
		}
		artifact, _ := objects.GetArtifact(candidate, store.ArtifactContent).(*store.SourceArtifact)
		support := selection.RootSourceSupport{
			Source: candidate, Allowed: objects.SourceAdmission(id, candidate), Available: artifact != nil,
		}
		if artifact != nil {
			support.Path, support.Origin = artifact.LocalPath, artifact.OriginPath(artifact.LocalPath)
		}
		evidence.Sources[i] = support
	}
}
