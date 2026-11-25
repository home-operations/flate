package selection

import (
	"iter"
	"slices"

	"github.com/home-operations/flate/internal/engine/change"
	"github.com/home-operations/flate/internal/engine/store"
	"github.com/home-operations/flate/pkg/manifest"
)

// DeclarationDemand borrows immutable comparison and census evidence until completion returns.
type DeclarationDemand struct {
	Changes *change.Set
	Side    manifest.TreeSide
	Files   map[manifest.NamedResource][]string
}

// DemandDeclarations MUST include every possible location of an unindexed declaration move.
func DemandDeclarations(side manifest.TreeSide, changes *change.Set, index *manifest.DiscoveryIndex) DeclarationDemand {
	demand := DeclarationDemand{Changes: changes, Side: side}
	if changes == nil || side > manifest.TreeHead {
		return demand
	}
	var indexed map[string]struct{}
	if index != nil {
		indexed = make(map[string]struct{}, len(index.Records))
		for id := range index.Records {
			indexed[index.Records[id].File] = struct{}{}
		}
	}
	_, bootstrap, _ := changes.ForSource(manifest.BootstrapSourceID)
	for source := range changes.Sources() {
		delta, inputs, _ := changes.ForSource(source)
		paths := delta.Paths()
		needed := paths[:0]
		for _, file := range paths {
			present, known := delta.Present(side, file)
			peer, peerKnown := delta.Present(1-side, file)
			bothPresent := known && peerKnown && present && peer
			if !bothPresent || needsDeclarationFile(indexed, inputs[side], bootstrap[side], file) {
				needed = append(needed, file)
			}
		}
		if len(needed) != 0 {
			if demand.Files == nil {
				demand.Files = make(map[manifest.NamedResource][]string)
			}
			demand.Files[source] = needed
		}
	}
	return demand
}

func needsDeclarationFile(index map[string]struct{}, input, bootstrap *store.SourceArtifact, file string) bool {
	// Unknown coordinates cannot prove that declaration evidence is unnecessary.
	if input == nil || bootstrap == nil {
		return true
	}
	coordinate, local := input.IndexPath(bootstrap.LocalPath, file)
	if !local {
		return true
	}
	_, indexed := index[coordinate]
	return indexed
}

// Declarations excludes executable raw siblings from dependency acquisition.
func Declarations(documents []store.Child) iter.Seq[store.Child] {
	return func(yield func(store.Child) bool) {
		for i := range documents {
			child := &documents[i]
			info := manifest.KindOf(child.Obj)
			if info.Tracked && (info.IsSource() || !info.Runnable) && !yield(*child) {
				return
			}
		}
	}
}

func (c *admissionClosure) producedDeclarationStability(
	id, owner manifest.NamedResource,
	record *manifest.DiscoveryRecord,
	file *declarationFile,
) declarationStability {
	declared := c.record(file.declaration)
	if declared.File == "" || declared.File == declared.Builder {
		return declarationStability{}
	}
	ownerRecord := c.record(owner)
	ownerFile := ownerRecord.File
	if authored, located := c.catalogOccurrence(owner); located {
		ownerFile = c.fileCoordinate(authored.Declaration)
	}
	ownerKnown := ownerRecord.File != ownerRecord.Builder || c.objects.GetDeclaration(owner) != nil ||
		c.declarationEvidence(owner).accepted
	if !ownerKnown {
		return declarationStability{}
	}
	stable := !c.fileChanged(c.boundSource(c.source(owner)), ownerFile).changed &&
		!slices.ContainsFunc(c.productions(owner), c.changedProduction)
	// A remapped declaration derives its owner from the qualified production.
	if stable && file.requiresClaim {
		stable = claimContains(c.facts.SelectionIndex(), file.coordinate, id, &RootCoveringOwner{
			Identity: owner, Source: file.source, Input: file.input,
		})
	}
	c.checkDeclarationReferences(owner, &stable)
	c.checkDeclarationFiles(file.source, &stable, &declared, record)
	if parent := ownerRecord.Parent; stable &&
		(parent != (manifest.NamedResource{}) && parent != owner && !parent.ReportHidden()) {
		state := c.states[id]
		if state.comparingDeclaration {
			return declarationStability{stable: false, known: true}
		}
		state.comparingDeclaration = true
		c.states[id] = state
		stable = c.unchangedDeclaration(owner, parent, c.object(owner))
		state = c.states[id]
		state.comparingDeclaration = false
		c.states[id] = state
	}
	if stable {
		return declarationStability{stable: true, known: true}
	}
	return declarationStability{}
}

type declarationFile struct {
	coordinate          string
	source, declaration manifest.NamedResource
	input               *store.SourceArtifact
	requiresClaim       bool
}
