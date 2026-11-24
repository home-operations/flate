package change

import (
	"slices"

	"github.com/home-operations/flate/internal/engine/store"
	"github.com/home-operations/flate/pkg/manifest"
)

type effectiveInventoryPair struct {
	input   [2]*store.SourceArtifact
	removed [2]map[manifest.NamedResource]bool
}

// WithEffectiveRemovals compares complete, current producer inventories at a paired drain.
func (s *Set) WithEffectiveRemovals(
	stores [2]*store.Store, owners [][2]manifest.NamedResource,
	indexedFile func(int, manifest.NamedResource) string,
) *Set {
	if s == nil {
		return nil
	}
	additions := make(map[manifest.NamedResource]effectiveInventoryPair)
	for i := range owners {
		s.compareEffectiveInventories(stores, &owners[i], additions, indexedFile)
	}
	if len(additions) == 0 {
		return s
	}
	result := *s
	result.effectiveRemovals = additions
	return &result
}

func (s *Set) compareEffectiveInventories(
	stores [2]*store.Store,
	owners *[2]manifest.NamedResource,
	additions map[manifest.NamedResource]effectiveInventoryPair,
	indexedFile func(int, manifest.NamedResource) string,
) {
	var pair [2]*store.Emission
	var sources [2]manifest.NamedResource
	for side, owner := range owners {
		st := stores[side]
		declaration := st.GetDeclaration(owner)
		inventory, _ := st.GetArtifact(owner, store.ArtifactContent).(*store.Emission)
		if inventory == nil || !inventory.Complete || !inventory.OutputReads.Valid() || declaration == nil {
			return
		}
		source, _, applies := declaration.SourcePath()
		if !applies {
			return
		}
		pair[side], sources[side] = inventory, source
	}
	source := sources[0]
	if source != sources[1] {
		return
	}
	_, snapshots, _ := s.ForSource(source)
	input, qualified := qualifiedEffectiveInputs(stores, source, snapshots)
	if !qualified {
		return
	}
	for side, inventory := range pair {
		present := make(map[manifest.NamedResource]bool, len(inventory.Children))
		for i := range inventory.Children {
			present[inventory.Children[i].Obj.Named()] = true
		}
		for i := range pair[1-side].Children {
			id := pair[1-side].Children[i].Obj.Named()
			if present[id] || stores[side].GetDeclaration(id) != nil ||
				survivingIndexedDeclaration(stores[side], source, id, indexedFile(side, id)) {
				continue
			}
			fact := additions[source]
			fact.input = input
			if fact.removed[side] == nil {
				fact.removed[side] = make(map[manifest.NamedResource]bool)
			}
			fact.removed[side][id] = true
			additions[source] = fact
		}
	}
}

func survivingIndexedDeclaration(st *store.Store, source, id manifest.NamedResource, file string) bool {
	if file == "" {
		return false
	}
	return slices.ContainsFunc(st.Productions(id, nil), func(p manifest.Production) bool {
		return p.Source == source && p.File == file && manifest.Resolve(p.Authored, id.Namespace, "") == id
	})
}

func qualifiedEffectiveInputs(
	stores [2]*store.Store, source manifest.NamedResource, snapshots [2]*store.SourceArtifact,
) ([2]*store.SourceArtifact, bool) {
	var inputs [2]*store.SourceArtifact
	for side, st := range stores {
		artifact, _ := st.GetArtifact(source, store.ArtifactContent).(*store.SourceArtifact)
		if artifact == nil || snapshots[side] == nil || !artifact.Reads.Valid() ||
			!snapshots[side].SameMaterial(artifact, store.ArtifactContent|store.ArtifactRevision) {
			return [2]*store.SourceArtifact{}, false
		}
		inputs[side] = artifact
	}
	return inputs, true
}

// EffectiveDeclarationChanged requires current source authority for an accepted paired removal.
func (s *Set) EffectiveDeclarationChanged(
	side manifest.TreeSide, id manifest.NamedResource, inputFor func(manifest.NamedResource) *store.SourceArtifact,
) bool {
	if s == nil || side > manifest.TreeHead {
		return false
	}
	for source, facts := range s.effectiveRemovals {
		if !facts.removed[manifest.TreeBase][id] && !facts.removed[manifest.TreeHead][id] {
			continue
		}
		if facts.matchesInput(side, inputFor(source)) {
			return true
		}
	}
	return false
}

func (p *effectiveInventoryPair) matchesInput(side manifest.TreeSide, input *store.SourceArtifact) bool {
	return input != nil && input == p.input[side] && input.Reads.Valid()
}
