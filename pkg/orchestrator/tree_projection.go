package orchestrator

import (
	"maps"
	"slices"

	"github.com/home-operations/flate/pkg/manifest"
)

func pairTreeSides(trees []Tree) []Tree {
	byID := make(map[manifest.TreeID]Tree, len(trees))
	sides := make(map[manifest.TreeSide]bool)
	for _, tree := range trees {
		byID[tree.ID] = tree
		sides[tree.ID.Side] = true
	}
	if sides[manifest.TreeBase] && sides[manifest.TreeHead] {
		for _, tree := range trees {
			other := tree.ID
			other.Side = manifest.TreeHead - other.Side
			if _, present := byID[other]; !present {
				byID[other] = Tree{ID: other}
				tree.BaseInput, tree.ExternalChanges = nil, nil
				byID[tree.ID] = tree
			}
		}
	}
	trees = trees[:0]
	for _, tree := range byID {
		trees = append(trees, tree)
	}
	return trees
}

func projectEntryReaders(index *manifest.DiscoveryIndex, from, to manifest.NamedResource) *manifest.DiscoveryIndex {
	if index == nil || from == to || from == (manifest.NamedResource{}) ||
		to == (manifest.NamedResource{}) {
		return index
	}
	projected := *index
	projected.Readers = maps.Clone(index.Readers)
	for file, readers := range index.Readers {
		if !slices.ContainsFunc(
			readers,
			func(reader manifest.FileReader) bool { return reader.Reader == from },
		) {
			continue
		}
		readers = slices.Clone(readers)
		for i := range readers {
			if readers[i].Reader == from {
				readers[i].Reader = to
			}
		}
		projected.Readers[file] = readers
	}
	return &projected
}
