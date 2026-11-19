package orchestrator

import "github.com/home-operations/flate/pkg/manifest"

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
