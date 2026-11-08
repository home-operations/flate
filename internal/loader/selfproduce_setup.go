package loader

import "github.com/home-operations/flate/pkg/manifest"

func (l *Loader) pinnedProductionOwners() map[manifest.NamedResource]bool {
	var pinned map[manifest.NamedResource]bool
	for _, input := range l.scopes.inputs {
		if l.scopes.sources[input.source].pinned {
			if pinned == nil {
				pinned = map[manifest.NamedResource]bool{}
			}
			pinned[input.owner.Named()] = true
		}
	}
	return pinned
}

func (l *Loader) scanRemainingGenerators(b *selfProduceBuilder, remaining map[string]struct{}) {
	//nolint:gocritic // Map iteration supplies value snapshots without an additional lookup.
	for dir, doc := range l.documents.cache {
		b.scanExclusions = b.scanExclusions || doc.scanExcluded
		if doc.body != nil && !doc.selected && len(doc.body.ConfigMapGenerator)+len(doc.body.SecretGenerator) != 0 {
			remaining[dir] = struct{}{}
		}
	}
}
