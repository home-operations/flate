package loader

import (
	"slices"

	"github.com/home-operations/flate/pkg/manifest"
)

func (l *Loader) recordUnownedProductions(b *selfProduceBuilder) {
	var candidates map[string][]manifest.NamedResource
	//nolint:gocritic // Files' public iterator yields value snapshots and cannot be indexed.
	for id, r := range l.Index.Files() {
		declaration, include := l.unownedProductionDeclaration(id, new(r))
		if !include {
			continue
		}
		if (declaration.obj != nil && manifest.ProducerNeedsCandidates(declaration.obj) || declaration.fact != nil && declaration.fact.facts.needsCandidates) &&
			candidates == nil {
			candidates = map[string][]manifest.NamedResource{}
			//nolint:gocritic // Files' public iterator yields value snapshots and cannot be indexed.
			for _, input := range l.Index.Files() {
				root := new(l.documents).mountedRoot(input.Path)
				candidates[root] = append(candidates[root], input.Refs...)
			}
		}
		v := graphVisit{owner: id, source: manifest.BootstrapSourceID, coordinate: r.File}
		declaration.visit = v
		b.recordProducer(new(declaration), candidates[new(l.documents).mountedRoot(r.Path)])
	}
}

func (l *Loader) unownedProductionDeclaration(
	id manifest.NamedResource,
	recordArg *manifest.DiscoveryRecord,
) (producerDeclaration, bool) {
	r := *recordArg
	if !id.KindInfo().Runnable || r.Path == "" {
		return producerDeclaration{}, false
	}
	obj := l.Store.GetObject(id)
	declaration := producerDeclaration{obj: obj, identity: id, authored: r.Authored}
	if l.documents.compact {
		if occurrence, ok := l.locations[id]; ok {
			declaration.fact = &occurrence
		}
	}
	if obj == nil && declaration.fact == nil {
		return producerDeclaration{}, false
	}
	if slices.ContainsFunc(r.Productions, func(p manifest.Production) bool {
		return p.Owner != (manifest.NamedResource{})
	}) {
		return producerDeclaration{}, false
	}
	ownership := l.Index.OwnersOfFile(r.File)
	if slices.ContainsFunc(append(ownership.Owners, ownership.Ancestors...), func(owner manifest.NamedResource) bool {
		return owner != id
	}) {
		return producerDeclaration{}, false
	}
	return declaration, true
}
