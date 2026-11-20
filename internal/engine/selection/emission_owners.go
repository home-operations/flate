package selection

import (
	"slices"

	"github.com/home-operations/flate/internal/engine/store"
	"github.com/home-operations/flate/pkg/manifest"
)

type documentOwner struct {
	child, emitter manifest.NamedResource
}

type documentOwners map[int]documentOwner

func (o *documentOwners) record(position int, owner *documentOwner) {
	if owner.child == (manifest.NamedResource{}) {
		return
	}
	if *o == nil {
		*o = make(documentOwners)
	}
	(*o)[position] = *owner
}

type batchOwners map[manifest.NamedResource]documentOwners

func (o *batchOwners) set(parent manifest.NamedResource, owners documentOwners) {
	if len(owners) == 0 {
		return
	}
	if *o == nil {
		*o = make(batchOwners)
	}
	(*o)[parent] = owners
}

func (o *batchOwners) record(parent, emitter manifest.NamedResource, position int, document *DocumentFact) {
	if !document.GeneratedOverlap {
		return
	}
	owners := (*o)[parent]
	owners.record(position, &documentOwner{child: document.Child, emitter: emitter})
	o.set(parent, owners)
}

type selectedOccurrence struct {
	batch, position int
	emitter         manifest.NamedResource
}

func collapseOverlaps(
	batches []manifest.DocumentBatch,
	owners batchOwners,
) []manifest.DocumentBatch {
	if len(owners) == 0 {
		return batches
	}
	selected := make(map[manifest.NamedResource]selectedOccurrence)
	for i, batch := range batches {
		positions := owners[batch.Parent.Named()]
		for j := range batch.Documents {
			owner, shared := positions[j]
			if !shared {
				continue
			}
			winner, found := selected[owner.child]
			if !found || store.PreferProducer(owner.emitter, winner.emitter) {
				selected[owner.child] = selectedOccurrence{batch: i, position: j, emitter: owner.emitter}
			}
		}
	}
	for i := range batches {
		batch := &batches[i]
		if positions := owners[batch.Parent.Named()]; len(positions) != 0 {
			dropOverlappingDocuments(batch, i, positions, selected)
		}
	}
	return slices.DeleteFunc(batches, func(batch manifest.DocumentBatch) bool { return len(batch.Documents) == 0 })
}

func dropOverlappingDocuments(
	batch *manifest.DocumentBatch,
	batchIndex int,
	owners documentOwners,
	selected map[manifest.NamedResource]selectedOccurrence,
) {
	var documents []map[string]any
	var identities []manifest.NamedResource
	filtered := false
	for i, doc := range batch.Documents {
		if owner, shared := owners[i]; shared {
			winner := selected[owner.child]
			if winner.batch != batchIndex || winner.position != i {
				if !filtered {
					documents = slices.Clone(batch.Documents[:i])
					identities = slices.Clone(batch.Identities[:i])
					filtered = true
				}
				continue
			}
		}
		if filtered {
			documents = append(documents, doc)
			identities = append(identities, batch.Identities[i])
		}
	}
	if filtered {
		batch.Documents, batch.Identities = documents, identities
	}
}
