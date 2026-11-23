package selection

import (
	"slices"

	"github.com/home-operations/flate/internal/engine/store"
	"github.com/home-operations/flate/pkg/manifest"
)

type documentOwner struct {
	child       manifest.NamedResource
	declaration *manifest.NamedResource
	producer    *manifest.DiagnosticIdentity
	peerOnly    bool
}

type documentOwners map[int]documentOwner

func (o *documentOwners) record(position int, owner *documentOwner) {
	if owner.child == (manifest.NamedResource{}) && owner.declaration == nil &&
		owner.producer == nil && !owner.peerOnly {
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

func (o *batchOwners) record(parent manifest.NamedResource, position int, owner *documentOwner) {
	if owner.child == (manifest.NamedResource{}) && owner.declaration == nil &&
		owner.producer == nil && !owner.peerOnly {
		return
	}
	owners := (*o)[parent]
	owners.record(position, owner)
	o.set(parent, owners)
}

func (o *batchOwners) producers(batches []manifest.DocumentBatch) map[int]map[int]manifest.DiagnosticIdentity {
	if len(*o) == 0 {
		return nil
	}
	var producers map[int]map[int]manifest.DiagnosticIdentity
	for i, batch := range batches {
		for position, owner := range (*o)[batch.Parent.Named()] {
			if owner.producer == nil {
				continue
			}
			if producers == nil {
				producers = make(map[int]map[int]manifest.DiagnosticIdentity)
			}
			if producers[i] == nil {
				producers[i] = make(map[int]manifest.DiagnosticIdentity)
			}
			producers[i][position] = *owner.producer
		}
	}
	return producers
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
		parent := batch.Parent.Named()
		positions := owners[parent]
		if len(positions) == 0 {
			continue
		}
		for j := range batch.Documents {
			owner := positions[j]
			if owner.child == (manifest.NamedResource{}) {
				continue
			}
			emitter := parent
			if owner.producer != nil {
				emitter = owner.producer.ID
			}
			winner, found := selected[owner.child]
			if !found || store.PreferProducer(emitter, winner.emitter) {
				selected[owner.child] = selectedOccurrence{batch: i, position: j, emitter: emitter}
			}
		}
	}
	if len(selected) == 0 {
		return batches
	}
	for i := range batches {
		batch := &batches[i]
		if positions := owners[batch.Parent.Named()]; len(positions) != 0 {
			owners[batch.Parent.Named()] = dropOverlappingDocuments(batch, i, positions, selected)
		}
	}
	return slices.DeleteFunc(batches, func(batch manifest.DocumentBatch) bool { return len(batch.Documents) == 0 })
}

func dropOverlappingDocuments(
	batch *manifest.DocumentBatch,
	batchIndex int,
	owners documentOwners,
	selected map[manifest.NamedResource]selectedOccurrence,
) documentOwners {
	var documents []map[string]any
	var identities []manifest.NamedResource
	var kept documentOwners
	filtered := false
	for i, doc := range batch.Documents {
		owner := owners[i]
		if owner.child != (manifest.NamedResource{}) {
			winner := selected[owner.child]
			if winner.batch != batchIndex || winner.position != i {
				if !filtered {
					documents = slices.Clone(batch.Documents[:i])
					identities = slices.Clone(batch.Identities[:i])
					for position := range i {
						previous := owners[position]
						kept.record(position, &previous)
					}
					filtered = true
				}
				continue
			}
		}
		if filtered {
			kept.record(len(documents), &owner)
			documents = append(documents, doc)
			identities = append(identities, batch.Identities[i])
		}
	}
	if filtered {
		batch.Documents, batch.Identities = documents, identities
		return kept
	}
	return owners
}
