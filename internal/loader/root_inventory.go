package loader

import (
	"path/filepath"

	"github.com/home-operations/flate/internal/selection"
	"github.com/home-operations/flate/pkg/manifest"
)

type rootCoordinate struct {
	declaration occurrenceID
	identity    manifest.NamedResource
	context     string
}

type rootInventory struct {
	occurrences map[rootCoordinate]*rootEvidence
	identities  map[manifest.NamedResource]*rootEvidence
	currentOnly manifest.DiscoveryIndex
}

func (r *rootInventory) lookup(id manifest.NamedResource) (*rootEvidence, bool) {
	evidence, known := r.identities[id]
	return evidence, known
}

func (r *rootInventory) get(id manifest.NamedResource) *rootEvidence {
	return r.identities[id]
}

func (r *rootInventory) set(id manifest.NamedResource, evidence *rootEvidence) {
	occurrence := evidence.occurrence
	if evidence.current != nil && evidence.current.Identity != id {
		evidence.current = new(*evidence.current)
	}
	evidence.identity, evidence.currentOccurrence().Identity = id, id
	if r.occurrences == nil {
		r.occurrences = map[rootCoordinate]*rootEvidence{}
		r.identities = map[manifest.NamedResource]*rootEvidence{}
	}
	coordinate := rootCoordinate{
		declaration: occurrence.Declaration,
		identity:    id,
		context:     filepath.ToSlash(filepath.Dir(occurrence.Declaration.File)),
	}
	snapshot := new(*evidence)
	r.replace(&coordinate, snapshot)
	r.identities[id] = snapshot
}

func (r *rootInventory) replace(coordinate *rootCoordinate, evidence *rootEvidence) {
	if previous := r.occurrences[*coordinate]; previous != nil && r.identities[coordinate.identity] == previous {
		r.identities[coordinate.identity] = evidence
	}
	r.occurrences[*coordinate] = evidence
}

func (r *rootInventory) retainIndependent() {
	for coordinate := range r.occurrences {
		evidence := r.occurrences[coordinate]
		if !selection.Root(evidence.view()).Independent {
			delete(r.occurrences, coordinate)
			if r.identities[coordinate.identity] == evidence {
				delete(r.identities, coordinate.identity)
			}
		}
	}
}

func (r *rootInventory) removeSource(source string) {
	for coordinate := range r.occurrences {
		evidence := r.occurrences[coordinate]
		if coordinate.declaration.Source == source || evidence.packageRoot {
			delete(r.occurrences, coordinate)
			if r.identities[coordinate.identity] == evidence {
				delete(r.identities, coordinate.identity)
			}
		}
	}
}
