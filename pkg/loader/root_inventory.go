package loader

import (
	"path/filepath"

	"github.com/home-operations/flate/pkg/manifest"
)

type rootCoordinate struct {
	declaration OccurrenceID
	identity    manifest.NamedResource
	context     string
}

type rootInventory struct {
	occurrences map[rootCoordinate]rootEvidence
	identities  map[manifest.NamedResource]rootCoordinate
	packages    map[string]rootCoordinate
}

func (r *rootInventory) lookup(id manifest.NamedResource) (rootEvidence, bool) {
	coordinate, known := r.identities[id]
	if !known {
		return rootEvidence{}, false
	}
	evidence, known := r.occurrences[coordinate]
	return evidence, known
}

func (r *rootInventory) get(id manifest.NamedResource) rootEvidence {
	evidence, _ := r.lookup(id)
	return evidence
}

func (r *rootInventory) remember(occurrence *Occurrence, context string) rootCoordinate {
	if r.occurrences == nil {
		r.occurrences = map[rootCoordinate]rootEvidence{}
		r.identities = map[manifest.NamedResource]rootCoordinate{}
	}
	coordinate := rootCoordinate{declaration: occurrence.Declaration, identity: occurrence.Identity, context: context}
	evidence := r.occurrences[coordinate]
	evidence.occurrence = *occurrence
	r.occurrences[coordinate] = evidence
	return coordinate
}

func (r *rootInventory) set(id manifest.NamedResource, evidence *rootEvidence) {
	occurrence := evidence.occurrence
	occurrence.Identity = id
	coordinate := r.remember(&occurrence, filepath.ToSlash(filepath.Dir(occurrence.Declaration.File)))
	r.occurrences[coordinate] = *evidence
	r.identities[id] = coordinate
}

func (r *rootInventory) retainIndependent() {
	for coordinate := range r.occurrences {
		evidence := r.occurrences[coordinate]
		if !evidence.independent {
			delete(r.occurrences, coordinate)
			if r.identities[coordinate.identity] == coordinate {
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
			if r.identities[coordinate.identity] == coordinate {
				delete(r.identities, coordinate.identity)
			}
		}
	}
	clear(r.packages)
}
