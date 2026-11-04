package loader

import (
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

func (l *Loader) publishAuthoredLocation(projected *Occurrence) {
	if old, known := l.locations[projected.Identity]; known && old.facts == projected.facts {
		projected.object = old.object
	}
	if projected.object == nil && projected.facts.catalog.Named() != projected.Identity {
		if projected.facts.catalog.Named().Name == projected.Identity.Name {
			projected.object = projected.facts.catalog.WithNamespace(projected.Identity.Namespace)
		} else {
			projected.object = manifest.CatalogObject(projected.facts.catalog, projected.Identity)
		}
	}
	l.locations[projected.Identity] = *projected
}

func (l *Loader) publishAuthoredObject(
	objects map[manifest.NamedResource]store.Child,
	identity manifest.NamedResource,
	authored manifest.BaseManifest,
	recordArg *manifest.DiscoveryRecord,
) {
	r := *recordArg

	if authored != nil {
		object := authored
		if previous := l.candidates[identity]; previous.Obj != nil && object.ProjectionCompatible(previous.Obj) {
			object = previous.Obj
		}
		if object.Named().Namespace != identity.Namespace {
			object = object.WithNamespace(identity.Namespace)
		}
		objects[identity] = store.Child{Obj: object}
	} else if previous := l.candidates[identity]; previous.Obj != nil {
		if current, ok := l.Index.Lookup(identity); ok && current.Path == r.Path && current.Authored == r.Authored {
			objects[identity] = previous
		}
	}
}

func withAuthoredStructure(occurrenceValue *Occurrence, authored manifest.BaseManifest) Occurrence {
	o := *occurrenceValue

	if structural, ok := authored.(*manifest.Kustomization); ok && structural != o.facts.structural {
		facts := *o.facts
		facts.structural = structural
		facts.catalog = structural
		o.facts = &facts
	}
	return o
}

func inAuthoredNamespace(occurrenceValue *Occurrence, namespace string) Occurrence {
	o := *occurrenceValue

	projected := o
	if o.facts.namespaced {
		projected.Identity.Namespace = namespace
	}
	return projected
}

func authoredReferences(occurrenceValue *Occurrence) []manifest.NamedResource {
	projected := *occurrenceValue
	refs := []manifest.NamedResource{}
	for ref := range projected.references() {
		if discoveryReference(new(ref)) {
			refs = append(refs, ref.NamedResource)
		}
	}
	return refs
}

func (l *Loader) projectAuthoredManifest(
	projected *manifest.BaseManifest,
	obj manifest.BaseManifest,
	id manifest.NamedResource,
	ns string,
) store.Child {
	// Candidate identities MUST stay stable until their source state changes.
	previous := l.candidates[id]
	if previous.Obj != nil && obj.ProjectionCompatible(previous.Obj) {
		*projected = previous.Obj
	}
	if (*projected).Named().Namespace != ns {
		*projected = (*projected).WithNamespace(ns)
	}
	return previous
}

func (l *Loader) authoredManifestRecord(
	recordArg *manifest.DiscoveryRecord,
	id manifest.NamedResource,
	previous manifest.BaseManifest,
	projected manifest.BaseManifest,
) manifest.DiscoveryRecord {
	r := *recordArg

	record := r
	record.Productions = nil
	if old, ok := l.Index.Lookup(id); ok && old.Path == r.Path && previous == projected {
		record.Refs = old.Refs
	} else {
		record.Refs = discoveryReferences(nil, projected)
	}
	return record
}
