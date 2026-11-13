package loader

import (
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

func (l *Loader) publishAuthoredLocation(projected *authoredOccurrence) {
	previous, known := l.locations[projected.Identity]
	// Preparation requirements MUST only grow until the supplying source is replaced.
	projected.requiresBuild = projected.requiresBuild || previous.requiresBuild
	sameCensus := known && previous.facts != nil && previous.facts.census != nil &&
		previous.facts.census == projected.facts.census
	equivalent := sameCensus && sameOccurrenceProjection(&previous, projected) &&
		projected.facts.catalog.ProjectionCompatible(previous.facts.catalog)
	if known && (previous.facts == projected.facts || equivalent) {
		projected.facts, projected.object = previous.facts, previous.object
	}
	if projected.object != nil && projected.object.Named().Namespace != projected.Identity.Namespace {
		projected.object = projected.object.WithNamespace(projected.Identity.Namespace)
	}
	if projected.facts.catalog.Named() != projected.Identity {
		facts := *projected.facts
		if facts.catalog.Named().Name == projected.Identity.Name {
			facts.catalog = facts.catalog.WithNamespace(projected.Identity.Namespace)
		} else {
			facts.catalog = manifest.CatalogObject(facts.catalog, projected.Identity)
		}
		projected.facts = &facts
		if projected.object == nil {
			projected.object = facts.catalog
		}
	}
	if l.locations == nil {
		l.locations = map[manifest.NamedResource]authoredOccurrence{}
	}
	l.locations[projected.Identity] = *projected
}

func (l *Loader) publishAuthoredObject(
	objects map[manifest.NamedResource]store.Child,
	identity manifest.NamedResource,
	authored *store.Child,
	recordArg *manifest.DiscoveryRecord,
) {
	r := *recordArg

	if authored.Obj == nil {
		if previous := l.candidates[identity]; previous.Obj != nil {
			if current, ok := l.Index.Lookup(identity); ok && current.Path == r.Path && current.Authored == r.Authored {
				objects[identity] = previous
			}
		}
		return
	}
	{
		object := authored.Obj
		if object.Named().Name != identity.Name {
			object = manifest.CatalogObject(object, identity)
		} else if object.Named().Namespace != identity.Namespace {
			object = object.WithNamespace(identity.Namespace)
		}
		if previous := l.candidates[identity]; previous.Obj != nil && manifest.Equal(object, previous.Obj) &&
			object.ProjectionCompatible(previous.Obj) {
			object = previous.Obj
		}
		objects[identity] = store.Child{Obj: object, Doc: authored.Doc, Warnings: authored.Warnings}
	}
}

func withAuthoredStructure(occurrenceValue *authoredOccurrence, authored manifest.BaseManifest) authoredOccurrence {
	o := *occurrenceValue

	if structural, ok := authored.(*manifest.Kustomization); ok && structural != o.facts.structural {
		facts := *o.facts
		facts.structural = structural
		facts.catalog = structural
		o.facts = &facts
	}
	return o
}

func (l *Loader) inAuthoredNamespace(occurrenceValue *authoredOccurrence, namespace string) authoredOccurrence {
	o := *occurrenceValue

	projected := o
	if o.facts.namespaced {
		projected.Identity.Namespace = namespace
	}
	if structural := projected.facts.structural; structural != nil &&
		(structural.Named() != projected.Identity || structural.TargetNamespace != projected.facts.policy.Namespace) {
		facts := *projected.facts
		previous := l.locations[projected.Identity]
		facts.projectStructure(projected.Identity, &previous)
		projected.facts = &facts
	}
	return projected
}

func authoredReferences(occurrenceValue *authoredOccurrence) []manifest.NamedResource {
	projected := *occurrenceValue
	refs := []manifest.NamedResource{}
	for ref := range projected.references() {
		if discoveryReference(new(ref)) {
			refs = append(refs, ref.NamedResource)
		}
	}
	return refs
}

//nolint:gocritic // The pointer replaces the caller's projected manifest with the stable candidate and namespace.
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
	if record.Builder != "" {
		return record
	}
	record.Productions = nil
	if old, ok := l.Index.Lookup(id); ok && old.Path == r.Path && previous == projected {
		record.Refs = old.Refs
	} else {
		record.Refs = discoveryReferences(nil, projected)
	}
	return record
}

func projectOccurrenceProduction(occurrence *authoredOccurrence, production *manifest.Production) {
	if len(occurrence.facts.authoredTargets) != 0 {
		production.Declaration = occurrence.Identity
		production.Path = ""
	}
}
