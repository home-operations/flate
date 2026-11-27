package loader

import (
	"cmp"
	"maps"
	"slices"

	"github.com/home-operations/flate/internal/engine/selection"
	"github.com/home-operations/flate/internal/engine/store"
	"github.com/home-operations/flate/pkg/manifest"
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
		facts.projectTemplateNamespace(projected.Identity.Namespace)
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

func (o *authoredOccurrence) setIdentity(id manifest.NamedResource) {
	namespaceChanged := o.Identity.Namespace != id.Namespace
	o.Identity = id
	facts := o.facts
	if !namespaceChanged || facts == nil && o.object == nil {
		return
	}
	switch {
	case id.CanDeclareGeneratedEntry():
		view := selection.OccurrenceFacts{Identity: id, Object: o.object}
		if facts != nil {
			view.Catalog = facts.catalog
		}
		source, path, present := view.EffectiveObject().SourcePath()
		resolved := facts != nil && len(facts.projection) != 0
		if resolved && manifest.HasEnvsubstReference(source.Name) {
			source.Name = o.source.Name
		}
		if resolved && manifest.HasEnvsubstReference(path) {
			path, present = o.path, o.hasPath
		}
		if !present {
			source = manifest.NamedResource{}
		}
		o.occurrencePath = occurrencePath{source: source, path: path, hasPath: present}
		o.setApplication(nil, false)
	case o.hasPath && facts != nil && facts.structural != nil:
		projected := *facts.structural
		projected.Name, projected.Namespace = id.Name, id.Namespace
		o.source.Namespace = projected.EffectiveSourceID().Namespace
		o.setApplication(&projected, true)
	case o.hasPath && o.object != nil:
		o.occurrencePath = objectPath(o.object.WithNamespace(id.Namespace))
	}
}

// ProjectedSyncDeclaration preserves authored sync data outside the resolved source and path.
func ProjectedSyncDeclaration(o *selection.OccurrenceFacts, object manifest.BaseManifest) manifest.BaseManifest {
	raw := object.(*manifest.RawObject) //nolint:errcheck // An admitted generated entry is a FluxInstance.
	sync, ok := raw.Spec["sync"].(map[string]any)
	if !ok {
		return raw
	}
	source, path, _ := o.SourcePath()
	authoredName, _ := sync["name"].(string)
	authoredPath, _ := sync["path"].(string)
	if authoredPath == path && cmp.Or(authoredName, raw.Namespace) == source.Name {
		return raw
	}
	projected := *raw
	projected.Spec = maps.Clone(raw.Spec)
	fields := maps.Clone(sync)
	fields["path"], fields["name"] = path, source.Name
	projected.Spec["sync"] = fields
	return &projected
}

func (f *occurrenceFacts) projectTemplateNamespace(namespace string) {
	if namespace == "" {
		return
	}
	var projected []manifest.BaseManifest
	for i, object := range f.templateObjects {
		if object.Named().Namespace == "" && manifest.KindOf(object).Namespaced() {
			if projected == nil {
				projected = slices.Clone(f.templateObjects)
			}
			projected[i] = object.WithNamespace(namespace)
		}
	}
	if projected != nil {
		f.templateObjects = projected
	}
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
		objects[identity] = store.Child{
			Obj:      object,
			Doc:      authored.Doc,
			Warnings: authored.Warnings,
		}
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
		id := projected.Identity
		id.Namespace = namespace
		projected.setIdentity(id)
	}
	if structural := projected.facts.structural; structural != nil &&
		(structural.Named() != projected.Identity || structural.TargetNamespace != projected.facts.policy.Namespace) {
		facts := *projected.facts
		previous := l.locations[projected.Identity]
		projected.facts = &facts
		projected.projectStructure(&previous)
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
