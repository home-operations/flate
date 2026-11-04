package loader

import (
	"maps"
	"slices"

	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

func (l *Loader) applyAdmittedObjects(
	next map[manifest.NamedResource]manifest.DiscoveryRecord,
	objects map[manifest.NamedResource]store.Child,
	allowed map[manifest.NamedResource]bool,
) bool {
	changed := false
	for _, id := range slices.SortedFunc(maps.Keys(l.admitted), manifest.NamedResource.Compare) {
		if _, survives := next[id]; !survives || !allowed[id] {
			if l.Store.GetObject(id) == l.admitted[id] && l.Store.GetArtifact(id, store.ArtifactContent) == nil {
				l.Store.DeleteObject(id)
				changed = true
			}
			delete(l.admitted, id)
		}
	}
	if l.admitted == nil {
		l.admitted = map[manifest.NamedResource]manifest.BaseManifest{}
	}
	for _, id := range slices.SortedFunc(maps.Keys(objects), manifest.NamedResource.Compare) {
		child := objects[id]
		obj := child.Obj
		if !allowed[id] || !manifest.KindOf(obj).Discovery || l.documents.compact && next[id].Path != "" {
			continue
		}
		if existing, policy := l.Store.Lookup(
			id,
		); existing != nil && policy.Root &&
			obj.ProjectionCompatible(existing) {
			continue
		}
		l.Store.AddObject(obj, child.Doc)
		l.admitted[id] = obj
		changed = true
	}
	return changed
}

func (l *Loader) allowAuthoredObjects(
	next map[manifest.NamedResource]manifest.DiscoveryRecord,
	objects map[manifest.NamedResource]store.Child,
	allowed map[manifest.NamedResource]bool,
	scopes map[string]rootAdmission,
) {
	for id, child := range objects {
		if !manifest.AdmitsRoot(child.Obj) {
			allowed[id] = false
			continue
		}
		r := next[id]
		if r.Path == "" && r.Authored == (manifest.NamedResource{}) {
			allowed[id] = r.Parent == (manifest.NamedResource{})
			continue
		}
		doc := l.documents.cache[r.Path]
		allowed[id] = doc.eligible != fileInactive &&
			(l.entry != entryKustomize && doc.loaded && !doc.bootstrap || scopes[r.Path].admits(l, id, new(r)))
	}
}

func (l *Loader) allowAuthoredRecords(
	next map[manifest.NamedResource]manifest.DiscoveryRecord,
	allowed map[manifest.NamedResource]bool,
	scopes map[string]rootAdmission,
) {
	{
		//nolint:gocritic // Map iteration supplies value snapshots without an additional lookup.
		for id, record := range next {
			if _, known := allowed[id]; known {
				continue
			}
			if obj := l.CatalogObject(id); obj == nil || !manifest.AdmitsRoot(obj) {
				allowed[id] = false
				continue
			}
			doc := l.documents.cache[record.Path]
			allowed[id] = doc.eligible != fileInactive &&
				(id.KindInfo().Discovery && l.entry != entryKustomize && doc.loaded && !doc.bootstrap || scopes[record.Path].admits(l, id, new(record)))
		}
	}
}

func (l *Loader) prepareAuthoredFile(
	file string,
	coordinate string,
	group []authoredRecord,
	scopes map[string]rootAdmission,
) bool {
	if l.terminalInput(file) {
		doc := l.documents.cache[file]
		doc.objects, doc.lookup, doc.loaded = nil, nil, false
		l.documents.cache[file] = doc
		return false
	}
	doc := l.documents.cache[file]
	if l.documents.compact || doc.bootstrap || l.entry == entryKustomize || !doc.loaded {
		structural := slices.ContainsFunc(group, func(r authoredRecord) bool {
			return r.id.KindInfo().Structural
		})
		scopes[file] = l.rootAdmission(file, coordinate, structural)
	}
	return true
}
