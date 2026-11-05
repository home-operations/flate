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
	allowed map[manifest.NamedResource]rootEvidence,
) bool {
	changed := false
	for _, id := range slices.SortedFunc(maps.Keys(l.admitted), manifest.NamedResource.Compare) {
		if _, survives := next[id]; !survives || !allowed[id].admitted {
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
		if !allowed[id].admitted || !manifest.KindOf(obj).Discovery || l.documents.compact && next[id].Path != "" {
			continue
		}
		if next[id].Builder != "" && !allowed[id].independent {
			continue
		}
		changed = l.publishAdmittedObject(&child) || changed
	}
	return changed
}

func (l *Loader) publishAdmittedObject(child *store.Child) bool {
	obj := child.Obj
	id := obj.Named()
	for i := range child.Warnings {
		warning := child.Warnings[i]
		warning.Resource = id
		l.Store.AddWarning(warning)
	}
	if existing, policy := l.Store.Lookup(id); existing != nil &&
		(policy.Origin == manifest.SuppliedDeclaration || policy.Root && obj.ProjectionCompatible(existing)) {
		return false
	}
	l.Store.AddObject(obj, child.Doc)
	l.admitted[id] = obj
	return true
}

func (l *Loader) allowAuthoredObjects(
	next map[manifest.NamedResource]manifest.DiscoveryRecord,
	objects map[manifest.NamedResource]store.Child,
	allowed map[manifest.NamedResource]rootEvidence,
	scopes map[string]rootAdmission,
) {
	for id := range objects {
		child := objects[id]
		if !manifest.AdmitsRoot(child.Obj) {
			allowed[id] = rootEvidence{}
			continue
		}
		r := next[id]
		if r.Path == "" && r.Authored == (manifest.NamedResource{}) {
			evidence := allowed[id]
			evidence.admitted = r.Parent == (manifest.NamedResource{})
			allowed[id] = evidence
			continue
		}
		allowed[id] = scopes[r.Path].evidence(l, id, &r)
	}
}

func (l *Loader) allowAuthoredRecords(
	next map[manifest.NamedResource]manifest.DiscoveryRecord,
	objects map[manifest.NamedResource]store.Child,
	allowed map[manifest.NamedResource]rootEvidence,
	scopes map[string]rootAdmission,
) {
	//nolint:gocritic // Map iteration supplies value snapshots without an additional lookup.
	for id, record := range next {
		if _, known := objects[id]; known {
			continue
		}
		if obj := l.catalogObject(id, &record); obj == nil || !manifest.AdmitsRoot(obj) {
			allowed[id] = rootEvidence{}
			continue
		}
		scope, known := scopes[record.Path]
		if !known {
			scope = l.rootAdmission(record.Path, record.File, id.KindInfo().Structural)
		}
		allowed[id] = scope.evidence(l, id, &record)
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
	structural := slices.ContainsFunc(group, func(r authoredRecord) bool {
		return r.id.KindInfo().Structural
	})
	scopes[file] = l.rootAdmission(file, coordinate, structural)
	return true
}
