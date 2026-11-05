package loader

import (
	"maps"
	"path/filepath"
	"slices"
	"strings"

	"github.com/home-operations/flate/internal/claimpath"
	"github.com/home-operations/flate/internal/kustomizeinput"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

// Admit resolves scope evidence and publishes independently runnable discovery roots.
func (l *Loader) Admit(repoRoot string) {
	l.mountSources()
	l.admit(repoRoot)
}

func (l *Loader) admit(repoRoot string) {
	l.projectTemplates()
	l.ensureScopes()
	kss := l.kustomizations()
	if l.scopes.complete && l.scopesMatch(kss, repoRoot) {
		return
	}

	if l.documents.compact {
		l.construction++
	}
	if repoRoot == "" {
		l.refreshClaims(repoRoot, kss)
		l.scopes.complete = true
		return
	}
	for {
		l.refreshClaims(repoRoot, kss)
		for file, visits := range l.scopes.visits {
			clear(visits)
			l.scopes.visits[file] = visits[:0]
		}
		l.scopes.contributions = l.scopes.contributions[:0]
		for i := range kss {
			child := &kss[i]
			//nolint:errcheck // Kustomization-only inputs must panic on an invalid object type.
			ks := child.Obj.(*manifest.Kustomization)
			l.visitKustomizationScope(repoRoot, ks, i)
		}
		l.visitRootScopes(repoRoot, kss)
		l.resolveEligibility()
		if !l.projectAuthored(repoRoot) {
			l.scopes.complete = true
			return
		}
		kss = l.kustomizations()
		if l.scopesMatch(kss, repoRoot) {
			l.scopes.complete = true
			return
		}
	}
}

func (l *Loader) anchorRoot(repoRoot, dir string) bool {
	if l.entry == entryKustomize && filepath.Join(repoRoot, dir) == l.entryPath {
		return true
	}
	var visits []graphVisit
	owned := false
	for _, claim := range l.scopes.claims {
		if !strings.HasPrefix(dir+"/", claim.Prefix) {
			continue
		}
		if !owned {
			visits = l.scopeWalk(new(graphVisit{root: repoRoot, dir: dir, role: kustomizeinput.Resource}))
		}
		owner, _ := l.Index.Lookup(claim.ID)
		origin := new(l.documents).localPath(owner.Path)
		if owner.Path == "" || !slices.ContainsFunc(visits, func(visit graphVisit) bool {
			return visit.role == kustomizeinput.Resource &&
				(visit.file == owner.Path || visit.file == origin || visit.origin != "" && visit.origin == origin)
		}) {
			return false
		}
		owned = true
	}
	return owned
}

func (l *Loader) projectAuthored(repoRoot string) bool {
	indexed := l.indexAuthoredRecords()
	scopes := make(map[string]rootAdmission)
	raw := indexed[:0]
	for start := 0; start < len(indexed); {
		first := indexed[start]
		end := authoredGroupEnd(indexed, start, first.path)
		file := first.path
		if !l.prepareAuthoredFile(file, first.file, indexed[start:end], scopes) {
			start = end
			continue
		}
		{
			rValues := indexed[start:end]
			for rIndex := range rValues {
				r := rValues[rIndex]
				switch {
				case len(raw) == 0 || raw[len(raw)-1].path != file || raw[len(raw)-1].authored != r.authored:
					raw = append(raw, r)
				case l.Index.Records[raw[len(raw)-1].id].Builder != "" && l.Index.Records[r.id].Builder == "":
					raw[len(raw)-1] = r
				}
			}
		}
		start = end
	}
	authored2 := l.desiredAuthored(raw, repoRoot)
	for id := range l.candidates {
		obj := l.candidates[id]
		if r, ok := l.Index.Lookup(id); ok && r.Path == "" && r.Authored == (manifest.NamedResource{}) {
			authored2.objects[id], authored2.records[id] = obj, r
		}
	}
	allowed := l.roots
	for id := range allowed {
		if !allowed[id].independent {
			delete(allowed, id)
		}
	}
	if allowed == nil && (len(authored2.objects) != 0 || len(authored2.records) != 0) {
		allowed = make(map[manifest.NamedResource]rootEvidence, len(authored2.objects))
	}
	l.allowAuthoredObjects(authored2.records, authored2.objects, allowed, scopes)
	l.allowAuthoredRecords(authored2.records, authored2.objects, allowed, scopes)
	l.scopes.candidates, l.candidates = l.candidates, authored2.objects
	l.roots = allowed
	return l.applyAuthored(authored2.records, authored2.objects, allowed)
}

type authoredState struct {
	records map[manifest.NamedResource]manifest.DiscoveryRecord
	objects map[manifest.NamedResource]store.Child
}

func (l *Loader) desiredAuthored(
	raw []authoredRecord,
	repoRoot string,
) authoredState {
	next := l.scopes.records
	if next == nil {
		next = map[manifest.NamedResource]manifest.DiscoveryRecord{}
		l.scopes.records = next
	}
	clear(next)
	objects := l.scopes.candidates
	if objects == nil {
		objects = map[manifest.NamedResource]store.Child{}
	}
	clear(objects)
	state := authoredState{records: next, objects: objects}
	l.retainBuiltDeclarations(&state)
	prediction := map[string]nsResolution{}
	{
		indexedValues := raw
		for indexedIndex := range indexedValues {
			indexed := indexedValues[indexedIndex]
			r := l.Index.Records[indexed.id]
			if next[indexed.id].Builder != "" {
				continue
			}
			path, id := r.Path, r.Authored
			authored := l.authoredObject(path, id)
			forced := ""
			if ks, ok := authored.Obj.(*manifest.Kustomization); ok {
				prediction2 := l.predictNamespaces(ks, repoRoot, new(r), prediction)
				authored.Obj, forced = prediction2.object, prediction2.forced
			}
			if l.documents.compact && l.projectAuthoredOccurrence(&state, &r, &authored, forced) {
				continue
			}
			if authored.Obj == nil {
				continue
			}
			obj := authored.Obj
			for _, ns := range l.recordNamespaces(new(r), forced) {
				id := obj.Named()
				id.Namespace = ns
				projected := obj
				previous := l.projectAuthoredManifest(&projected, obj, id, ns)
				record := l.authoredManifestRecord(new(r), id, previous.Obj, projected)
				next[id] = record
				if manifest.KindOf(projected).Discovery {
					objects[id] = store.Child{Obj: projected, Doc: authored.Doc, Warnings: authored.Warnings}
				}
			}
		}
	}
	return state
}

func (l *Loader) retainBuiltDeclarations(state *authoredState) {
	// Built declarations keep their effective identity until their supplying batch changes.
	for id := range l.Index.Records {
		record := l.Index.Records[id]
		if record.Builder == "" || l.registeredRoot(&record) == "" {
			continue
		}
		state.records[id] = record
		if previous := l.candidates[id]; previous.Obj != nil {
			state.objects[id] = previous
		}
	}
}

func (l *Loader) projectAuthoredOccurrence(
	state *authoredState,
	record *manifest.DiscoveryRecord,
	authored *store.Child,
	forced string,
) bool {
	input, ok := l.authoredOccurrence(record.Path, record.Authored)
	if !ok {
		return false
	}
	structured := withAuthoredStructure(input.fact, authored.Obj)
	publish := func(projected Occurrence, production manifest.Production) {
		if state.records[projected.Identity].Builder != "" {
			return
		}
		next := *record
		if next.Builder == "" {
			next.Productions = state.records[projected.Identity].Productions
			if production != (manifest.Production{}) && !slices.Contains(next.Productions, production) {
				next.Productions = append(next.Productions, production)
			}
			next.Refs = authoredReferences(&projected)
		}
		state.records[projected.Identity] = next
		l.publishAuthoredLocation(&projected)
		if projected.Identity.Name == projected.Authored.Name {
			l.publishAuthoredObject(state.objects, projected.Identity, authored.Obj, record)
		}
	}
	projected := l.projectAuthoredVisits(record, &structured, input.version, publish)
	if !projected {
		for _, namespace := range l.recordNamespaces(record, forced) {
			publish(inAuthoredNamespace(&structured, namespace), manifest.Production{})
		}
	}
	return true
}

func (l *Loader) projectAuthoredVisits(
	record *manifest.DiscoveryRecord,
	structured *Occurrence,
	version string,
	publish func(Occurrence, manifest.Production),
) bool {
	projected := false
	for i := range l.scopes.contributions {
		contribution := &l.scopes.contributions[i]
		if !contribution.active || contribution.membershipOnly {
			continue
		}
		for j := range contribution.visits {
			visit := &contribution.visits[j]
			if visit.role != kustomizeinput.Resource || visit.file != record.Path && visit.origin != record.Path {
				continue
			}
			occurrence := *structured
			identity, err := l.projectIdentity(occurrence.Authored, visit, version, occurrence.facts.namespaced)
			if err != nil {
				// Production finalization reports invalid upstream identity projections.
				return true
			}
			occurrence.Identity = identity
			occurrence.requiresBuild = visit.requiresBuild && contribution.owner == (manifest.NamedResource{})
			production := manifest.Production{
				Owner: contribution.owner, Source: contribution.source,
				Authored: record.Authored, Path: record.Path, File: record.File,
			}
			if production.Owner == (manifest.NamedResource{}) &&
				(occurrence.requiresBuild || identity.Name != occurrence.Authored.Name) {
				production.Owner = manifest.NewEntryKustomization(production.Source, contribution.context.dir, manifest.NamedResource{}).
					Named()
			}
			publish(occurrence, production)
			projected = true
		}
	}
	return projected
}

func (l *Loader) authoredObject(path string, id manifest.NamedResource) store.Child {
	doc := l.documents.cache[path]
	if l.documents.compact {
		{
			childValues := doc.objects
			for childIndex := range childValues {
				child := childValues[childIndex]
				if child.fact != nil && child.fact.Authored == id && child.fact.facts.structural != nil {
					return store.Child{Obj: child.fact.facts.structural}
				}
			}
		}
		return store.Child{}
	}
	if len(doc.objects) > 1 {
		if doc.lookup == nil {
			doc.indexAuthoredObjects()
			l.documents.cache[path] = doc
		}
		return doc.lookup[id]
	}
	if len(doc.objects) == 1 && doc.objects[0].Obj != nil && doc.objects[0].outcome.Err == nil &&
		doc.objects[0].Obj.Named() == id {
		return doc.objects[0].Child
	}
	return store.Child{}
}

func (l *Loader) applyAuthored(
	next map[manifest.NamedResource]manifest.DiscoveryRecord,
	objects map[manifest.NamedResource]store.Child,
	allowed map[manifest.NamedResource]rootEvidence,
) bool {
	var deletes []manifest.NamedResource
	changed := false
	//nolint:gocritic // Discovery iterator values preserve traversal snapshots without another lookup.
	for id, r := range l.Index.Files() {
		if r.Authored == (manifest.NamedResource{}) || r.Path == "" {
			continue
		}
		if _, survives := next[id]; !survives {
			deletes = append(deletes, id)
		}
	}
	slices.SortFunc(deletes, manifest.NamedResource.Compare)
	for _, id := range deletes {
		delete(l.Index.Records, id)
		delete(l.locations, id)
		l.Store.DeleteObject(id)
		changed = true
	}
	for _, id := range slices.SortedFunc(maps.Keys(next), manifest.NamedResource.Compare) {
		r := next[id]
		if previous, ok := l.Index.Lookup(
			id,
		); !ok || previous.Authored != r.Authored || previous.Path != r.Path || previous.File != r.File ||
			!slices.Equal(previous.Refs, r.Refs) {
			changed = true
		}
		l.Index.Records[id] = r
	}
	return l.applyAdmittedObjects(next, objects, allowed) || changed
}

func (l *Loader) recordNamespaces(rArg *manifest.DiscoveryRecord, forced string) []string {
	r := *rArg

	var owners []manifest.NamedResource
	for _, group := range claimpath.Groups(l.Index.Claims, r.File) {
		for _, id := range group {
			if !l.selfClaim(id, r.Authored) {
				owners = appendUniqueProducer(owners, id)
			}
		}
		if len(owners) != 0 {
			break
		}
	}
	var namespaces inheritedNamespaces
	namespaces.collectVisits(l, r.Path, r.Authored, owners)
	return namespaces.finish(l, r.Authored, forced, owners)
}

func (l *Loader) selfClaim(id, authored manifest.NamedResource) bool {
	owner, _ := l.Index.Lookup(id)
	return id == authored || owner.Authored == authored
}

func (l *Loader) terminalInput(file string) bool {
	if l.scopes == nil {
		return l.documents.cache[file].terminal
	}
	terminal := false
	{
		vValues3 := l.scopes.visits[file]
		for vIndex3 := range vValues3 {
			v := vValues3[vIndex3]
			if v.role == kustomizeinput.Resource && v.readable {
				return false
			}
			terminal = terminal || v.role != kustomizeinput.Resource
		}
	}
	return terminal
}

func (l *Loader) authoredOccurrence(file string, id manifest.NamedResource) (parsedObject, bool) {
	objects := l.documents.cache[file].objects
	for objectIndex := range slices.Backward(objects) {
		object := objects[objectIndex]
		if occurrence := object.fact; occurrence != nil && occurrence.Authored == id {
			return object, true
		}
	}
	return parsedObject{}, false
}
