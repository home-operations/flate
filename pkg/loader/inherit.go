package loader

import (
	"context"
	"maps"
	"path/filepath"
	"slices"
	"strings"

	"github.com/home-operations/flate/internal/claimpath"
	"github.com/home-operations/flate/internal/kustomizeinput"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/selection"
	"github.com/home-operations/flate/pkg/store"
)

// Admit MUST finish construction before its caller publishes admission.
func (l *Loader) Admit(ctx context.Context, repoRoot string) error {
	if err := ctx.Err(); err != nil {
		return err //nolint:wrapcheck // Cancellation preserves the caller context error identity.
	}
	l.mountSources()
	return l.admit(ctx, repoRoot, nil)
}

//nolint:gocognit // Admission MUST preserve round and publication order while returning cancellation.
func (l *Loader) admit(ctx context.Context, repoRoot string, prediction map[string]nsResolution) error {
	if err := ctx.Err(); err != nil {
		return err //nolint:wrapcheck // Cancellation preserves the caller context error identity.
	}
	l.projectTemplates()
	l.ensureScopes()
	kss := l.kustomizations()
	if l.scopes.complete && l.scopesMatch(kss, repoRoot) {
		return ctx.Err() //nolint:wrapcheck // Cancellation preserves the caller context error identity.
	}

	if prediction == nil {
		prediction = map[string]nsResolution{}
	}
	if l.documents.compact {
		l.construction++
	}
	if repoRoot == "" {
		l.refreshClaims(repoRoot, kss)
		if err := ctx.Err(); err != nil {
			return err //nolint:wrapcheck // Cancellation preserves the caller context error identity.
		}
		l.scopes.complete = true
		return nil
	}
	for {
		if err := ctx.Err(); err != nil {
			return err //nolint:wrapcheck // Cancellation preserves the caller context error identity.
		}
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
		for dir, err := range selection.ReduceStatic(ctx, staticScope{loader: l}) {
			if err != nil {
				break
			}
			l.documents.read(dir)
		}
		// ReduceStatic yields only context errors, which MUST persist after iteration.
		if err := ctx.Err(); err != nil {
			return err //nolint:wrapcheck // Cancellation preserves the caller context error identity.
		}
		changed := l.projectAuthored(repoRoot, prediction)
		if err := ctx.Err(); err != nil {
			return err //nolint:wrapcheck // Cancellation preserves the caller context error identity.
		}
		if !changed {
			l.scopes.complete = true
			return nil
		}
		kss = l.kustomizations()
		if l.scopesMatch(kss, repoRoot) {
			if err := ctx.Err(); err != nil {
				return err //nolint:wrapcheck // Cancellation preserves the caller context error identity.
			}
			l.scopes.complete = true
			return nil
		}
	}
}

func (l *Loader) anchorRoot(repoRoot, dir string) bool {
	if l.entry == entryKustomize && filepath.Join(repoRoot, dir) == l.entryPath {
		return true
	}
	var visits []graphVisit
	owned := false
	coordinate := dir + "/"
	for _, claim := range l.scopes.claims {
		if !strings.HasPrefix(coordinate, claim.Prefix) {
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

func (l *Loader) projectAuthored(repoRoot string, prediction map[string]nsResolution) bool {
	if prediction == nil {
		prediction = map[string]nsResolution{}
	}
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
	authored2 := l.desiredAuthored(raw, repoRoot, prediction)
	for id := range l.candidates {
		obj := l.candidates[id]
		if r, ok := l.Index.Lookup(id); ok && r.Path == "" && r.Authored == (manifest.NamedResource{}) {
			authored2.objects[id], authored2.records[id] = obj, r
		}
	}
	allowed := &l.roots
	allowed.retainIndependent()
	l.allowAuthoredObjects(authored2.records, authored2.objects, allowed, scopes)
	l.allowAuthoredRecords(authored2.records, authored2.objects, allowed, scopes)
	l.scopes.candidates, l.candidates = l.candidates, authored2.objects
	return l.applyAuthored(authored2.records, authored2.objects, allowed)
}

type authoredState struct {
	records map[manifest.NamedResource]manifest.DiscoveryRecord
	objects map[manifest.NamedResource]store.Child
}

func (l *Loader) desiredAuthored(
	raw []authoredRecord,
	repoRoot string,
	prediction map[string]nsResolution,
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
	{
		indexedValues := raw
		for indexedIndex := range indexedValues {
			indexed := indexedValues[indexedIndex]
			r := l.Index.Records[indexed.id]
			if r.Path == "" {
				r = manifest.DiscoveryRecord{Authored: indexed.authored, Path: indexed.path, File: indexed.file}
			}
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
			if (l.documents.compact || l.contextualDeclaration(r.Path, r.Authored)) &&
				l.projectAuthoredOccurrence(&state, &r, &authored, forced) {
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
		if record.Builder == "" || l.registeredRoot(id, &record) == "" {
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
	publish := func(projected authoredOccurrence, production manifest.Production) {
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
		l.publishAuthoredObject(state.objects, projected.Identity, authored, record)
	}
	projected := l.projectAuthoredVisits(record, &structured, input.version, publish)
	if !projected {
		for _, namespace := range l.recordNamespaces(record, forced) {
			publish(l.inAuthoredNamespace(&structured, namespace), manifest.Production{})
		}
	}
	return true
}

func (l *Loader) projectAuthoredVisits(
	record *manifest.DiscoveryRecord,
	structured *authoredOccurrence,
	version string,
	publish func(authoredOccurrence, manifest.Production),
) bool {
	projected, owned := false, false
	l.fileOwners(record.Path, record.File, func(manifest.NamedResource) bool {
		owned = true
		return false
	})
	for _, ref := range l.scopes.visits[record.Path] {
		contribution := &l.scopes.contributions[ref.contribution]
		if !contribution.active || contribution.membershipOnly {
			continue
		}
		visit := &contribution.visits[ref.visit]
		if visit.role != kustomizeinput.Resource || visit.file != record.Path && visit.origin != record.Path {
			continue
		}
		projectedVisit := *visit
		projectedVisit.owner, projectedVisit.source = contribution.owner, contribution.source
		occurrence, err := l.projectOccurrence(structured, &projectedVisit, version)
		identity := occurrence.Identity
		if err != nil {
			// Production finalization reports invalid upstream identity projections.
			return true
		}
		if occurrence.facts.uncertain {
			projected = true
			continue
		}
		occurrence.requiresBuild = occurrence.requiresBuild || contribution.applies && visit.requiresBuild
		production := manifest.Production{
			Owner: contribution.owner, Source: contribution.source,
			Authored: record.Authored, Path: record.Path, File: record.File,
		}
		projectOccurrenceProduction(&occurrence, &production)
		if production.Owner == (manifest.NamedResource{}) &&
			!owned &&
			(occurrence.requiresBuild || identity.Name != occurrence.Authored.Name) {
			production.Owner = manifest.NewEntryKustomization(production.Source, contribution.context.dir, manifest.NamedResource{}).
				Named()
		}
		publish(occurrence, production)
		projected = true
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
				if child.fact != nil && child.fact.Authored == id && child.fact.object != nil {
					return store.Child{Obj: child.fact.object, Doc: child.Doc, Warnings: child.Warnings}
				}
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
	allowed *rootInventory,
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
	for v := range l.scopes.fileVisits(file) {
		if v.role == kustomizeinput.Resource && v.readable {
			return false
		}
		terminal = terminal || v.role != kustomizeinput.Resource
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
		if object.Obj != nil && object.outcome.Err == nil && object.Obj.Named() == id {
			projection, _ := manifest.ProjectProducer(object.Obj, nil)
			fact := compactOccurrence(object.Obj, projection)
			fact.facts.substituteDisabled = object.substituteDisabled
			origin := new(l.documents).localPath(file)
			root := new(l.documents).mountedRoot(origin)
			relative, _ := filepath.Rel(root, origin)
			fact.Declaration = occurrenceID{
				Source:   root,
				File:     filepath.ToSlash(relative),
				Document: object.outcome.Ordinal,
				item:     object.item,
			}
			object.fact = &fact
			return object, true
		}
	}
	return parsedObject{}, false
}
