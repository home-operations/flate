package loader

import (
	"cmp"
	"iter"
	"path/filepath"
	"slices"
	"strings"

	"github.com/home-operations/flate/internal/kustomizeinput"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/selection"
	"github.com/home-operations/flate/pkg/store"
)

type rootQuery struct {
	loader   *Loader
	objects  admissionReader
	covering *rootOwnerInventory
}

type rootAdmission struct {
	owners []selection.RootOwnerFacts
	peers  [][]manifest.NamedResource
}

type rootEvidence struct {
	identity          manifest.NamedResource
	occurrence        authoredOccurrence
	current           *authoredOccurrence
	record            selection.RootRecord
	scope             selection.RootScopeFacts
	initialScope      *selection.RootScopeFacts
	initialOccurrence *selection.OccurrenceFacts
	packageRoot       bool
}

func (e *rootEvidence) currentOccurrence() *authoredOccurrence {
	if e.current != nil {
		return e.current
	}
	return &e.occurrence
}

func (e *rootEvidence) view() selection.RootEvidence {
	if e == nil {
		return selection.RootEvidence{}
	}
	return selection.RootEvidence{
		Identity: e.identity, Record: e.record, Occurrence: occurrenceView(e.currentOccurrence()),
		Scope: e.scope, InitialScope: e.initialScope, InitialOccurrence: e.initialOccurrence,
	}
}

func (l *Loader) initialOccurrence(id manifest.NamedResource, record *manifest.DiscoveryRecord) authoredOccurrence {
	occurrence, _ := l.occurrence(id)
	origin := new(l.documents).localPath(record.Path)
	if located, known := l.catalogOccurrence(id); known &&
		filepath.Join(located.Declaration.Source, filepath.FromSlash(located.Declaration.File)) == origin {
		return located
	}
	if occurrence.Declaration != (occurrenceID{}) &&
		filepath.Join(occurrence.Declaration.Source, filepath.FromSlash(occurrence.Declaration.File)) == origin {
		return occurrence
	}
	occurrence = authoredOccurrence{}
	doc := l.documents.cache[record.Path]
	for i := range doc.objects {
		child := &doc.objects[i]
		if child.fact != nil && child.fact.Authored == record.Authored {
			occurrence = *child.fact
			occurrence.Identity = id
			return occurrence
		}
		if child.Obj == nil || child.Obj.Named() != record.Authored {
			continue
		}
		source := l.documents.mountedRoot(record.Path)
		file, _ := filepath.Rel(source, record.Path)
		occurrence = compactOccurrence(child.Obj, manifest.TemplateProjection{})
		occurrence.Declaration = occurrenceID{
			Source: source, File: filepath.ToSlash(file), Document: child.outcome.Ordinal, item: child.item,
		}
		occurrence.Identity, occurrence.Authored, occurrence.object = id, record.Authored, child.Obj
		break
	}
	return occurrence
}

func (l *Loader) nativeOperand(file string) bool {
	if l.scopes == nil {
		return l.documents.cache[file].rootNative
	}
	for _, ref := range l.scopes.visits[file] {
		contribution := &l.scopes.contributions[ref.contribution]
		if contribution.membershipOnly || contribution.owner != (manifest.NamedResource{}) ||
			l.documents.cache[filepath.Join(contribution.context.root, contribution.context.dir)].body == nil {
			continue
		}
		visit := &contribution.visits[ref.visit]
		if visit.role == kustomizeinput.Resource &&
			(visit.file == file || visit.origin != "" && visit.origin == file) {
			return true
		}
	}
	return false
}

// EntryRoots MUST use captured admission evidence without I/O or publication.
func (l *Loader) EntryRoots(
	published iter.Seq[manifest.BaseManifest],
	reads *store.ReadSet,
) []selection.OccurrenceFacts {
	var roots []selection.OccurrenceFacts
	var objects admissionReader = l.Store
	if reads != nil {
		objects = reads
	}
	query := rootQuery{loader: l, objects: objects}
	seen := map[rootCoordinate]bool{}
	queried := map[manifest.NamedResource]bool{}
	add := func(id manifest.NamedResource, original *rootEvidence) {
		if original == nil && queried[id] {
			return
		}
		if original == nil {
			queried[id] = true
		}
		occurrence, available := query.entryRoot(id, original)
		if !available {
			return
		}
		coordinate := rootCoordinate{declaration: physicalOccurrence(&occurrence).Declaration, identity: id}
		if seen[coordinate] {
			return
		}
		seen[coordinate] = true
		roots = append(roots, occurrence)
	}
	for id := range l.roots.identities {
		add(id, nil)
	}
	for coordinate, evidence := range l.roots.occurrences {
		if l.roots.identities[coordinate.identity] == evidence || coordinate.declaration == (occurrenceID{}) {
			continue
		}
		add(coordinate.identity, evidence)
	}
	if published != nil {
		for object := range published {
			add(object.Named(), nil)
		}
	}
	slices.SortFunc(roots, func(a, b selection.OccurrenceFacts) int {
		return cmp.Or(
			a.Identity.Compare(b.Identity),
			strings.Compare(a.Declaration.Source, b.Declaration.Source),
			strings.Compare(
				a.Declaration.File,
				b.Declaration.File,
			),
			cmp.Compare(a.Declaration.Document, b.Declaration.Document),
			cmp.Compare(a.Declaration.Item, b.Declaration.Item),
		)
	})
	if l.nativeEntry != nil {
		add(l.nativeEntry.Named(), nil)
	}
	// The complete query MUST be discarded if any captured dependency changes.
	if !reads.Valid() {
		return nil
	}
	return roots
}

func (q *rootQuery) entryRoot(
	id manifest.NamedResource,
	original *rootEvidence,
) (selection.OccurrenceFacts, bool) {
	l, objects := q.loader, q.objects
	view := l.physicalView()
	evidence := view.rootFacts(id, l, nil)
	if original != nil {
		evidence.Scope, evidence.Occurrence = original.scope, occurrenceView(original.currentOccurrence())
		evidence.InitialScope, evidence.InitialOccurrence = original.initialScope, original.initialOccurrence
	}
	if l.nativeEntry != nil && l.nativeEntry.Named() == id {
		evidence.Occurrence = selection.OccurrenceFacts{Identity: id, Object: l.nativeEntry, Catalog: l.nativeEntry}
	}
	classification := evidence
	classification.Captured = false
	occurrence := selection.Root(classification).Occurrence
	var object manifest.BaseManifest
	if !evidence.Scope.NativeEntry {
		object = objects.GetDeclaration(id)
	}
	if object == nil {
		object = occurrence.Object
	}
	if object == nil {
		object = occurrence.Catalog
	}
	if !l.entryRootAvailable(id, object, objects) {
		return selection.OccurrenceFacts{}, false
	}
	rootSupport(&evidence, objects, l.Index, &l.roots.currentOnly, l.SourceRoot)
	evidence.Needed = l.Options.Session.Snapshot().Reconcile(id)
	if selection.Root(evidence).NeedsOwnerSupport {
		var owners *rootOwnerInventory
		if evidence.Record.Path != "" {
			owners = q.coveringFacts()
		}
		captureRootOwners(&evidence, objects, l.Index, owners)
	}
	result := selection.Root(evidence)
	if !result.Entry {
		return selection.OccurrenceFacts{}, false
	}
	occurrence = result.Occurrence
	occurrence.Identity = id
	if occurrence.Authored == (manifest.NamedResource{}) {
		occurrence.Authored = evidence.Record.Authored
	}
	if occurrence.Object == nil {
		occurrence.Object = object
	}
	return occurrence, true
}

func (l *Loader) entryRootAvailable(
	id manifest.NamedResource,
	object manifest.BaseManifest,
	objects admissionReader,
) bool {
	app, application := manifest.ApplicationFor(object)
	if application {
		location := SourceView(objects, app.Source, l.SourceRoot)
		known := objects.GetObject(app.Source) != nil || objects.GetArtifact(app.Source, store.ArtifactContent) != nil
		if location.Pinned || known && location.External && app.Declaration == nil {
			return false
		}
	}
	if manifest.KindOf(object).Parent || application {
		return true
	}
	output, _ := objects.GetArtifact(id, store.ArtifactContent).(*store.Emission)
	return output != nil
}

func (l *Loader) rootAdmission(file, coordinate string, structural bool) rootAdmission {
	scope := rootAdmission{}
	l.fileOwners(file, coordinate, func(owner manifest.NamedResource) bool {
		if slices.ContainsFunc(
			scope.owners,
			func(existing selection.RootOwnerFacts) bool { return existing.Identity == owner },
		) {
			return true
		}
		record, indexed := l.Index.Lookup(owner)
		scope.owners = append(scope.owners, selection.RootOwnerFacts{
			Identity: owner,
			Path:     record.Path,
			Indexed:  indexed,
			Initial:  l.documents.cache[record.Path].initial,
		})
		return true
	})
	if structural && len(scope.owners) != 0 {
		for _, group := range l.Index.Claims {
			if !slices.ContainsFunc(group, func(peer manifest.NamedResource) bool {
				return slices.ContainsFunc(
					scope.owners,
					func(owner selection.RootOwnerFacts) bool { return owner.Identity == peer },
				)
			}) {
				continue
			}
			if !l.documents.cache[file].bootstrap ||
				slices.ContainsFunc(group, func(owner manifest.NamedResource) bool {
					record, _ := l.Index.Lookup(owner)
					return record.Path == file
				}) {
				scope.peers = append(scope.peers, group)
			}
		}
	}
	return scope
}

func (l *Loader) fileOwners(file, coordinate string, yield func(manifest.NamedResource) bool) {
	if l.documents.cache[file].eligible == selection.RootInactive {
		return
	}
	if origin := new(l.documents).localPath(file); origin != file {
		coordinate = l.fileCoordinate(origin)
	}
	if l.scopes != nil {
		if !l.yieldVisitedOwners(file, yield) {
			return
		}
	}
	if coordinate == "" || l.documents.cache[file].eligible == selection.RootIndependent {
		return
	}
	if len(l.Index.Claims) != 0 {
		if !l.yieldFileClaims(coordinate, yield) {
			return
		}
	}
	for reader := range l.Index.ReadersOfFile(coordinate) {
		if reader.Declaration == (manifest.NamedResource{}) && !reader.Exact && !reader.Materialize &&
			!yield(reader.Reader) {
			return
		}
	}
}

func (q *rootQuery) admitsRoot(obj manifest.BaseManifest, file string) bool {
	if obj == nil {
		return false
	}
	record, indexed := q.loader.Index.Lookup(obj.Named())
	if !indexed || record.Path != file {
		return false
	}

	objects := q.objects
	if objects == nil {
		objects = q.loader.Store
	}
	view := q.loader.physicalView()
	evidence := view.rootFacts(obj.Named(), q.loader, obj)
	if q.objects == nil && !q.loader.frozen {
		return q.loader.admitsDeclaration(&evidence)
	}
	rootSupport(&evidence, objects, q.loader.Index, &q.loader.roots.currentOnly, q.loader.SourceRoot)
	evidence.Supplied = false
	if selection.Root(evidence).NeedsOwnerSupport {
		var owners *rootOwnerInventory
		if evidence.Record.Path != "" {
			owners = q.coveringFacts()
		}
		captureRootOwners(&evidence, objects, q.loader.Index, owners)
	}
	if reads, ok := objects.(*store.ReadSet); ok {
		evidence.Stale = !reads.Valid()
	}
	return q.loader.admitsDeclaration(&evidence)
}

func (q *rootQuery) coveringFacts() *rootOwnerInventory {
	if q.covering == nil {
		objects := q.objects
		if objects == nil {
			objects = q.loader.Store
		}
		q.covering = rootCoveringFacts(objects, q.loader.Index)
	}
	return q.covering
}

func (l *Loader) classifyPackages(packages []PackageEvidence) {
	if l.roots.occurrences == nil {
		l.roots.occurrences = map[rootCoordinate]*rootEvidence{}
		l.roots.identities = map[manifest.NamedResource]*rootEvidence{}
	}
	owners := l.packageOwners(packages)
	paths := make([]string, len(packages))
	eligible := make([]bool, len(packages))
	runnable := make([]bool, len(packages))
	for i := range packages {
		p := &packages[i]
		paths[i] = p.Path
		builder := l.documents.cache[filepath.Join(l.SourceRoot, p.Path)].file
		for index := range p.Occurrences {
			occurrence := &p.Occurrences[index]
			file := filepath.Join(occurrence.Declaration.Source, filepath.FromSlash(occurrence.Declaration.File))
			active := l.documents.cache[file].eligible != selection.RootInactive
			_, _, application := occurrence.SourcePath()
			eligible[i] = eligible[i] || active
			runnable[i] = runnable[i] || active && (occurrence.Identity.KindInfo().Runnable || application)
		}
		if builder != "" {
			eligible[i] = l.documents.cache[builder].eligible != selection.RootInactive
		}
	}
	for i := range packages {
		p := &packages[i]
		native := l.documents.cache[filepath.Join(l.SourceRoot, p.Path)].body != nil
		id := manifest.NewEntryKustomization(manifest.BootstrapSourceID, p.Path, manifest.NamedResource{}).Named()
		scope := selection.RootScopeFacts{
			NamespaceEligible: true,
			PackageKnown:      true,
			PackageRoot:       true,
			PackagePaths:      paths,
			PackageOwners:     owners,
			PackageIndex:      i,
			PackageEligible:   eligible[i],
			PackageRunnable:   runnable[i],
		}
		coordinate := rootCoordinate{identity: id, context: p.Path}
		l.roots.replace(&coordinate, &rootEvidence{
			packageRoot: true,
			identity:    id,
			scope:       scope,
		})
		for index := range p.Occurrences {
			occurrence := &p.Occurrences[index]
			coordinate := rootCoordinate{
				declaration: physicalOccurrence(occurrence).Declaration,
				identity:    occurrence.Identity,
				context:     p.Path,
			}
			file := filepath.Join(occurrence.Declaration.Source, filepath.FromSlash(occurrence.Declaration.File))
			record := manifest.DiscoveryRecord{Authored: occurrence.Authored, Path: file, File: l.fileCoordinate(file)}
			view := l.physicalView()
			evidence := view.rootScope(l, &rootScopeInput{
				identity: occurrence.Identity, record: record,
				admission: l.rootAdmission(file, record.File, occurrence.Identity.KindInfo().Structural),
			})
			evidence.occurrence = physicalOccurrence(occurrence)
			evidence.current = nil
			evidence.scope.PackageKnown, evidence.scope.PackagePaths = true, paths
			evidence.scope.PackageOwners, evidence.scope.PackageIndex = owners, i
			evidence.scope.PackageEligible, evidence.scope.NativePackage = eligible[i], native
			l.roots.replace(&coordinate, &evidence)
		}
		p.Independent = selection.Root(selection.RootEvidence{Identity: id, Scope: scope}).Admitted
	}
}

func (l *Loader) packageOwners(packages []PackageEvidence) [][]int {
	owners := make([][]int, len(packages))
	paths := make([]string, len(packages))
	targets := make(map[string]int, len(packages))
	for i := range packages {
		paths[i] = filepath.Join(l.SourceRoot, filepath.FromSlash(packages[i].Path))
		targets[paths[i]] = i
	}
	claims := map[string][]int{}
	for i := range packages {
		p := &packages[i]
		doc := l.documents.cache[paths[i]]
		if l.documents.cache[doc.file].eligible == selection.RootInactive {
			continue
		}
		if doc.body != nil {
			l.addNativePackageOwners(p.Path, i, targets, owners)
		}
		for _, claim := range l.packageClaims(p.Occurrences) {
			claims[claim] = append(claims[claim], i)
		}
	}
	for target, dir := range paths {
		for {
			owners[target] = append(owners[target], claims[dir]...)
			parent := filepath.Dir(dir)
			if parent == dir {
				break
			}
			dir = parent
		}
		owners[target] = slices.DeleteFunc(owners[target], func(owner int) bool { return owner == target })
		slices.Sort(owners[target])
		owners[target] = slices.Compact(owners[target])
	}
	return owners
}

func (l *Loader) packageClaims(occurrences []selection.OccurrenceFacts) []string {
	var claims []string
	for i := range occurrences {
		source, path, application := occurrences[i].SourcePath()
		file := filepath.Join(occurrences[i].Declaration.Source, filepath.FromSlash(occurrences[i].Declaration.File))
		if !application || l.documents.cache[file].eligible == selection.RootInactive {
			continue
		}
		location := SourceView(l.Store, source, l.SourceRoot)
		declared := l.Index.IsFileIndexed(source) || l.Store.GetObject(source) != nil ||
			l.Store.GetArtifact(source, store.ArtifactContent) != nil
		if !location.Pinned && (!location.External || !declared) {
			claims = append(claims, filepath.Join(location.Path, manifest.NormalizeClaimBase(path)))
		}
	}
	return claims
}

func (l *Loader) addNativePackageOwners(path string, owner int, targets map[string]int, owners [][]int) {
	visits := l.scopeWalk(new(graphVisit{root: l.SourceRoot, dir: path, role: kustomizeinput.Resource}))
	for visitIndex := range visits {
		visit := &visits[visitIndex]
		for _, file := range [2]string{visit.file, filepath.Dir(visit.file)} {
			if target, known := targets[file]; known && target != owner {
				owners[target] = append(owners[target], owner)
			}
		}
	}
}

func (l *Loader) admitsDeclaration(evidence *selection.RootEvidence) bool {
	root := selection.Root(*evidence)
	scope := &evidence.Scope
	nativePrediction := l.documents.compact && l.documents.eager && scope.EntryKustomize && scope.NativeOperand
	nativeApplication := !scope.Application || scope.ApplicationSource == manifest.BootstrapSourceID
	pending := !l.frozen || nativeApplication
	waiting := nativePrediction && pending && !scope.Prepared && !root.Independent
	return root.Admitted && !waiting
}
