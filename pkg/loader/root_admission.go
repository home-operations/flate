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
	loader        *Loader
	objects       admissionReader
	covering      []selection.RootCoveringOwner
	coveringKnown bool
}

type rootAdmission struct {
	owners []selection.RootOwnerFacts
	peers  [][]manifest.NamedResource
}

type rootEvidence struct {
	occurrence  authoredOccurrence
	facts       selection.RootEvidence
	packageRoot bool
}

func (a rootAdmission) evidence(l *Loader, id manifest.NamedResource, record *manifest.DiscoveryRecord) rootEvidence {
	if record.Builder != "" && l.registeredRoot(id, record) != "" {
		if previous, known := l.roots.lookup(id); known {
			return previous
		}
	}
	occurrence := l.initialOccurrence(id, record)
	evidence := rootEvidence{occurrence: occurrence, facts: selection.RootEvidence{
		Identity: id, Record: *record, Occurrence: occurrenceView(&occurrence), Scope: l.rootScopeFacts(id, record, &a),
	}}
	if !l.documents.cache[record.Path].initial {
		if previous, known := l.roots.lookup(id); known {
			evidence.facts.InitialScope = &previous.facts.Scope
			evidence.facts.InitialOccurrence = previous.facts.Occurrence
			if selection.Root(previous.facts).Independent {
				evidence.occurrence = previous.occurrence
			}
		}
	}
	return evidence
}

func (l *Loader) initialOccurrence(id manifest.NamedResource, record *manifest.DiscoveryRecord) authoredOccurrence {
	occurrence, _ := l.occurrence(id)
	origin := new(l.documents).localPath(record.Path)
	if located, known := l.catalogOccurrence(id); known &&
		filepath.Join(located.Declaration.Source, filepath.FromSlash(located.Declaration.File)) == origin {
		return occurrence
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
		if original == nil {
			if queried[id] {
				return
			}
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
	for coordinate := range l.roots.occurrences {
		evidence := l.roots.occurrences[coordinate]
		if coordinate.declaration != (occurrenceID{}) {
			add(coordinate.identity, &evidence)
		}
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
	if reads != nil && !reads.Valid() {
		return nil
	}
	return roots
}

func (q *rootQuery) entryRoot(
	id manifest.NamedResource,
	original *rootEvidence,
) (selection.OccurrenceFacts, bool) {
	l, objects := q.loader, q.objects
	evidence := l.rootFacts(id)
	if original != nil {
		evidence.Scope, evidence.Occurrence = original.facts.Scope, original.facts.Occurrence
		evidence.InitialScope, evidence.InitialOccurrence = original.facts.InitialScope, original.facts.InitialOccurrence
	}
	if l.nativeEntry != nil && l.nativeEntry.Named() == id {
		evidence.Occurrence = selection.OccurrenceFacts{Identity: id, Object: l.nativeEntry, Catalog: l.nativeEntry}
		evidence.Scope.NativeEntry, evidence.Scope.NamespaceEligible = true, true
	}
	captureRootSupport(&evidence, objects, l.Index, &l.roots.currentOnly, l.SourceRoot)
	evidence.Needed = l.Options.Session.Snapshot().Reconcile(id)
	if selection.Root(evidence).NeedsOwnerSupport {
		if evidence.Record.Path != "" {
			evidence.CoveringOwners = q.coveringFacts()
		}
		captureRootOwnerFacts(&evidence, objects, l.Index)
	}
	result := selection.Root(evidence)
	if !result.Entry {
		return selection.OccurrenceFacts{}, false
	}
	occurrence := result.Occurrence
	object := evidence.Declaration
	if object == nil {
		object = occurrence.Object
	}
	if object == nil {
		object = occurrence.Catalog
	}
	if !l.entryRootAvailable(id, object, objects) {
		return selection.OccurrenceFacts{}, false
	}
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
	_, producer := object.(interface {
		EmissionPolicy() manifest.EmissionPolicy
	})
	if producer || application {
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
			Authored: record.Authored,
			Path:     record.Path,
			Indexed:  indexed,
			Initial:  l.documents.cache[record.Path].initial,
		})
		return true
	})
	if structural {
		for _, group := range l.Index.Claims {
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
	evidence := q.loader.rootFacts(obj.Named())
	if q.objects == nil && !q.loader.frozen {
		return selection.Root(evidence).Admitted
	}
	rootSupport(&evidence, objects, q.loader.Index, &q.loader.roots.currentOnly, q.loader.SourceRoot)
	evidence.Supplied = false
	if selection.Root(evidence).NeedsOwnerSupport {
		if evidence.Record.Path != "" {
			evidence.CoveringOwners = q.coveringFacts()
		}
		captureRootOwners(&evidence, objects, q.loader.Index)
	}
	return selection.Root(evidence).Admitted
}

func (q *rootQuery) coveringFacts() []selection.RootCoveringOwner {
	if !q.coveringKnown {
		objects := q.objects
		if objects == nil {
			objects = q.loader.Store
		}
		q.covering = rootCoveringFacts(objects, q.loader.Index, q.loader.SourceRoot)
		q.coveringKnown = true
	}
	return q.covering
}

func (l *Loader) classifyPackages(packages []PackageEvidence) {
	owners := l.packageOwners(packages)
	paths := make([]string, len(packages))
	eligible := make([]bool, len(packages))
	runnable := make([]bool, len(packages))
	l.roots.packages = make(map[string]rootCoordinate, len(packages))
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
			l.roots.remember(new(physicalOccurrence(occurrence)), p.Path)
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
		coordinate := l.roots.remember(&authoredOccurrence{Identity: id}, p.Path)
		l.roots.occurrences[coordinate] = rootEvidence{
			packageRoot: true,
			facts:       selection.RootEvidence{Identity: id, Scope: scope},
		}
		l.roots.packages[p.Path] = coordinate
		for index := range p.Occurrences {
			occurrence := &p.Occurrences[index]
			coordinate := l.roots.remember(new(physicalOccurrence(occurrence)), p.Path)
			file := filepath.Join(occurrence.Declaration.Source, filepath.FromSlash(occurrence.Declaration.File))
			record := manifest.DiscoveryRecord{Authored: occurrence.Authored, Path: file, File: l.fileCoordinate(file)}
			evidence := l.rootAdmission(file, record.File, occurrence.Identity.KindInfo().Structural).
				evidence(l, occurrence.Identity, &record)
			evidence.occurrence, evidence.facts.Occurrence = physicalOccurrence(occurrence), *occurrence
			evidence.facts.Scope.PackageKnown, evidence.facts.Scope.PackagePaths = true, paths
			evidence.facts.Scope.PackageOwners, evidence.facts.Scope.PackageIndex = owners, i
			evidence.facts.Scope.PackageEligible, evidence.facts.Scope.NativePackage = eligible[i], native
			l.roots.occurrences[coordinate] = evidence
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
