package loader

import (
	"iter"
	"path/filepath"
	"slices"

	"github.com/home-operations/flate/internal/kustomizeinput"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

type fileEligibility uint8

const (
	fileUnknown fileEligibility = iota
	fileInactive
	fileEligible
	fileIndependent
)

type rootAdmission struct {
	denied bool
	owners []manifest.NamedResource
	peers  [][]manifest.NamedResource
}

type rootEvidence struct {
	occurrence  Occurrence
	admitted    bool
	independent bool
}

func (a rootAdmission) evidence(l *Loader, id manifest.NamedResource, record *manifest.DiscoveryRecord) rootEvidence {
	if record.Builder != "" && l.registeredRoot(record) != "" {
		return l.roots[id]
	}
	doc := l.documents.cache[record.Path]
	admitted := doc.eligible != fileInactive && a.admits(l, id, record)
	if doc.eligible != fileInactive && id.KindInfo().Discovery && l.entry != entryKustomize &&
		doc.loaded &&
		!doc.bootstrap {
		admitted = true
	}
	evidence := rootEvidence{occurrence: l.initialOccurrence(id, record), admitted: admitted}
	if !doc.initial {
		if previous := l.roots[id]; previous.independent {
			evidence.occurrence, evidence.independent = previous.occurrence, true
		}
		return evidence
	}
	evidence.independent = a.independent(l, id, record)
	return evidence
}

func (l *Loader) initialOccurrence(id manifest.NamedResource, record *manifest.DiscoveryRecord) Occurrence {
	occurrence, _ := l.occurrence(id)
	if _, located := l.catalogOccurrence(id); located {
		return occurrence
	}
	if occurrence.Declaration != (OccurrenceID{}) {
		return occurrence
	}
	doc := l.documents.cache[record.Path]
	for i := range doc.objects {
		child := &doc.objects[i]
		if child.Obj == nil || child.Obj.Named() != record.Authored {
			continue
		}
		source := l.documents.mountedRoot(record.Path)
		file, _ := filepath.Rel(source, record.Path)
		occurrence = compactOccurrence(child.Obj, manifest.TemplateProjection{})
		occurrence.Declaration = OccurrenceID{
			Source: source, File: filepath.ToSlash(file), Document: child.outcome.Ordinal, item: child.item,
		}
		occurrence.Identity, occurrence.Authored, occurrence.object = id, record.Authored, child.Obj
		break
	}
	return occurrence
}

func (a rootAdmission) independent(l *Loader, id manifest.NamedResource, record *manifest.DiscoveryRecord) bool {
	if !a.admits(l, id, record) {
		for _, owner := range a.owners {
			peer, indexed := l.Index.Lookup(owner)
			if !l.selfClaim(owner, record.Authored) && (!indexed || l.documents.cache[peer.Path].initial) {
				return false
			}
		}
	}
	if l.entry != entryKustomize || l.documents.cache[record.Path].bootstrap {
		return !l.nativeOperand(record.Path)
	}
	object := l.CatalogObject(id)
	app, valid := manifest.ApplicationFor(object)
	if !valid {
		return false
	}
	location := SourceView(l.Store, app.Source, l.SourceRoot)
	input, _ := l.Store.GetArtifact(manifest.BootstrapSourceID, store.ArtifactContent).(*store.SourceArtifact)
	source, _ := l.Store.GetArtifact(app.Source, store.ArtifactContent).(*store.SourceArtifact)
	target := filepath.Join(location.Path, manifest.NormalizeClaimBase(app.Path))
	return !l.nativeOperand(record.Path) && !id.KindInfo().Structural || !location.Pinned && !location.External &&
		sameOriginPath(input, source, l.entryPath, target)
}

func (l *Loader) nativeOperand(file string) bool {
	for i := range l.scopes.contributions {
		contribution := &l.scopes.contributions[i]
		if contribution.membershipOnly || contribution.owner != (manifest.NamedResource{}) ||
			l.documents.cache[filepath.Join(contribution.context.root, contribution.context.dir)].body == nil {
			continue
		}
		for j := range contribution.visits {
			visit := &contribution.visits[j]
			if visit.role == kustomizeinput.Resource &&
				(visit.file == file || visit.origin != "" && visit.origin == file) {
				return true
			}
		}
	}
	return false
}

// AdmitsEntry MUST use current independent bootstrap evidence.
func (l *Loader) AdmitsEntry(id manifest.NamedResource) bool {
	if l.nativeEntry != nil && l.nativeEntry.Named() == id {
		return true
	}
	evidence := l.roots[id]
	return evidence.independent && evidence.occurrence.Declaration != (OccurrenceID{})
}

// EntryRoots MUST use captured admission evidence without I/O or publication.
func (l *Loader) EntryRoots(published iter.Seq[manifest.BaseManifest], reads *store.ReadSet) []Occurrence {
	var roots []Occurrence
	var objects admissionReader = l.Store
	if reads != nil {
		objects = reads
	}
	closure := admissionClosure{loader: l, objects: objects}
	seen := map[manifest.NamedResource]bool{}
	add := func(id manifest.NamedResource) {
		if seen[id] {
			return
		}
		seen[id] = true
		if occurrence, valid := l.entryRoot(id, &closure); valid {
			roots = append(roots, occurrence)
		}
	}
	for id := range l.roots {
		add(id)
	}
	if published != nil {
		for object := range published {
			add(object.Named())
		}
	}
	slices.SortFunc(roots, func(a, b Occurrence) int { return a.Identity.Compare(b.Identity) })
	if l.nativeEntry != nil {
		add(l.nativeEntry.Named())
	}
	return roots
}

func (l *Loader) entryRoot(id manifest.NamedResource, closure *admissionClosure) (Occurrence, bool) {
	if l.nativeEntry != nil && l.nativeEntry.Named() == id {
		return Occurrence{
			Identity: id,
			object:   l.nativeEntry,
		}, closure.objects.SourceAdmission(
			id,
			manifest.BootstrapSourceID,
		)
	}
	evidence := closure.rootEvidence(id)
	object := closure.objects.GetDeclaration(id)
	_, policy := closure.objects.Lookup(id)
	if l.AdmitsEntry(id) && object != nil {
		evidence = l.roots[id]
		evidence.admitted = true
	}
	if !evidence.independent || !evidence.admitted ||
		evidence.occurrence.Declaration == (OccurrenceID{}) && !closure.role(id).needed &&
			policy.Origin != manifest.SuppliedDeclaration {
		return Occurrence{}, false
	}
	if object == nil && evidence.occurrence.Declaration != (OccurrenceID{}) {
		object = evidence.occurrence.object
		if object == nil && evidence.occurrence.facts != nil {
			object = evidence.occurrence.facts.catalog
		}
	}
	if !l.entryRootAvailable(id, object, closure.objects) {
		return Occurrence{}, false
	}
	occurrence, _ := l.occurrence(id)
	occurrence.Declaration, occurrence.Identity = evidence.occurrence.Declaration, id
	if occurrence.Authored == (manifest.NamedResource{}) {
		record, _ := l.Index.Lookup(id)
		occurrence.Authored = record.Authored
	}
	if occurrence.object == nil {
		occurrence.object = object
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
	eligible := l.documents.cache[file].eligible
	scope := rootAdmission{denied: eligible == fileInactive}
	if scope.denied {
		return scope
	}
	l.fileOwners(file, coordinate, func(owner manifest.NamedResource) bool {
		scope.owners = appendUniqueProducer(scope.owners, owner)
		return true
	})
	if !structural {
		return scope
	}
	for _, group := range l.Index.Claims {
		if !l.documents.cache[file].bootstrap || slices.ContainsFunc(group, func(owner manifest.NamedResource) bool {
			record, _ := l.Index.Lookup(owner)
			return record.Path == file
		}) {
			scope.peers = append(scope.peers, group)
		}
	}
	return scope
}

func (l *Loader) fileOwners(file, coordinate string, yield func(manifest.NamedResource) bool) {
	if l.documents.cache[file].eligible == fileInactive {
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
	if coordinate == "" || l.documents.cache[file].eligible == fileIndependent {
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

func (a rootAdmission) admits(l *Loader, id manifest.NamedResource, recordValue *manifest.DiscoveryRecord) bool {
	record := *recordValue

	if a.denied {
		return false
	}
	var peers [][]manifest.NamedResource
	if id.KindInfo().Structural {
		for _, group := range a.peers {
			if slices.ContainsFunc(
				group,
				func(peer manifest.NamedResource) bool { return l.selfClaim(peer, record.Authored) },
			) {
				peers = append(peers, group)
			}
		}
	}
	for _, owner := range a.owners {
		if owner == id || l.selfClaim(owner, record.Authored) {
			continue
		}
		peer, _ := l.Index.Lookup(owner)
		if l.documents.cache[record.Path].bootstrap && peer.Path != record.Path {
			return false
		}
		if !slices.ContainsFunc(
			peers,
			func(group []manifest.NamedResource) bool { return slices.Contains(group, owner) },
		) {
			return false
		}
	}
	return true
}
