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
	owner       manifest.NamedResource
	occurrence  Occurrence
	admitted    bool
	independent bool
	entry       bool
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
	evidence := rootEvidence{
		owner:      a.owner(l, id, record),
		occurrence: l.initialOccurrence(id, record),
		admitted:   admitted,
	}
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
	if a.denied {
		return false
	}
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
	defer closure.releaseComparisons()
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
	if !evidence.entry ||
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

func (a rootAdmission) admits(l *Loader, id manifest.NamedResource, record *manifest.DiscoveryRecord) bool {
	return !a.denied && a.owner(l, id, record) == (manifest.NamedResource{})
}

func (a rootAdmission) owner(
	l *Loader,
	id manifest.NamedResource,
	record *manifest.DiscoveryRecord,
) manifest.NamedResource {
	for _, owner := range a.owners {
		if owner == id || l.selfClaim(owner, record.Authored) {
			continue
		}
		peer, _ := l.Index.Lookup(owner)
		if l.documents.cache[record.Path].bootstrap && peer.Path != record.Path {
			return owner
		}
		if !id.KindInfo().Structural || !slices.ContainsFunc(a.peers, func(group []manifest.NamedResource) bool {
			return slices.Contains(group, owner) && slices.ContainsFunc(group, func(peer manifest.NamedResource) bool {
				return l.selfClaim(peer, record.Authored)
			})
		}) {
			return owner
		}
	}
	return manifest.NamedResource{}
}

type rootQuery struct {
	loader *Loader
	live   *admissionClosure
}

func (q *rootQuery) admitsRoot(obj manifest.BaseManifest, file string) bool {
	if obj == nil || !manifest.AdmitsRoot(obj) {
		return false
	}
	id := obj.Named()
	record, indexed := q.loader.Index.Lookup(id)
	if !indexed || record.Path != file {
		return false
	}
	if q.live == nil && q.loader.frozen {
		q.live = &admissionClosure{loader: q.loader, objects: q.loader.Store}
	}
	if q.live != nil {
		q.live.incoming = store.Child{Obj: obj, Productions: q.live.objects.Productions(id, q.loader.Index)}
		defer q.live.releaseComparisons()
		return q.live.rootEvidence(id).admitted
	}
	return q.loader.classifyRoot(id, &record).admitted
}

func (l *Loader) classifyRoot(id manifest.NamedResource, record *manifest.DiscoveryRecord) rootEvidence {
	if record.Path != "" && l.terminalInput(record.Path) {
		return rootEvidence{}
	}
	if evidence, known := l.roots[id]; known {
		return evidence
	}
	if record.Path == "" {
		native := l.documents.compact && record.Parent.ReportHidden() && record.Parent == record.BuilderOwner
		admitted := record.Authored == (manifest.NamedResource{}) &&
			(record.Parent == (manifest.NamedResource{}) || native) && l.candidates[id].Obj != nil
		return rootEvidence{admitted: admitted, independent: admitted}
	}
	if doc := l.documents.cache[record.Path]; !id.KindInfo().Discovery && doc.rootKnown {
		return rootEvidence{admitted: doc.rootAdmitted}
	}
	scope := l.rootAdmission(record.Path, record.File, id.KindInfo().Structural)
	return scope.evidence(l, id, record)
}

func (c *admissionClosure) rootEvidence(id manifest.NamedResource) rootEvidence {
	root := id
	if occurrence, ok := c.loader.occurrence(id); ok && occurrence.Authored.Namespace == "" {
		if _, exact := c.loader.Index.Lookup(id); !exact {
			root.Namespace = ""
		}
	}
	if evidence, known := c.loader.roots[root]; known {
		return c.supportedRoot(id, &evidence)
	}
	if record, indexed := c.loader.Index.Lookup(root); indexed {
		evidence := c.loader.classifyRoot(root, &record)
		return c.supportedRoot(id, &evidence)
	}
	object := c.object(id)
	if object == nil || !manifest.AdmitsRoot(object) {
		return rootEvidence{}
	}
	return c.supportedRoot(id, &rootEvidence{admitted: true, independent: true})
}

func (c *admissionClosure) supportedRoot(id manifest.NamedResource, original *rootEvidence) rootEvidence {
	evidence := *original
	if evidence.occurrence.Declaration == (OccurrenceID{}) {
		evidence.independent = evidence.admitted
	}
	if c.incoming.Obj == nil || c.incoming.Obj.Named() != id {
		if object, policy := c.objects.Lookup(id); object != nil && policy.Origin == manifest.SuppliedDeclaration &&
			c.objects.GetDeclaration(id) == object && manifest.AdmitsRoot(object) {
			return rootEvidence{admitted: true, independent: true, entry: true}
		}
	}
	if !evidence.independent {
		return evidence
	}
	supported := evidence.occurrence.Declaration == (OccurrenceID{})
	open := supported
	for _, source := range [...]manifest.NamedResource{c.source(id), manifest.BootstrapSourceID} {
		_, input, _ := c.delta(source)
		allowed := c.objects.SourceAdmission(id, source)
		open = open || allowed
		if input != nil && allowed {
			supported = true
			break
		}
	}
	evidence.admitted = evidence.admitted && open
	if owner := c.rootOwner(id, evidence.occurrence.Declaration); owner != (manifest.NamedResource{}) {
		evidence.owner = owner
		evidence.admitted, supported = false, false
	}
	evidence.independent = supported
	evidence.entry = supported || c.acceptedEntry(id, evidence.occurrence.Declaration)
	if supported {
		evidence.admitted, evidence.owner = true, manifest.NamedResource{}
	} else if !evidence.entry {
		evidence.occurrence = Occurrence{}
	}
	return evidence
}

func (c *admissionClosure) acceptedEntry(id manifest.NamedResource, occurrence OccurrenceID) bool {
	if occurrence == (OccurrenceID{}) {
		return false
	}
	object := c.objects.GetDeclaration(id)
	if object == nil {
		return false
	}
	productions := c.productions(id)
	for i := range productions {
		owner := productions[i].Owner
		if owner == (manifest.NamedResource{}) || owner == id {
			continue
		}
		output, _ := c.objects.GetArtifact(owner, store.ArtifactContent).(*store.Emission)
		if output != nil && output.Complete && c.acceptedDeclaration(id, owner, object) {
			return true
		}
	}
	return false
}

func (c *admissionClosure) rootOwner(id manifest.NamedResource, occurrence OccurrenceID) manifest.NamedResource {
	record := c.record(id)
	native := record.Parent.ReportHidden() && record.Parent == record.BuilderOwner &&
		c.loader.registeredRoot(&record) != ""
	productions := c.productions(id)
	if occurrence != (OccurrenceID{}) {
		productions = c.objects.Productions(id, &manifest.DiscoveryIndex{})
	}
	for i := range productions {
		production := &productions[i]
		owner := production.Materializer(id)
		if owner == (manifest.NamedResource{}) && c.states[id].staticOwner {
			owner = production.Owner
		}
		if native && owner == record.BuilderOwner {
			continue
		}
		if owner != (manifest.NamedResource{}) && owner != id && !c.rootPeer(id, owner) {
			return owner
		}
	}
	var result manifest.NamedResource
	if record.Path != "" {
		c.hasOwner(func(owner manifest.BaseManifest) bool {
			if owner.Named() != id && c.coversDeclaration(owner, id) && !c.rootPeer(id, owner.Named()) {
				result = owner.Named()
				return true
			}
			return false
		})
	}
	return result
}

func (c *admissionClosure) rootPeer(id, owner manifest.NamedResource) bool {
	if c.rootProducer(id, owner) {
		return true
	}
	if !id.KindInfo().Structural {
		return false
	}
	object := c.loader.CatalogObject(id)
	application, valid := manifest.ApplicationFor(object)
	if !valid {
		return false
	}
	location := SourceView(c.objects, application.Source, c.loader.SourceRoot)
	if location.Pinned || location.External {
		return false
	}
	coordinate := c.loader.fileCoordinate(
		new(c.loader.documents).localPath(filepath.Join(location.Path, application.Path)),
	)
	group := c.loader.Index.Claims[coordinate+"/"]
	record, _ := c.loader.Index.Lookup(id)
	return slices.Contains(group, owner) && slices.ContainsFunc(group, func(peer manifest.NamedResource) bool {
		return peer == id || c.loader.selfClaim(peer, record.Authored)
	})
}

// An emitted application MUST NOT suppress the producer route that supports it.
func (c *admissionClosure) rootProducer(id, target manifest.NamedResource) bool {
	pending := []manifest.NamedResource{target}
	for cursor := 0; cursor < len(pending); cursor++ {
		current := pending[cursor]
		productions := c.productions(current)
		for i := range productions {
			owner := productions[i].Materializer(current)
			if owner == id {
				return true
			}
			if owner != (manifest.NamedResource{}) && !slices.Contains(pending, owner) {
				pending = append(pending, owner)
			}
		}
	}
	return false
}

func (c *admissionClosure) hasOwner(matches func(manifest.BaseManifest) bool) bool {
	if !c.ownersKnown {
		c.collectOwners()
	}
	for _, owner := range c.ownerCandidates {
		if matches(owner) && slices.ContainsFunc(c.productions(owner.Named()), func(p manifest.Production) bool {
			return p.Owner != (manifest.NamedResource{}) && p.Owner != owner.Named()
		}) {
			return true
		}
	}
	return false
}

func (c *admissionClosure) collectOwners() {
	var prospective map[manifest.NamedResource]bool
	include := func(id manifest.NamedResource, productions []manifest.Production) {
		if id.KindInfo().Structural && slices.ContainsFunc(productions, func(p manifest.Production) bool {
			return p.Owner != (manifest.NamedResource{}) && p.Owner != id
		}) {
			if prospective == nil {
				prospective = make(map[manifest.NamedResource]bool)
			}
			id.Namespace = ""
			prospective[id] = true
		}
	}
	for source := range c.loader.Options.Changes.Sources() {
		delta, _, _ := c.loader.Options.Changes.ForSource(source)
		for id, input := range delta.Inputs(manifest.NamedResource{}) {
			include(id, input.Productions)
		}
	}
	for _, records := range c.accepted {
		for id := range records {
			include(id, records[id].Productions)
		}
	}
	for info := range manifest.Kinds() {
		if !info.Structural {
			continue
		}
		candidates := c.objects.ListProductionCandidates(info.Group, info.Kind, c.loader.Index, prospective)
		c.ownerCandidates = append(c.ownerCandidates, candidates...)
	}
	c.ownersKnown = true
}
