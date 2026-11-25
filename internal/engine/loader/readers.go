package loader

import (
	"cmp"
	"context"
	"io/fs"
	"path"
	"path/filepath"
	"slices"
	"strings"

	"github.com/home-operations/flate/internal/engine/change/inputprojection"
	"github.com/home-operations/flate/internal/engine/kustomize/kustomizeinput"
	"github.com/home-operations/flate/internal/engine/selection"
	"github.com/home-operations/flate/internal/engine/store"
	"github.com/home-operations/flate/pkg/manifest"
)

func (l *Loader) recordReader(file, local string, readerValue *manifest.FileReader) {
	reader := *readerValue
	add := func(file string) {
		readers := l.Index.Readers[file]
		i, found := slices.BinarySearchFunc(readers, reader, func(a, b manifest.FileReader) int {
			if order := cmp.Or(
				a.Reader.Compare(b.Reader),
				a.Source.Compare(b.Source),
				a.Declaration.Compare(b.Declaration),
			); order != 0 {
				return order
			}
			if a.Materialize != b.Materialize {
				if a.Materialize {
					return 1
				}
				return -1
			}
			if a.Exact == b.Exact {
				return 0
			}
			if a.Exact {
				return 1
			}
			return -1
		})
		if !found {
			l.Index.Readers[file] = slices.Insert(readers, i, reader)
		}
	}
	add(file)
	if local != "" {
		add(local)
	}
}

func (l *Loader) finalizeRecords(
	ctx context.Context,
	repoRoot string,
	emittedFiles map[string][]manifest.NamedResource,
) error {
	rf := readerFinalizer{
		loader: l,

		repoRoot: repoRoot,

		emittedFiles: emittedFiles,
	}
	for id := range l.locations {
		occurrence := l.locations[id]
		if occurrence.facts.derivedInputs == 0 {
			continue
		}
		facts := *occurrence.facts
		facts.refs = slices.Clone(facts.refs[:len(facts.refs)-facts.derivedInputs])
		facts.reverse = slices.Clone(facts.reverse[:len(facts.reverse)-facts.derivedInputs])
		facts.derivedInputs = 0
		occurrence.facts = &facts
		l.locations[id] = occurrence
	}
	rf.collectEntryInputs()
	rf.collectControls()
	rf.objects = map[manifest.NamedResource]manifest.BaseManifest{}
	rf.sources = map[manifest.NamedResource]graphVisit{}
	rf.charts = map[string]chartReaders{}
	rf.templateBodies = map[string][][]byte{}
	rf.templateRefs = map[[2]string]manifest.TemplateProjection{}
	rf.templateWalks = map[[2]string]bool{}
	rf.visited = map[manifest.NamedResource]struct{}{}

	rf.loader.projectTemplates()
	//nolint:gocritic // Map iteration supplies value snapshots without an additional lookup.
	for id, r := range rf.loader.Index.Records {
		occurrence, located := rf.loader.locations[id]
		if rf.repoRoot == "" || len(r.Refs) == 0 && (!located || len(occurrence.facts.templateInputs) == 0) {
			continue
		}
		if occurrence.requiresBuild && r.Builder == "" && len(r.Productions) != 0 {
			if err := rf.collectPendingPackageReads(ctx, &occurrence, &r); err != nil {
				return err
			}
			continue
		}
		clear(rf.visited)
		clear(rf.templateWalks)
		if err := rf.collect(ctx, id, id, false); err != nil {
			return err
		}
	}
	//nolint:gocritic // Map iteration supplies value snapshots without an additional lookup.
	for id, r := range rf.loader.Index.Records {
		rf.admitRecord(id, &r)
		rf.setRecordOrigin(id, &r)
		rf.compactOrigins(id, &r)
		rf.setRecordOutput(id, &r)
		rf.setRecordParent(id, &r)
		rf.setRecordInputs(id, &r)
		rf.loader.Index.Records[id] = r
	}
	return nil
}

type readerFinalizer struct {
	loader          *Loader
	repoRoot        string
	emittedFiles    map[string][]manifest.NamedResource
	controls        map[manifest.NamedResource][]string
	objects         map[manifest.NamedResource]manifest.BaseManifest
	sources         map[manifest.NamedResource]graphVisit
	charts          map[string]chartReaders
	repositories    map[string][]manifest.NamedResource
	templateBodies  map[string][][]byte
	templateRefs    map[[2]string]manifest.TemplateProjection
	templateWalks   map[[2]string]bool
	visited         map[manifest.NamedResource]struct{}
	unresolvedPaths map[manifest.NamedResource]bool
}

func (rf *readerFinalizer) object(id manifest.NamedResource) manifest.BaseManifest {
	if record, ok := rf.loader.Index.Lookup(id); ok && record.Builder != "" && record.Declaration != nil {
		return record.Declaration
	}
	if obj := rf.loader.Store.GetObject(id); obj != nil {
		return obj
	}
	if obj, seen := rf.objects[id]; seen {
		return obj
	}
	r, _ := rf.loader.Index.Lookup(id)
	obj := rf.loader.IndexedObject(id, r).Obj
	rf.objects[id] = obj
	return obj
}

func (rf *readerFinalizer) project(ctx context.Context,

	inputValue *manifest.TemplateInput, consumer manifest.NamedResource,
) error {
	input := *inputValue
	if input.Reader.Namespace == "" && input.Reader.KindInfo().Namespaced() {
		input.Reader.Namespace = consumer.Namespace
	}
	if rf.loader.Index.IsFileIndexed(input.Reader) && !input.UnresolvedPath {
		return nil
	}
	ref := input.Reference
	if input.UnresolvedPath {
		if rf.unresolvedPaths == nil {
			rf.unresolvedPaths = map[manifest.NamedResource]bool{}
		}
		rf.unresolvedPaths[consumer] = true
		if ref.Name == "" || rf.sourceVisit(ref.NamedResource).root == "" {
			rf.loader.recordReader(".", "", new(manifest.FileReader{Reader: consumer, Materialize: true}))
			return nil
		}
		ref.Files = append(slices.Clone(ref.Files), ".")
		ref.Subtree = true
	}
	if ref.NamespaceDefault && ref.Namespace == "" {
		ref.Namespace = consumer.Namespace
	}
	return rf.observe(ctx, new(ref), consumer, true)
}

func (rf *readerFinalizer) observe(ctx context.Context,

	refValue *manifest.Reference, consumer manifest.NamedResource, materialize bool,
) error {
	ref := *refValue
	if ref.ContentInput() {
		rf.observeContent(&ref, consumer, materialize)
	}
	if ref.Use == "chart" {
		if err := rf.collect(ctx, ref.NamedResource, consumer, materialize); err != nil {
			return err
		}
		return nil
	}
	if len(ref.Files) == 0 && ref.ChartPath == "" || ref.Name == "" || ref.Kind == "" {
		return nil
	}
	v := rf.sourceVisit(ref.NamedResource)
	if v.root == "" {
		return nil
	}
	rf.observeFiles(&ref, consumer, materialize, new(v))
	return rf.observeChart(
		ctx,
		&ref,
		new(manifest.FileReader{Reader: consumer, Source: ref.NamedResource, Exact: true, Materialize: materialize}),
		new(v),
	)
}

func (rf *readerFinalizer) collect(ctx context.Context,

	id, consumer manifest.NamedResource, materialize bool,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if _, seen := rf.visited[id]; seen {
		return nil
	}
	rf.visited[id] = struct{}{}
	if occurrence, ok := rf.loader.locations[id]; ok {
		if err := rf.projectOccurrence(ctx, &occurrence, consumer); err != nil {
			return err
		}
	}
	for ref := range rf.loader.References(id) {
		if err := rf.observe(ctx, new(ref), consumer, materialize); err != nil {
			return err
		}
	}
	return nil
}

func (rf *readerFinalizer) collectControls() {
	rf.controls = map[manifest.NamedResource][]string{}
	if rf.loader.documents.compact && rf.loader.scopes != nil {
		{
			contributionValues := rf.loader.scopes.contributions
			for contributionIndex := range contributionValues {
				contribution := contributionValues[contributionIndex]
				if contribution.owner == (manifest.NamedResource{}) || contribution.membershipOnly ||
					!contribution.active {
					continue
				}
				rf.collectControlVisits(contribution.owner, contribution.visits)
			}
		}
	}
	for owner, files := range rf.controls {
		slices.Sort(files)
		rf.controls[owner] = slices.Compact(files)
	}
}

func (rf *readerFinalizer) collectControlVisits(owner manifest.NamedResource, visitValues []graphVisit) {
	for visitIndex := range visitValues {
		visit := visitValues[visitIndex]
		if visit.role == kustomizeinput.Resource && visit.document == nil {
			continue
		}
		for _, file := range []string{visit.coordinate, visit.localCoordinate()} {
			if file != "" {
				rf.controls[owner] = append(rf.controls[owner], file)
			}
		}
	}
}

func (rf *readerFinalizer) admitRecord(id manifest.NamedResource, r *manifest.DiscoveryRecord) {
	if r.Path == "" {
		return
	}
	kind := id.KindInfo()
	if kind.Discovery {
		if previous, known := rf.loader.roots.lookup(id); known {
			prepared := r.Builder != "" && rf.loader.registeredRoot(id, r) != ""
			if previous.scope.Prepared != prepared {
				evidence := *previous
				evidence.scope.Prepared = prepared
				rf.loader.roots.set(id, &evidence)
			}
			rf.loader.retainRootApplication(id, r.Path, &previous.scope)
			return
		}
		view := rf.loader.physicalView()
		evidence := view.rootScope(rf.loader, &rootScopeInput{
			identity: id, record: *r, admission: rf.loader.rootAdmission(r.Path, r.File, kind.Structural),
		})
		rf.loader.roots.set(id, &evidence)
		rf.loader.retainRootApplication(id, r.Path, &evidence.scope)
		return
	}
	doc := rf.loader.documents.cache[r.Path]
	if r.Authored == (manifest.NamedResource{}) || doc.rootKnown {
		return
	}
	scope := rf.loader.rootAdmission(r.Path, r.File, false)
	doc.rootOwners, doc.rootPeers, doc.rootNative = scope.owners, scope.peers, rf.loader.nativeOperand(r.Path)
	view := rf.loader.physicalView()
	evidence := view.rootScope(rf.loader, &rootScopeInput{identity: id, record: *r, admission: scope})
	doc.rootAdmitted = selection.Root(evidence.view()).Admitted
	doc.rootKnown = true
	rf.loader.documents.cache[r.Path] = doc
}

func (l *Loader) retainRootApplication(id manifest.NamedResource, file string, scope *selection.RootScopeFacts) {
	if !scope.Application {
		return
	}
	doc := l.documents.cache[file]
	if doc.rootApplications == nil {
		doc.rootApplications = map[manifest.NamedResource]rootApplication{}
	}
	doc.rootApplications[id] = rootApplication{
		local: scope.LocalApplication, input: scope.InputApplication, sameOrigin: scope.SameEntryOrigin,
	}
	l.documents.cache[file] = doc
}

func (rf *readerFinalizer) setRecordOrigin(id manifest.NamedResource, r *manifest.DiscoveryRecord) {
	if r.File == "" {
		return
	}
	kind := id.KindInfo()
	if parents := rf.emittedFiles[r.File]; kind.Structural &&
		!selection.Root(rf.loader.roots.get(id).view()).Independent &&
		r.Parent == (manifest.NamedResource{}) &&
		len(parents) == 1 &&
		parents[0] != id {
		r.Parent = parents[0]
	}
	if selection.Root(rf.loader.roots.get(id).view()).Independent {
		input := rf.loader.documents.artifacts[new(rf.loader.documents).mountedRoot(r.Path)]
		matchesRecord := func(p *manifest.Production) bool {
			return p.File == r.File || p.Path != "" && p.Path == r.Path
		}
		for i := range r.Productions {
			p := &r.Productions[i]
			bound, _ := rf.loader.Store.GetArtifact(p.Source, store.ArtifactContent).(*store.SourceArtifact)
			sameMount := input != nil && bound == input && bound.Provider != store.ProviderInput
			if sameMount && p.Owner != id && matchesRecord(p) {
				p.Owner, p.Declaration = manifest.NamedResource{}, manifest.NamedResource{}
				p.Authored, p.Path, p.File = r.Authored, r.Path, r.File
			}
		}
		r.Productions = slices.DeleteFunc(r.Productions, func(p manifest.Production) bool {
			return p.Owner != (manifest.NamedResource{}) && p.Owner != id && matchesRecord(&p)
		})
	}
	if len(r.Productions) == 0 && r.Authored != (manifest.NamedResource{}) {
		r.Productions = []manifest.Production{authoredProduction(r)}
	}
}

func authoredProduction(r *manifest.DiscoveryRecord) manifest.Production {
	if r.File == "" {
		return manifest.Production{}
	}
	return manifest.Production{Authored: r.Authored, Path: r.Path, File: r.File, Source: manifest.BootstrapSourceID}
}

func (rf *readerFinalizer) compactOrigins(id manifest.NamedResource, r *manifest.DiscoveryRecord) {
	// Owner evidence MUST survive compaction while provisional origins are removed.
	r.Productions = slices.DeleteFunc(r.Productions, func(p manifest.Production) bool {
		if p.Owner != (manifest.NamedResource{}) || p.Path == "" {
			return false
		}
		return slices.ContainsFunc(r.Productions, func(owner manifest.Production) bool {
			return owner.Owner != (manifest.NamedResource{}) &&
				(owner.Source == p.Source && owner.Authored == p.Authored && owner.Path == p.Path && owner.File == p.File ||
					p.Source == manifest.BootstrapSourceID && p.Path == r.Path && !selection.Root(selection.CaptureRoot(rf.loader.RootEvidence(id, nil), rf.loader.Store, rf.loader.Index)).Admitted)
		})
	})
	if rf.loader.documents.compact && !rf.loader.documents.eager {
		rf.retainBootstrapOrigins(id, r)
	}
	slices.SortFunc(r.Productions, func(a, b manifest.Production) int { return a.Owner.Compare(b.Owner) })
}

func (rf *readerFinalizer) retainBootstrapOrigins(id manifest.NamedResource, r *manifest.DiscoveryRecord) {
	admitted := false
	for i := range r.Productions {
		p := r.Productions[i]
		if p.Owner == (manifest.NamedResource{}) || p.Materializer(id) != (manifest.NamedResource{}) ||
			p.Declaration != (manifest.NamedResource{}) || p.Authored == (manifest.NamedResource{}) {
			continue
		}
		// Self-sources MUST bootstrap before their readers can publish them.
		if !p.Owner.ReportHidden() && p.Source != id && !rf.loader.Options.Session.RequestsNamedRoot(id) {
			continue
		}
		if !admitted {
			root := selection.CaptureRoot(rf.loader.RootEvidence(id, nil), rf.loader.Store, rf.loader.Index)
			admitted = selection.Root(root).Admitted
			if !admitted {
				return
			}
		}
		p.Owner = manifest.NamedResource{}
		if !slices.Contains(r.Productions, p) {
			r.Productions = append(r.Productions, p)
		}
	}
}

func (rf *readerFinalizer) setRecordOutput(id manifest.NamedResource, r *manifest.DiscoveryRecord) {
	kind := id.KindInfo()
	if (!rf.loader.documents.compact || rf.loader.documents.eager) && r.Builder == "" {
		r.Declaration = rf.loader.IndexedObject(id, *r).Obj
	}
	r.StaticOutput = kind.Structural || len(rf.loader.templateProjections[id].Targets) != 0
	if r.Builder == "" && !rf.loader.documents.eager && len(rf.loader.locations) != 0 {
		if occurrence, known := rf.loader.catalogOccurrence(id); known {
			r.StaticOutput = !occurrence.facts.opaque(rf.loader.templateProjections[id].Targets)
		}
	}
	r.StaticOutput = r.StaticOutput && !rf.unresolvedPaths[id]
}

func (rf *readerFinalizer) setRecordParent(id manifest.NamedResource, r *manifest.DiscoveryRecord) {
	l := rf.loader
	if !l.documents.compact || l.documents.eager || !id.KindInfo().Parent {
		return
	}
	if r.Parent != (manifest.NamedResource{}) || selection.Root(l.roots.get(id).view()).Independent {
		return
	}
	if !l.locations[id].requiresBuild {
		evidence := selection.CaptureRoot(l.RootEvidence(id, nil), l.Store, l.Index)
		if selection.Root(evidence).Admitted {
			return
		}
	}
	var owner manifest.NamedResource
	for i := range r.Productions {
		parent := r.Productions[i].Materializer(id)
		if parent == (manifest.NamedResource{}) || parent == id {
			continue
		}
		if owner != (manifest.NamedResource{}) && owner != parent {
			return
		}
		owner = parent
	}
	r.Parent = owner
}

func (rf *readerFinalizer) setRecordInputs(id manifest.NamedResource, r *manifest.DiscoveryRecord) {
	if len(rf.controls) != 0 {
		owners := []manifest.NamedResource{manifest.Resolve(r.Parent, "", id.Namespace)}
		{
			pValues3 := r.Productions
			for pIndex3 := range pValues3 {
				p := pValues3[pIndex3]
				owner := p.Materializer(id)
				if owner != (manifest.NamedResource{}) && !slices.Contains(owners, owner) {
					owners = append(owners, owner)
				}
			}
		}
		for _, owner := range owners {
			files := rf.controls[owner]
			if len(r.Inputs) == 0 {
				r.Inputs = files
			} else if len(files) != 0 && !slices.Equal(r.Inputs, files) {
				r.Inputs = append(slices.Clone(r.Inputs), files...)
				slices.Sort(r.Inputs)
				r.Inputs = slices.Compact(r.Inputs)
			}
		}
	}
}

func (rf *readerFinalizer) observeContent(ref *manifest.Reference, consumer manifest.NamedResource, materialize bool) {
	record, _ := rf.loader.Index.Lookup(ref.NamedResource)
	if record.File != "" {
		sources := productionSources(record.Productions)
		for _, source := range sources {
			rf.loader.recordReader(
				record.File,
				"",
				new(manifest.FileReader{
					Reader:      consumer,
					Source:      source,
					Declaration: ref.NamedResource,
					Exact:       true,
					Materialize: materialize,
				}),
			)
		}
	}
	for _, file := range record.Inputs {
		if file == record.File {
			continue
		}
		rf.loader.recordReader(
			file,
			"",
			new(manifest.FileReader{
				Reader:      consumer,
				Source:      manifest.BootstrapSourceID,
				Declaration: ref.NamedResource,
				Materialize: materialize,
			}),
		)
	}
}

func (rf *readerFinalizer) sourceVisit(id manifest.NamedResource) graphVisit {
	v, seen := rf.sources[id]
	if seen {
		return v
	}
	location2 := SourceView(rf.loader.Store, id, rf.repoRoot)
	if !location2.External {
		v.root = location2.Path
		prefix, _ := filepath.Rel(rf.repoRoot, location2.Path)
		if prefix != "." {
			v.prefix = filepath.ToSlash(prefix)
			if !location2.Pinned {
				v.localBase = new(rf.loader.documents).localBase(location2.Path, rf.repoRoot)
			}
		}
	}
	rf.sources[id] = v
	return v
}

func (rf *readerFinalizer) observeFiles(
	ref *manifest.Reference,
	consumer manifest.NamedResource,
	materialize bool,
	visitValue *graphVisit,
) {
	v := *visitValue
	var input sourcePathView
	if ref.Subtree {
		art, _ := rf.loader.Store.GetArtifact(ref.NamedResource, store.ArtifactContent).(*store.SourceArtifact)
		if art == nil || art.FS == nil {
			return
		}
		input.FS = art.FS
	}
	for _, name := range ref.Files {
		clean := path.Clean(name)
		if ref.Subtree {
			var valid bool
			clean, valid = input.resolve(cmp.Or(name, "."))
			if !valid {
				continue
			}
		} else if name == "" || !fs.ValidPath(clean) || clean == "." || strings.IndexByte(clean, 0) >= 0 {
			continue
		}
		v.coordinate = path.Join(v.prefix, clean)
		rf.loader.recordReader(
			v.coordinate,
			v.localCoordinate(),
			new(manifest.FileReader{
				Reader:      consumer,
				Source:      ref.NamedResource,
				Exact:       !ref.Subtree,
				Materialize: materialize,
			}),
		)
	}
}

func (rf *readerFinalizer) observeChart(
	ctx context.Context,
	ref *manifest.Reference,
	readerValue *manifest.FileReader,
	visitValue *graphVisit,
) error {
	reader := *readerValue
	v := *visitValue
	if ref.ChartPath == "" {
		return nil
	}
	ref.ChartPath = path.Clean(ref.ChartPath)
	location := filepath.Join(v.root, filepath.FromSlash(ref.ChartPath))
	names, err := rf.chartNames(ctx, location, ref.ChartPath)
	if err != nil {
		return err
	}
	for _, name := range names.names {
		v.coordinate = path.Join(v.prefix, name)
		rf.loader.recordReader(v.coordinate, v.localCoordinate(), new(reader))
	}
	for _, name := range names.missing {
		v.coordinate = path.Join(v.prefix, name)
		missing := reader
		missing.Exact = false
		rf.loader.recordReader(v.coordinate, v.localCoordinate(), &missing)
	}
	if names.remote {
		if err := rf.observeRepositories(ctx, reader.Reader, ref.Namespace, reader.Materialize); err != nil {
			return err
		}
	}
	namespace := reader.Reader.Namespace
	if object := rf.object(reader.Reader); manifest.KindOf(object).Parent {
		namespace = object.EmissionPolicy().Namespace
	}
	key := [2]string{location, namespace}
	refs, seen := rf.templateRefs[key]
	if !seen {
		rf.templateRefs[key] = manifest.TemplateProjection{}
		for _, data := range rf.templateBodies[location] {
			next, err := rf.loader.documents.projections.ProjectTemplate(ctx, data, namespace)
			if err != nil {
				return err
			}
			refs.Inputs = append(refs.Inputs, next.Inputs...)
			refs.Targets = append(refs.Targets, next.Targets...)
			refs.Objects = append(refs.Objects, next.Objects...)
		}
		rf.templateRefs[key] = refs
	}
	if rf.templateWalks[key] {
		return nil
	}
	rf.templateWalks[key] = true
	rf.loader.retainTemplateProjection(reader.Reader, &refs)
	{
		inputValues := refs.Inputs
		for inputIndex := range inputValues {
			input := inputValues[inputIndex]
			if err := rf.project(ctx, new(input), reader.Reader); err != nil {
				return err
			}
		}
	}

	return nil
}

func productionSources(productions []manifest.Production) []manifest.NamedResource {
	if len(productions) == 0 {
		return []manifest.NamedResource{manifest.BootstrapSourceID}
	}
	var sources []manifest.NamedResource
	for i := range productions {
		source := productions[i].Source
		if source == (manifest.NamedResource{}) {
			continue
		}
		if !slices.Contains(sources, source) {
			sources = append(sources, source)
		}
	}
	return sources
}

func (rf *readerFinalizer) projectOccurrence(
	ctx context.Context,
	occurrence *authoredOccurrence,
	consumer manifest.NamedResource,
) error {
	for i := range occurrence.facts.templateInputs {
		input := occurrence.facts.templateInputs[i]
		if err := rf.project(ctx, new(input), consumer); err != nil {
			return err
		}
	}
	return nil
}

func (rf *readerFinalizer) collectEntryInputs() {
	if !rf.loader.documents.compact || rf.loader.scopes == nil {
		return
	}
	entry := rf.loader.fileCoordinate(rf.loader.entryPath)
	external := rf.aggregateEntryReads(entry)
	recordInputs := func(root string, inputs []string) {
		id := manifest.NewEntryKustomization(manifest.BootstrapSourceID, root, manifest.NamedResource{}).Named()
		record := rf.loader.Index.Records[id]
		slices.Sort(inputs)
		record.ContentRoots = map[manifest.NamedResource][]string{manifest.BootstrapSourceID: slices.Compact(inputs)}
		rf.loader.Index.Records[id] = record
	}
	for _, root := range rf.loader.scopes.roots {
		visits := rf.loader.scopeWalk(new(graphVisit{root: rf.repoRoot, dir: root}))
		inputs := rf.appendEntryInputs(nil, visits)
		if root == entry {
			inputs = append(inputs, external...)
		}
		recordInputs(root, inputs)
	}
	if len(external) != 0 && !slices.Contains(rf.loader.scopes.roots, entry) {
		recordInputs(entry, external)
	}
}

func (rf *readerFinalizer) aggregateEntryReads(entry string) []string {
	input := rf.loader.documents.artifacts[rf.repoRoot]
	if input == nil {
		return nil
	}
	overlay, ok := input.FS.(*inputprojection.Overlay)
	if !ok || entry == "" || entry == "." {
		return nil
	}
	var inputs []string
	for _, resource := range overlay.Resources {
		member := path.Join(entry, resource)
		owners := rf.loader.Index.OwnersOfFile(path.Join(member, manifest.KustomizeBuilderFilenames[0])).Owners
		// A named Flux owner retains the read footprint of its package.
		if slices.ContainsFunc(owners, func(id manifest.NamedResource) bool {
			return id.KindInfo().Render && !id.ReportHidden()
		}) {
			continue
		}
		visits := rf.loader.scopeWalk(new(graphVisit{root: rf.repoRoot, dir: member}))
		inputs = rf.appendEntryInputs(inputs, visits)
	}
	return slices.DeleteFunc(inputs, func(input string) bool {
		return input == entry || strings.HasPrefix(input, entry+"/")
	})
}

func (rf *readerFinalizer) appendEntryInputs(inputs []string, visits []graphVisit) []string {
	for i := range visits {
		visit := &visits[i]
		if visit.incomplete != "" {
			inputs = append(inputs, path.Join(visit.prefix, visit.dir))
		} else if rf.loader.documents.cache[visit.file].pathState == pathMissing {
			inputs = append(inputs, visit.coordinate)
		}
		for _, file := range []string{visit.coordinate, visit.localCoordinate()} {
			if file != "" {
				inputs = append(inputs, file)
			}
		}
	}
	return inputs
}

func (rf *readerFinalizer) observeRepositories(
	ctx context.Context,
	consumer manifest.NamedResource,
	namespace string,
	materialize bool,
) error {
	ids := rf.repositoryInputs(namespace)
	object := rf.object(consumer)
	if object == nil {
		return nil
	}
	occurrence, present := rf.loader.catalogOccurrence(consumer)
	if !present || occurrence.object != object && occurrence.facts.catalog != object {
		projection, _ := manifest.ProjectProducer(object, nil)
		previous := occurrence
		occurrence = compactOccurrence(object, projection)
		record := rf.loader.Index.Records[consumer]
		if present && record.Path == filepath.Join(previous.Declaration.Source, previous.Declaration.File) {
			occurrence.Declaration, occurrence.Authored = previous.Declaration, previous.Authored
			occurrence.requiresBuild = previous.requiresBuild
		}
		occurrence.object = object
	}
	facts := *occurrence.facts
	facts.refs, facts.reverse = slices.Clone(facts.refs), slices.Clone(facts.reverse)
	for _, id := range ids {
		candidate := rf.object(id)
		if candidate == nil {
			continue
		}
		record := rf.loader.Index.Records[id]
		record.Declaration = candidate
		rf.loader.Index.Records[id] = record
		ref := manifest.Reference{NamedResource: id, Content: true}
		if !slices.ContainsFunc(facts.relations(), func(r manifest.Reference) bool { return r.NamedResource == id }) {
			facts.refs = append(facts.refs, ref)
			facts.reverse = append(facts.reverse, id)
			facts.derivedInputs++
		}
		rf.observeContent(&ref, consumer, materialize)
		if err := rf.collect(ctx, id, consumer, materialize); err != nil {
			return err
		}
	}
	occurrence.facts = &facts
	occurrence.object = object
	if rf.loader.locations == nil {
		rf.loader.locations = map[manifest.NamedResource]authoredOccurrence{}
	}
	rf.loader.locations[consumer] = occurrence
	return nil
}

func (rf *readerFinalizer) repositoryInputs(namespace string) []manifest.NamedResource {
	if rf.repositories == nil {
		rf.repositories = map[string][]manifest.NamedResource{}
	}
	ids, known := rf.repositories[namespace]
	if !known {
		for id := range rf.loader.Index.Records {
			if id.Group == manifest.SourceDomain && id.Kind == manifest.KindHelmRepository &&
				id.Namespace == namespace {
				ids = append(ids, id)
			}
		}
		for _, object := range rf.loader.Store.ListObjects(manifest.SourceDomain, manifest.KindHelmRepository) {
			if object.Named().Namespace == namespace {
				ids = append(ids, object.Named())
			}
		}
		slices.SortFunc(ids, manifest.NamedResource.Compare)
		ids = slices.Compact(ids)
		rf.repositories[namespace] = ids
	}
	return ids
}
