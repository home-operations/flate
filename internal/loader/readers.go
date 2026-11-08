package loader

import (
	"cmp"
	"context"
	"io/fs"
	"path"
	"path/filepath"
	"slices"
	"strings"

	"github.com/home-operations/flate/internal/chartinput"
	"github.com/home-operations/flate/internal/kustomizeinput"
	"github.com/home-operations/flate/internal/selection"
	"github.com/home-operations/flate/internal/store"
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
	rf.collectEntryInputs()
	rf.collectControls()
	rf.objects = map[manifest.NamedResource]manifest.BaseManifest{}
	rf.sources = map[manifest.NamedResource]graphVisit{}
	rf.charts = map[string][]string{}
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
	charts          map[string][]string
	templateBodies  map[string][][]byte
	templateRefs    map[[2]string]manifest.TemplateProjection
	templateWalks   map[[2]string]bool
	visited         map[manifest.NamedResource]struct{}
	unresolvedPaths map[manifest.NamedResource]bool
}

//nolint:ireturn // Indexed manifests retain their concrete polymorphic types.
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
		//nolint:wrapcheck // The caller preserves cancellation error identity.
		return err
	}
	if _, seen := rf.visited[id]; seen {
		return nil
	}
	rf.visited[id] = struct{}{}
	if rf.loader.documents.compact {
		if occurrence, ok := rf.loader.locations[id]; ok {
			return rf.collectOccurrence(ctx, &occurrence, consumer, materialize)
		}
	}
	for ref := range manifest.References(rf.object(id), manifest.ReferenceRelations) {
		if err := rf.observe(ctx, new(ref), consumer, materialize); err != nil {
			return err
		}
	}
	return nil
}

func (rf *readerFinalizer) collectOccurrence(
	ctx context.Context,
	occurrence *authoredOccurrence,
	consumer manifest.NamedResource,
	materialize bool,
) error {
	if err := rf.projectOccurrence(ctx, occurrence, consumer); err != nil {
		return err
	}
	for ref := range occurrence.references() {
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
		if _, known := rf.loader.roots.lookup(id); known {
			return
		}
		evidence := rf.loader.rootAdmission(r.Path, r.File, kind.Structural).evidence(rf.loader, id, r)
		rf.loader.roots.set(id, &evidence)
		return
	}
	doc := rf.loader.documents.cache[r.Path]
	if r.Authored == (manifest.NamedResource{}) || doc.rootKnown {
		return
	}
	scope := rf.loader.rootAdmission(r.Path, r.File, false)
	doc.rootOwners, doc.rootPeers, doc.rootNative = scope.owners, scope.peers, rf.loader.nativeOperand(r.Path)
	evidence := scope.evidence(rf.loader, id, r)
	doc.rootAdmitted = selection.Root(evidence.view()).Admitted
	doc.rootKnown = true
	rf.loader.documents.cache[r.Path] = doc
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
		for i := range r.Productions {
			p := &r.Productions[i]
			bound, _ := rf.loader.Store.GetArtifact(p.Source, store.ArtifactContent).(*store.SourceArtifact)
			if p.Source != manifest.BootstrapSourceID && input != nil && bound == input && p.Owner != id &&
				(p.File == r.File || p.Path != "" && p.Path == r.Path) {
				p.Owner, p.Declaration = manifest.NamedResource{}, manifest.NamedResource{}
				p.Authored, p.Path, p.File = r.Authored, r.Path, r.File
			}
		}
		r.Productions = slices.DeleteFunc(r.Productions, func(p manifest.Production) bool {
			return p.Owner != (manifest.NamedResource{}) && p.Owner != id &&
				(p.File == r.File || p.Path != "" && p.Path == r.Path)
		})
	}
	if len(r.Productions) == 0 && r.Authored != (manifest.NamedResource{}) {
		r.Productions = []manifest.Production{
			{Authored: r.Authored, Path: r.Path, File: r.File, Source: manifest.BootstrapSourceID},
		}
	}
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
					p.Source == manifest.BootstrapSourceID && p.Path == r.Path && !selection.Root(rf.loader.RootEvidence(id, nil)).Admitted)
		})
	})
	if rf.loader.documents.compact && selection.Root(rf.loader.RootEvidence(id, nil)).Admitted {
		{
			pValues := r.Productions
			for pIndex := range pValues {
				p := pValues[pIndex]
				if p.Owner != (manifest.NamedResource{}) && p.Materializer(id) == (manifest.NamedResource{}) &&
					p.Declaration == (manifest.NamedResource{}) &&
					p.Authored != (manifest.NamedResource{}) {
					p.Owner = manifest.NamedResource{}
					if !slices.Contains(r.Productions, p) {
						r.Productions = append(r.Productions, p)
					}
				}
			}
		}
	}
	slices.SortFunc(r.Productions, func(a, b manifest.Production) int { return a.Owner.Compare(b.Owner) })
}

func (rf *readerFinalizer) setRecordOutput(id manifest.NamedResource, r *manifest.DiscoveryRecord) {
	kind := id.KindInfo()
	if !rf.loader.documents.compact && r.Builder == "" {
		r.Declaration = rf.loader.IndexedObject(id, *r).Obj
	}
	r.StaticOutput = kind.Structural || len(rf.loader.templateProjections[id].Targets) != 0
	if r.Builder == "" && len(rf.loader.locations) != 0 {
		if occurrence, known := rf.loader.catalogOccurrence(id); known {
			r.StaticOutput = !occurrence.facts.opaque(rf.loader.templateProjections[id].Targets)
		}
	}
	r.StaticOutput = r.StaticOutput && !rf.unresolvedPaths[id]
}

func (rf *readerFinalizer) setRecordParent(id manifest.NamedResource, r *manifest.DiscoveryRecord) {
	kind := id.KindInfo()
	if rf.loader.documents.compact && kind.Parent && !selection.Root(rf.loader.roots.get(id).view()).Independent &&
		r.Parent == (manifest.NamedResource{}) &&
		!selection.Root(rf.loader.RootEvidence(id, nil)).Admitted {
		var owner manifest.NamedResource
		multiple := false
		{
			pValues2 := r.Productions
			for pIndex2 := range pValues2 {
				p := pValues2[pIndex2]
				if parent := p.Materializer(id); parent != (manifest.NamedResource{}) {
					multiple = multiple || owner != (manifest.NamedResource{}) && owner != parent
					owner = parent
				}
			}
		}
		if !multiple && owner != id {
			r.Parent = owner
		}
	}
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
	for _, name := range ref.Files {
		clean := path.Clean(name)
		if ref.Subtree {
			var valid bool
			clean, valid = resolveRoot(rf.loader.Store, ref.NamedResource, cmp.Or(name, "."))
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

func (rf *readerFinalizer) chartNames(ctx context.Context, location, chartPath string) ([]string, error) {
	names, seen := rf.charts[location]
	if seen {
		return names, nil
	}
	inputFile := new(rf.loader.documents).input(location)
	info, err := fs.Stat(inputFile.fs, inputFile.name)
	if err == nil {
		if info.IsDir() {
			names = append(names, path.Join(chartPath, ".helmignore"))
			if sub, subErr := fs.Sub(inputFile.fs, inputFile.name); subErr == nil {
				names = append(names, rf.readChartFiles(ctx, sub, chartPath, location)...)
			}
		} else if info.Mode().IsRegular() {
			names = append(names, chartPath)
		}
	}
	if err := ctx.Err(); err != nil {
		//nolint:wrapcheck // The caller preserves cancellation error identity.
		return nil, err
	}
	rf.charts[location] = names
	return names, nil
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
	for _, name := range names {
		v.coordinate = path.Join(v.prefix, name)
		rf.loader.recordReader(v.coordinate, v.localCoordinate(), new(reader))
	}
	namespace := reader.Reader.Namespace
	if producer, ok := rf.object(reader.Reader).(emissionProducer); ok {
		namespace = producer.EmissionPolicy().Namespace
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
	rf.retainTemplateProjection(reader.Reader, &refs)
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
	sources := []manifest.NamedResource{manifest.BootstrapSourceID}
	if len(productions) == 0 {
		return sources
	}
	sources = nil
	for i := range productions {
		source := productions[i].Source
		if source == (manifest.NamedResource{}) {
			source = manifest.BootstrapSourceID
		}
		if !slices.Contains(sources, source) {
			sources = append(sources, source)
		}
	}
	return sources
}

func (rf *readerFinalizer) readChartFiles(ctx context.Context, input fs.FS, chartPath, location string) []string {
	var names []string
	// Reconciliation owns chart loading errors, including policy failures.
	_ = chartinput.Walk(ctx, input, true, func(name string, _ fs.FileInfo, data []byte) error {
		names = append(names, path.Join(chartPath, name))
		if strings.HasPrefix(name, "templates/") || strings.Contains(name, "/templates/") {
			if body, ok := readTemplateBody(ctx, input, name, data); ok {
				rf.templateBodies[location] = append(rf.templateBodies[location], body)
			}
		}
		return nil
	})
	return names
}

func readTemplateBody(_ context.Context, input fs.FS, name string, data []byte) ([]byte, bool) {
	if data != nil {
		return data, true
	}
	data, err := fs.ReadFile(input, name)
	return data, err == nil
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
	for _, root := range rf.loader.scopes.roots {
		id := manifest.NewEntryKustomization(manifest.BootstrapSourceID, root, manifest.NamedResource{}).Named()
		record := rf.loader.Index.Records[id]
		// Exact package inputs MUST remain authoritative after construction is frozen.
		inputs := rf.entryInputs(root)
		slices.Sort(inputs)
		record.ContentRoots = map[manifest.NamedResource][]string{manifest.BootstrapSourceID: slices.Compact(inputs)}
		rf.loader.Index.Records[id] = record
	}
}

func (rf *readerFinalizer) entryInputs(root string) []string {
	var inputs []string
	visits := rf.loader.scopeWalk(new(graphVisit{root: rf.repoRoot, dir: root}))
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

func (rf *readerFinalizer) retainTemplateProjection(
	consumer manifest.NamedResource,
	refs *manifest.TemplateProjection,
) {
	if len(refs.Targets)+len(refs.Objects) != 0 {
		if rf.loader.templateProjections == nil {
			rf.loader.templateProjections = map[manifest.NamedResource]manifest.TemplateProjection{}
		}
		projection := rf.loader.templateProjections[consumer]
		for _, target := range refs.Targets {
			if !slices.ContainsFunc(projection.Targets, func(known manifest.ProducerTarget) bool {
				return known.NamedResource == target.NamedResource
			}) {
				projection.Targets = append(projection.Targets, target)
			}
		}
		projection.Objects = append(projection.Objects, refs.Objects...)
		rf.loader.templateProjections[consumer] = projection
	}
}
