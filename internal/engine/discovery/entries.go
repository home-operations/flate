package discovery

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"io/fs"
	"maps"
	"path"
	"path/filepath"
	"slices"

	"github.com/home-operations/flate/internal/core/kustpath"
	"github.com/home-operations/flate/internal/engine/change/inputprojection"
	"github.com/home-operations/flate/internal/engine/selection"

	kustypes "sigs.k8s.io/kustomize/api/types"
	"sigs.k8s.io/yaml"

	"github.com/home-operations/flate/internal/engine/kustomize/kustomizeinput"
	"github.com/home-operations/flate/internal/engine/loader"
	"github.com/home-operations/flate/internal/engine/source/safepath"
	"github.com/home-operations/flate/internal/engine/store"
	"github.com/home-operations/flate/internal/engine/tree"
	"github.com/home-operations/flate/pkg/manifest"
)

// Entry keeps structural identity separate from the path used for admission.
type Entry struct {
	ID          manifest.ClusterID
	Path        string
	Input       *store.SourceArtifact
	prepared    *entryInputs
	aggregate   bool
	partitioned bool
	roots       []int
	target      string
}

type entryInputs struct {
	input             store.SourceArtifact
	parsed            *loader.ParsedInputs
	aliasViews        sourceViews
	sources           map[manifest.NamedResource]entryResolution
	projections       *manifest.ProducerProjections
	publicOperands    []manifest.NamedResource
	packagesToQualify [][]selection.OccurrenceFacts
	generatorPackages []string
	packages          []*plannedPackage
	unknown           bool
}

func (e *Entry) matches(input *store.SourceArtifact, scan string) bool {
	if e.prepared == nil || input == nil || scan != e.Path {
		return false
	}
	prepared := e.prepared
	if input == prepared.sources[manifest.BootstrapSourceID].input && !e.aggregate {
		input = e.Input
	}
	authored := inputprojection.Authored(input)
	return prepared.parsed.Matches(authored) &&
		input.LocalPath == prepared.input.LocalPath && input.LocalRoot == prepared.input.LocalRoot &&
		prepared.sameContent(authored)
}

func (p *entryInputs) sameContent(input *store.SourceArtifact) bool {
	return input.Key == p.input.Key && input.Content == p.input.Content &&
		input.Revision == p.input.Revision && input.Digest == p.input.Digest
}

// Plan requires an isolated session and resumes after PendingSources are ready.
func (d *Session) Plan(ctx context.Context) ([]Entry, error) {
	d.pendingSources = d.pendingSources[:0]
	switch err := d.loadPlan(ctx); {
	case errors.Is(err, errPendingSources):
		return nil, nil
	case err != nil:
		return nil, err
	}
	l := d.loader
	input := d.cfg.Input
	roots, err := d.clusterRoots(ctx)
	switch {
	case errors.Is(err, errPendingSources):
		return nil, nil
	case err != nil:
		return nil, err
	}
	entries := clusterEntries(roots, input, d.cfg.Path)
	for i := range entries {
		if !entries[i].aggregate {
			continue
		}
		relative, relErr := filepath.Rel(input.LocalPath, d.cfg.Path)
		if relErr != nil {
			return nil, fmt.Errorf("entry path: %w", relErr)
		}
		entry := aggregateEntry{requested: d.cfg.Path, directory: relative, authored: d.hasEntryBuilder()}
		entries[i].Input, err = aggregateInput(input, entry, func() ([]loader.PackageEvidence, error) {
			return d.packages, nil
		})
		if err != nil {
			return nil, err
		}
		entries[i].aggregate = entries[i].Input != input
	}
	if err := l.PersistInputs(ctx); err != nil {
		return nil, err
	}
	for i := range entries {
		if err := d.prepareEntry(ctx, &entries[i], roots); err != nil {
			return nil, err
		}
	}
	return entries, nil
}

func (p *entryInputs) merge(root *clusterRoot) {
	if root.packageBuild != nil {
		p.packages = append(p.packages, root.packageBuild)
	}
	p.parsed = root.parsed
	if p.aliasViews == nil {
		p.aliasViews = maps.Clone(root.views)
	} else {
		maps.Copy(p.aliasViews, root.views)
	}
	if p.sources == nil {
		p.sources = make(map[manifest.NamedResource]entryResolution, len(root.sources))
	}
	for id := range root.sources {
		resolution := root.sources[id]
		previous := p.sources[id]
		previous.merge(&resolution)
		p.sources[id] = previous
	}
}

func (d *Session) prepareEntry(ctx context.Context, entry *Entry, roots []clusterRoot) error {
	input := d.cfg.Input
	prepared := &entryInputs{input: *input, projections: d.cfg.Projections}
	if entry.aggregate {
		prepared.unknown = aggregateRequiresBuild(entry, d.packages)
	}
	relative, err := filepath.Rel(input.LocalPath, cmp.Or(entry.target, entry.Path))
	if err != nil {
		return fmt.Errorf("entry path: %w", err)
	}
	relative = filepath.ToSlash(relative)
	scope := prepared.collectEntryOperands(entry, roots, relative, d.cfg.Selection.Changed)
	if d.cfg.Trees != nil {
		prepared.generatorPackages, err = d.plannedGeneratorPackages(ctx, entry, roots, relative)
		if err != nil {
			return err
		}
	}
	for _, id := range slices.SortedFunc(maps.Keys(prepared.sources), manifest.NamedResource.Compare) {
		resolution := prepared.sources[id]
		_, producers := scope.source(id)
		if len(producers) == 0 {
			continue
		}
		object := resolution.object
		if resolution.synthetic {
			object = nil
		}
		resolved, resolveErr := d.admission().resolve(ctx, id, &sourceRequest{
			object: object, producers: producers, scoped: true,
			entry: resolution.entry, projected: resolution.projected,
		})
		if resolveErr != nil {
			return resolveErr
		}
		resolved.entryScoped = resolution.entryScoped
		prepared.sources[id] = resolved
	}
	if prepared.sources == nil {
		prepared.sources = map[manifest.NamedResource]entryResolution{}
	}
	bootstrap, bound := prepared.sources[manifest.BootstrapSourceID]
	if !bound {
		bootstrap = d.resolvedSources.snapshot()[manifest.BootstrapSourceID]
	}
	if entry.aggregate &&
		(!bound || !bootstrap.synthetic && bootstrap.artifact == inputprojection.Authored(entry.Input)) {
		bootstrap.input, bootstrap.artifact = entry.Input, entry.Input
		bootstrap.viewKey = sourceViewKey{}
	}
	prepared.sources[manifest.BootstrapSourceID] = bootstrap
	if prepared.parsed == nil {
		prepared.parsed = d.loader.ParsedInputs()
	}
	entry.prepared = prepared
	return nil
}

func (d *Session) plannedGeneratorPackages(
	ctx context.Context, entry *Entry, roots []clusterRoot, relative string,
) ([]string, error) {
	input := d.cfg.Input
	packages, err := d.loader.EnclosingPackages(
		ctx, input, relative, manifest.NamedResource.CanDeclareGeneratedEntry,
	)
	if err != nil {
		return packages, err
	}
	for _, index := range entry.roots {
		root := &roots[index]
		pkg := root.Path
		if entry.target == "" && (root.packageBuild == nil || len(root.qualifying) == 0 ||
			!slices.ContainsFunc(root.seeds, func(occurrence selection.OccurrenceFacts) bool {
				return generatesEntry(&occurrence)
			})) {
			continue
		}
		if pkg == relative || slices.Contains(packages, pkg) {
			continue
		}
		if nativeEntry(input.FS, pkg) != "" {
			packages = slices.Insert(packages, 0, pkg)
		}
	}
	return packages, nil
}

func (p *entryInputs) collectEntryOperands(
	entry *Entry, roots []clusterRoot, relative string, changed bool,
) *rootScope {
	var scope *rootScope
	var preferred, remaining []selection.OccurrenceFacts
	for _, j := range entry.roots {
		root := &roots[j]
		scope = &rootScope{occurrences: root.Occurrences, parent: scope}
		p.merge(root)
		if entry.aggregate {
			switch root.qualificationPriority(changed) {
			case qualificationHint:
				preferred = append(preferred, root.seeds...)
			case qualificationOpaque:
				remaining = append(remaining, root.seeds...)
			case qualificationIneligible:
			}
			continue
		}
		if !safepath.Contains(relative, root.Path) {
			continue
		}
		for i := range root.seeds {
			seed := &root.seeds[i]
			if !seed.RequiresBuild && seed.Identity.KindInfo().Parent {
				p.publicOperands = append(p.publicOperands, seed.Identity)
			}
		}
	}
	for _, operands := range [][]selection.OccurrenceFacts{preferred, remaining} {
		if len(operands) > 0 {
			p.packagesToQualify = append(p.packagesToQualify, operands)
		}
	}
	return scope
}

type qualificationPriority uint8

const (
	qualificationIneligible qualificationPriority = iota
	qualificationOpaque
	qualificationHint
)

// Producer hints change preparation order. Coverage requires accepted output.
func (r *clusterRoot) qualificationPriority(changed bool) qualificationPriority {
	priority := qualificationIneligible
	for i := range r.Occurrences {
		occurrence := &r.Occurrences[i]
		kind := occurrence.Identity.KindInfo()
		if !kind.Parent {
			continue
		}
		if kind.Structural {
			source, _, applies := occurrence.SourcePath()
			// Paired content equality does not settle whether a produced source carries an entry producer.
			if changed && applies && len(r.sources[source].producers) != 0 {
				priority = qualificationOpaque
			}
			continue
		}
		priority = qualificationOpaque
		if obj := occurrenceObject(occurrence); obj != nil {
			if _, hint := manifest.ProjectProducer(obj, func(func([]byte) bool) {}); hint {
				return qualificationHint
			}
		}
	}
	return priority
}

func (d *Session) loadPlan(ctx context.Context) error {
	if d.planLoaded {
		return nil
	}
	if _, err := d.resolveEntrySources(ctx, false); err != nil {
		return err
	}
	l := d.loader
	l.SourceRoot = d.cfg.Input.LocalPath
	l.IgnoreFile = d.cfg.KRMIgnoreFile
	if _, err := l.Load(
		ctx,
		d.cfg.Path,
		manifest.NamedResource{},
		d.cfg.Work.Record(manifest.WorkPreparation, d.cfg.Path),
	); err != nil {
		return err
	}
	l.IgnoreFile = ""
	l.PreferExisting = true
	d.planLoaded = true
	return nil
}

func clusterEntries(roots []clusterRoot, input *store.SourceArtifact, scan string) []Entry {
	owned := ownedUnanchoredRoots(roots)
	entries := map[string]Entry{}
	for i := range roots {
		root := &roots[i]
		if len(root.qualifying) == 0 || owned[i] {
			continue
		}
		anchor := root.clusterAnchor(input.LocalPath, scan)
		coordinate := anchor.target
		entry := entries[coordinate]
		entry.ID = manifest.ClusterID(coordinate)
		entry.Path = filepath.Join(input.LocalPath, filepath.FromSlash(coordinate))
		entry.Input = input
		if anchor.source != (manifest.NamedResource{}) {
			entry.target = entry.Path
		}
		entry.roots = append(entry.roots, i)
		entries[coordinate] = entry
	}
	if len(entries) == 0 {
		coordinate, _ := filepath.Rel(input.LocalPath, scan)
		members := make([]int, len(roots))
		for i := range members {
			members[i] = i
		}
		return []Entry{{
			ID: manifest.ClusterID(filepath.ToSlash(coordinate)), Path: scan, Input: input,
			aggregate: true, roots: members,
		}}
	}
	result := make([]Entry, 0, len(entries))
	for _, coordinate := range slices.Sorted(maps.Keys(entries)) {
		entry := entries[coordinate]
		if len(entries) == 1 {
			// An explicit ancestor retains its own output inventory.
			if scan != input.LocalPath && scan != entry.Path {
				entry.target = ""
			}
			entry.Path = scan
		}
		entry.partitioned = entry.Path != scan
		result = append(result, entry)
	}
	return result
}

func (r *clusterRoot) partitionAnchors(input *store.SourceArtifact) []clusterRoot {
	type anchorOwner struct {
		index, length int
	}
	roots := make([]clusterRoot, len(r.anchors))
	prefixes := make([]string, len(r.anchors))
	best := make(map[string]anchorOwner, len(r.anchors))
	for i, anchor := range r.anchors {
		roots[i].Path = anchor.target
		dir := filepath.Join(input.LocalPath, anchor.target)
		prefix := kustpath.ClaimPrefix(kustpath.HostKey(dir))
		prefixes[i] = prefix
		if len(dir) > best[prefix].length {
			best[prefix] = anchorOwner{index: i, length: len(dir)}
		}
	}
	claims, _, owners := loader.NewPackageClaimIndex(len(roots), func(i int) string { return roots[i].Path },
		func(add func(int, string)) {
			for i, prefix := range prefixes {
				if best[prefix].index == i {
					add(i, prefix)
				}
			}
		})
	for i := range r.seeds {
		seed := &r.seeds[i]
		file := kustpath.HostKey(filepath.Join(seed.Declaration.Source, seed.Declaration.File))
		for group := range claims.ClaimOwners(file, manifest.NamedResource{}, manifest.NamedResource{}, nil) {
			owner := &roots[owners[group[0]]]
			owner.seeds = append(owner.seeds, *seed)
			break
		}
	}
	return roots
}

func (r *clusterRoot) clusterAnchor(inputRoot, scan string) bootstrapAnchor {
	if len(r.anchors) == 1 {
		anchor := r.anchors[0]
		absolute := filepath.Join(inputRoot, filepath.FromSlash(anchor.target))
		if safepath.Contains(scan, absolute) {
			return anchor
		}
	}
	return bootstrapAnchor{target: r.Path}
}

func ownedUnanchoredRoots(roots []clusterRoot) []bool {
	// Source references and predictions cannot establish package ownership.
	owners := map[selection.OccurrenceID]int{}
	for i := range roots {
		root := &roots[i]
		if len(root.qualifying) == 0 {
			continue
		}
		for declaration := range root.reached {
			if _, shared := owners[declaration]; shared {
				owners[declaration] = -1
			} else {
				owners[declaration] = i
			}
		}
	}
	owned := make([]bool, len(roots))
	for i := range roots {
		root := &roots[i]
		if len(root.qualifying) == 0 || len(root.anchors) != 0 {
			continue
		}
		owned[i] = !slices.ContainsFunc(root.qualifying, func(declaration selection.OccurrenceID) bool {
			owner, reached := owners[declaration]
			return !reached || owner == i
		})
	}
	return owned
}

type aggregateEntry struct {
	requested, directory string
	authored             bool
}

func aggregateInput(
	input *store.SourceArtifact, entry aggregateEntry, packages func() ([]loader.PackageEvidence, error),
) (*store.SourceArtifact, error) {
	if entry.authored {
		return input, nil
	}
	evidence, err := packages()
	if err != nil {
		return nil, err
	}
	directory := filepath.ToSlash(entry.directory)
	resources := aggregateResources(input, entry, evidence)
	if len(resources) == 0 {
		return input, nil
	}
	document := kustypes.Kustomization{
		APIVersion: kustypes.KustomizationVersion, Kind: kustypes.KustomizationKind,
		Resources: resources,
	}
	data, err := yaml.Marshal(document)
	if err != nil {
		return nil, manifest.NewError(manifest.ErrInput, fmt.Errorf("encode entry package: %w", err), "")
	}
	const fileMode = 0o644
	overlay := tree.NewBuilder(input.FS)
	if err = overlay.AddFile(
		path.Join(directory, manifest.KustomizeBuilderFilenames[0]),
		data,
		fileMode,
	); err != nil {
		return nil, manifest.NewError(manifest.ErrInput, fmt.Errorf("construct entry package: %w", err), "")
	}
	content, err := overlay.Build()
	if err != nil {
		return nil, manifest.NewError(manifest.ErrInput, fmt.Errorf("construct entry package: %w", err), "")
	}
	projected := input.WithSourceView(&inputprojection.Overlay{Tree: content, Authored: input, Resources: resources})
	projected.LocalRoot = input.OriginPath(input.LocalPath)
	projected.Content, projected.Digest, projected.ContentFingerprint = "", "", nil
	return projected, nil
}

func aggregateRequiresBuild(entry *Entry, packages []loader.PackageEvidence) bool {
	for i := range packages {
		pkg := &packages[i]
		if !pkg.Independent || !safepath.Contains(entry.Path, filepath.Join(entry.Input.LocalPath, pkg.Path)) {
			continue
		}
		for j := range pkg.Occurrences {
			if pkg.Occurrences[j].RequiresBuild {
				return true
			}
		}
	}
	return false
}

func aggregateResources(
	input *store.SourceArtifact,
	entry aggregateEntry,
	packages []loader.PackageEvidence,
) []string {
	knownDirectory := filepath.Join(input.LocalPath, entry.directory)
	requested := entry.requested
	var resources []string
	for i := range packages {
		pkg := &packages[i]
		absolute := filepath.Join(input.LocalPath, filepath.FromSlash(pkg.Path))
		if !pkg.Independent || !safepath.Contains(requested, absolute) {
			continue
		}
		if kustpath.RelateHost(knownDirectory, absolute) != kustpath.HostEqual &&
			nativeEntry(input.FS, pkg.Path) != "" {
			resource, _ := filepath.Rel(requested, absolute)
			resources = append(resources, filepath.ToSlash(resource))
			continue
		}
		for j := range pkg.Occurrences {
			occurrence := &pkg.Occurrences[j]
			file := filepath.Join(occurrence.Declaration.Source, filepath.FromSlash(occurrence.Declaration.File))
			if !safepath.Contains(requested, file) {
				continue
			}
			resource, _ := filepath.Rel(requested, file)
			resources = append(resources, filepath.ToSlash(resource))
		}
	}
	slices.Sort(resources)
	resources = slices.Compact(resources)
	return resources
}

type entryDocuments map[manifest.NamedResource]struct{}

func (a *Admission) requestedDocuments(
	ctx context.Context, relative string, work manifest.WorkScope,
) (*entryDocuments, error) {
	if a.documents != nil || a.trees == nil {
		return a.documents, nil
	}
	owner := manifest.NewEntryKustomization(manifest.BootstrapSourceID, relative, manifest.NamedResource{})
	documents, children, err := a.requestedInventory(ctx, owner)
	if err != nil {
		return nil, err
	}
	for i := range children {
		child := &children[i]
		_, applies := manifest.ApplicationFor(child.Obj)
		if !applies || manifest.KindOf(child.Obj).Runnable {
			continue
		}
		generated, err := a.prepareGeneration(child.Obj, work)
		if err != nil {
			return nil, err
		}
		for j := range generated.Visible {
			documents[manifest.DocIdentity(generated.Visible[j].Document)] = struct{}{}
		}
	}
	a.documents = &documents
	return a.documents, nil
}

func (a *Admission) requestedInventory(
	ctx context.Context, owner *manifest.Kustomization,
) (entryDocuments, []store.Child, error) {
	input := a.Input()
	if frame := a.preparedInventory(owner, input); frame != nil {
		return inventoryDocuments(frame.children, frame.failedDocuments), frame.children, frame.inventoryError()
	}
	entry := aggregateEntry{
		requested: a.path, directory: owner.Path, authored: nativeEntry(input.FS, owner.Path) != "",
	}
	input, err := aggregateInput(input, entry, func() ([]loader.PackageEvidence, error) {
		return a.loader.Packages(ctx)
	})
	if err != nil {
		return nil, nil, err
	}
	frame, err := a.packageInventory(ctx, owner, input)
	if err != nil {
		return nil, nil, err
	}
	return inventoryDocuments(frame.children, frame.failedDocuments), frame.children, nil
}

func inventoryDocuments(children []store.Child, failed []manifest.DocOccurrence) entryDocuments {
	documents := make(entryDocuments, len(children)+len(failed))
	for i := range children {
		documents[manifest.DocIdentity(children[i].Doc)] = struct{}{}
	}
	// The accepted producer can repair a document without changing its identity.
	for i := range failed {
		id := manifest.DocIdentity(failed[i].Document)
		if id.Kind != "" && id.Name != "" {
			documents[id] = struct{}{}
		}
	}
	return documents
}

func nativeEntry(input fs.FS, dir string) string {
	for file := range kustomizeinput.Builders(dir, func(file string) bool {
		return kustomizeinput.BuilderFile(fs.Stat(input, filepath.ToSlash(file)))
	}) {
		return file
	}
	return ""
}

func (d *Session) beginRootPlan(ctx context.Context) error {
	packages, err := d.loader.Packages(ctx)
	if err != nil {
		return err
	}
	if _, err := filepath.Rel(d.cfg.Input.LocalPath, d.cfg.Path); err != nil {
		return fmt.Errorf("entry path: %w", err)
	}
	d.packages = slices.Clone(packages)
	d.planner = &entryPlanner{
		directoryInventory: !d.hasEntryBuilder(),
		session:            d,
		inputs:             map[string]*store.SourceArtifact{d.cfg.Input.LocalPath: d.cfg.Input},
		coordinates:        map[selection.OccurrenceID]bool{},
		seen:               map[occurrenceKey]bool{},
	}
	for _, pkg := range packages {
		for i := range pkg.Occurrences {
			occurrence := pkg.Occurrences[i]
			projected := occurrenceObject(&occurrence)
			if occurrence.Object == nil {
				occurrence.Catalog = projected
			} else {
				occurrence.Object = projected
			}
			d.planner.catalog.occurrences = append(d.planner.catalog.occurrences, occurrence)
		}
		if !pkg.Independent || !slices.ContainsFunc(pkg.Occurrences, func(o selection.OccurrenceFacts) bool {
			_, _, applies := o.SourcePath()
			return applies || o.Identity.KindInfo().Runnable
		}) {
			continue
		}
		d.planner.roots = append(
			d.planner.roots,
			clusterRoot{Path: pkg.Path, seeds: pkg.Occurrences},
		)
	}
	return nil
}

func (d *Session) requiresEntry() bool {
	if d.loader == nil || d.loader.Options.Session.Snapshot().Output(manifest.NamedResource{}) {
		return d.hasEntryOperands() || d.hasEntryBuilder()
	}
	if d.entryDemand || d.entryReceiptRequest != nil {
		return true
	}
	return d.aggregateRequiresBuild && d.hasEntryBuilder()
}

func (d *Session) hasEntryBuilder() bool {
	input := d.cfg.Input
	if input == nil {
		return false
	}
	if file, known := d.cachedEntryBuilder(input, d.cfg.Path); known {
		return file != ""
	}
	d.entryBuilderInput, d.entryBuilderPath, d.entryBuilder = input, d.cfg.Path, ""
	relative, err := filepath.Rel(input.LocalPath, d.cfg.Path)
	if err == nil {
		d.entryBuilder = nativeEntry(input.FS, relative)
	}
	return d.entryBuilder != ""
}

func (d *Session) cachedEntryBuilder(input *store.SourceArtifact, absolute string) (string, bool) {
	if d != nil && d.entryBuilderInput != nil && d.entryBuilderPath == absolute &&
		input.SameMaterial(d.entryBuilderInput, store.ArtifactContent|store.ArtifactRevision|store.ArtifactOrigin) {
		return d.entryBuilder, true
	}
	return "", false
}
