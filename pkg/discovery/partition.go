package discovery

import (
	"context"
	"iter"
	"path/filepath"
	"slices"
	"strings"

	"github.com/home-operations/flate/pkg/selection"

	"github.com/home-operations/flate/pkg/loader"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source/safepath"
	"github.com/home-operations/flate/pkg/store"
)

type entryRoot struct {
	Path        string
	Scans       []string
	Occurrences []selection.OccurrenceFacts
	Anchors     []string
	pinned      []manifest.KSClaim
	sources     map[manifest.NamedResource]entryResolution
	views       map[string]*store.SourceArtifact
	parsed      *loader.ParsedInputs
	seeds       []selection.OccurrenceFacts
}

type occurrenceProjection struct {
	path, namespace string
	input           *store.SourceArtifact
}

type entryPlanner struct {
	roots       []entryRoot
	index       int
	queue       []partitionVisit
	head        int
	parsed      *loader.ParsedInputs
	started     bool
	scope       partitionScope
	coordinates map[selection.OccurrenceID]bool
	seen        map[occurrenceKey]bool

	session     *Session
	inputs      map[string]*store.SourceArtifact
	objects     map[occurrenceKey]store.Child
	projections map[occurrenceProjection][]selection.OccurrenceFacts
}

func (d *Session) partitionRoots(ctx context.Context) ([]entryRoot, error) {
	if d.planner == nil {
		packages, err := d.loader.Packages(ctx)
		if err != nil {
			return nil, err
		}
		d.planner = &entryPlanner{
			session:     d,
			inputs:      map[string]*store.SourceArtifact{d.cfg.Input.LocalPath: d.cfg.Input},
			coordinates: map[selection.OccurrenceID]bool{},
			seen:        map[occurrenceKey]bool{},
		}
		for _, pkg := range packages {
			if !pkg.Independent {
				continue
			}
			d.planner.roots = append(
				d.planner.roots,
				entryRoot{Path: pkg.Path, Scans: pkg.Scans, seeds: pkg.Occurrences},
			)
		}
	}
	planner := d.planner
	for planner.index < len(planner.roots) {
		root := &planner.roots[planner.index]
		seeds := root.seeds
		if !planner.started {
			if err := planner.start(ctx, seeds); err != nil {
				return nil, err
			}
			planner.started = true
		}
		if err := planner.reach(ctx, root); err != nil {
			return nil, err
		}
		if planner.parsed == nil {
			planner.parsed = d.loader.ParsedInputs()
		}
		root.parsed = planner.parsed
		planner.index++
		planner.started = false
		root.seeds = nil
	}
	roots := d.unpinnedRoots(planner.roots)
	d.planner = nil
	return roots, nil
}

func (p *entryPlanner) start(ctx context.Context, seeds []selection.OccurrenceFacts) error {
	p.queue = slices.Grow(p.queue[:0], len(seeds))
	p.head = 0
	clear(p.coordinates)
	clear(p.seen)
	p.scope = partitionScope{occurrences: seeds}
	if err := p.prepareScope(ctx, &p.scope); err != nil {
		return err
	}
	for i := range seeds {
		p.queue = append(
			p.queue,
			partitionVisit{occurrence: &seeds[i], scope: &p.scope},
		)
		p.coordinates[seeds[i].Declaration] = true
	}
	return nil
}

func (d *Session) unpinnedRoots(pending []entryRoot) []entryRoot {
	claimed := func(root entryRoot, claims []manifest.KSClaim) bool {
		record := manifest.DiscoveryRecord{
			Path: filepath.Join(d.cfg.Input.LocalPath, root.Path), File: root.Path + "/",
		}
		return d.pinnedWorkingTreeFile(d.cfg.Input.LocalPath, claims, manifest.NamedResource{}, &record)
	}
	slices.SortFunc(pending, func(a, b entryRoot) int { return strings.Compare(a.Path, b.Path) })
	var roots []entryRoot
	for len(pending) != 0 {
		owners := pinnedRootOwners(pending, claimed)
		selected := 0
		for i := range pending {
			independent := true
			for j := range pending {
				if owners[i][j] && !owners[j][i] {
					independent = false
					break
				}
			}
			if independent {
				selected = i
				break
			}
		}
		root := pending[selected]
		roots = append(roots, root)
		pending = slices.DeleteFunc(pending, func(candidate entryRoot) bool {
			return candidate.Path == root.Path || claimed(candidate, root.pinned)
		})
	}
	return roots
}

func pinnedRootOwners(roots []entryRoot, claimed func(entryRoot, []manifest.KSClaim) bool) [][]bool {
	owners := make([][]bool, len(roots))
	for i := range roots {
		owners[i] = make([]bool, len(roots))
		queue := []int{i}
		for len(queue) != 0 {
			current := queue[0]
			queue = queue[1:]
			for j := range roots {
				if !owners[i][j] && claimed(roots[current], roots[j].pinned) {
					owners[i][j] = true
					queue = append(queue, j)
				}
			}
		}
	}
	return owners
}

type partitionVisit struct {
	occurrence *selection.OccurrenceFacts
	scope      *partitionScope
}

type partitionScope struct {
	occurrences []selection.OccurrenceFacts
	parent      *partitionScope
}

func (s *partitionScope) objects() iter.Seq[*selection.OccurrenceFacts] {
	return func(yield func(*selection.OccurrenceFacts) bool) {
		for scope := s; scope != nil; scope = scope.parent {
			for i := range scope.occurrences {
				if !yield(&scope.occurrences[i]) {
					return
				}
			}
		}
	}
}

func occurrenceReferences(occurrence *selection.OccurrenceFacts) iter.Seq[manifest.Reference] {
	return func(yield func(manifest.Reference) bool) {
		for i := range occurrence.Refs {
			ref := occurrence.Refs[i]
			if ref.NamespaceDefault {
				ref.Namespace = occurrence.Identity.Namespace
			}
			if !yield(ref) {
				return
			}
		}
	}
}

func (p *entryPlanner) reach(ctx context.Context, root *entryRoot) error {
	for p.head < len(p.queue) {
		visit := p.queue[p.head]
		if p.seen[occurrenceIdentity(visit.occurrence)] {
			p.queue[p.head] = partitionVisit{}
			p.head++
			continue
		}
		_, _, hasPath := visit.occurrence.SourcePath()
		if visit.occurrence.RequiresBuild || !hasPath && !visit.occurrence.Identity.KindInfo().Runnable {
			root.Occurrences = append(root.Occurrences, *visit.occurrence)
			p.seen[occurrenceIdentity(visit.occurrence)] = true
			p.queue[p.head] = partitionVisit{}
			p.head++
			continue
		}
		object := partitionObject(visit.occurrence)
		application, _ := manifest.ApplicationFor(object)
		references := manifest.References(object, manifest.ReferenceRelations)
		if visit.occurrence.Witness != nil {
			references = occurrenceReferences(visit.occurrence)
		}
		for ref := range references {
			if !ref.IsSource() || !ref.NamedResource.KindInfo().IsSource() || ref.NamedResource == application.Source {
				continue
			}
			if _, sourceErr := p.source(ctx, root, &visit, &ref); sourceErr != nil {
				return sourceErr
			}
		}
		children, err := p.follow(ctx, root, &visit, p.coordinates[visit.occurrence.Declaration])
		if err != nil {
			return err
		}
		root.Occurrences = append(root.Occurrences, *visit.occurrence)
		p.seen[occurrenceIdentity(visit.occurrence)] = true
		p.queue[p.head] = partitionVisit{}
		p.head++
		p.queue = append(p.queue, children...)
	}
	slices.Sort(root.Anchors)
	return nil
}

func (p *entryPlanner) follow(
	ctx context.Context,
	root *entryRoot,
	visit *partitionVisit,
	seed bool,
) ([]partitionVisit, error) {
	occurrence := visit.occurrence
	object := partitionObject(occurrence)
	application, valid := manifest.ApplicationFor(object)
	if !valid {
		return nil, nil
	}
	reference := sourceReference(object, application.Source)
	reference.NamedResource = application.Source
	input, err := p.source(ctx, root, visit, &reference)
	if err != nil {
		return nil, err
	}
	if input == nil {
		return nil, nil
	}

	target := filepath.ToSlash(filepath.Clean(manifest.NormalizeClaimBase(application.Path)))
	absolute := filepath.Join(input.LocalPath, target)
	if !safepath.Contains(input.LocalPath, absolute) {
		return nil, nil
	}
	total := 0
	_, err = p.session.loadAt(ctx, absolute, &total, &sourceLoad{
		work:     p.session.cfg.Work.Source(application.Source).Record(manifest.WorkPreparation, absolute),
		artifact: input,
	})
	if err != nil {
		return nil, err
	}
	pinned := p.session.pinnedWorkingTreeClaims(p.session.cfg.Input.LocalPath, []selection.OccurrenceFacts{*occurrence})
	root.pinned = append(root.pinned, pinned.claims...)
	key := occurrenceProjection{path: absolute, input: input}
	if manifest.KindOf(object).Parent {
		key.namespace = object.EmissionPolicy().Namespace
	}
	children, known := p.projections[key]
	if !known {
		p.parsed = nil
		children, err = p.session.loader.Occurrences(ctx, absolute, key.namespace)
		if err != nil {
			return nil, err
		}
		if p.projections == nil {
			p.projections = map[occurrenceProjection][]selection.OccurrenceFacts{}
		}
		p.projections[key] = children
	}
	local := input == p.session.cfg.Input || input.InputAlias && input.LocalRoot == p.session.cfg.Input.LocalPath
	if local && seed && slices.ContainsFunc(children, func(o selection.OccurrenceFacts) bool {
		return o.Declaration == occurrence.Declaration
	}) && !slices.Contains(root.Anchors, target) {
		root.Anchors = append(root.Anchors, target)
	}
	scope := &partitionScope{occurrences: children, parent: visit.scope}
	if err := p.prepareScope(ctx, scope); err != nil {
		return nil, err
	}
	next := make([]partitionVisit, len(children))
	for i := range children {
		next[i] = partitionVisit{occurrence: &children[i], scope: scope}
	}
	return next, nil
}

func (p *entryPlanner) prepareScope(ctx context.Context, scope *partitionScope) error {
	for i := range scope.occurrences {
		occurrence := &scope.occurrences[i]
		kind := manifest.KindOf(partitionObject(occurrence))
		if !kind.Parent || !kind.Discovery || kind.Structural || len(occurrence.TemplateObjects) != 0 {
			continue
		}
		child, err := p.object(ctx, occurrence)
		if err != nil {
			return err
		}
		if child.Obj != nil {
			facts := p.session.loader.ProducerFacts(child.Obj).Occurrence
			occurrence.Targets, occurrence.TemplateObjects = facts.Targets, facts.TemplateObjects
		}
	}
	return nil
}

func (p *entryPlanner) object(ctx context.Context, occurrence *selection.OccurrenceFacts) (store.Child, error) {
	key := occurrenceIdentity(occurrence)
	key.input = p.inputs[occurrence.Declaration.Source]
	if child, ok := p.objects[key]; ok {
		return child, nil
	}
	child, err := p.session.loader.OccurrenceObject(ctx, occurrence, p.inputs[occurrence.Declaration.Source])
	if err == nil && child.Obj != nil {
		if p.objects == nil {
			p.objects = map[occurrenceKey]store.Child{}
		}
		p.objects[key] = child
		p.parsed = nil
	}
	return child, err
}

func (p *entryPlanner) source(
	ctx context.Context,
	root *entryRoot,
	visit *partitionVisit,
	reference *manifest.Reference,
) (*store.SourceArtifact, error) {
	id := reference.NamedResource
	object := partitionObject(visit.occurrence)
	application, _ := manifest.ApplicationFor(object)
	selected, projected, producers := visit.scope.source(id)
	if selected != nil && selected.RequiresBuild {
		return nil, nil //nolint:nilnil // Unprepared predictions MUST NOT supply source material.
	}

	group := entrySourceGroup{
		scoped: true, required: true, reference: *reference, producers: producers,
		entry: application.Source == id && p.session.admission().appliesEntry(&application),
	}
	for i := range p.session.cfg.Declarations {
		declaration := &p.session.cfg.Declarations[i]
		if declaration.Obj.Named() == id {
			group.object, group.document = declaration.Obj, declaration.Doc
		}
	}
	switch {
	case group.object != nil:
	case selected != nil:
		child, err := p.object(ctx, selected)
		if err != nil {
			return nil, err
		}
		group.object, group.document = child.Obj, child.Doc
	default:
		group.project(projected)
	}
	admission := p.session.admission()
	before := p.session.resolvedSources.snapshot()[id]
	if _, err := p.session.resolveEntrySource(ctx, admission, id, &group); err != nil {
		return nil, err
	}
	binding := p.session.resolvedSources.snapshot()[id]
	if !binding.artifact.SameMaterial(before.artifact) {
		p.parsed = nil
	}
	if binding.artifact != nil {
		p.inputs[binding.artifact.LocalPath] = binding.artifact
	}
	if binding.viewKey == "" && !binding.projected && len(binding.producers) == 0 {
		return p.session.sourceMaterial(id, binding.artifact), nil
	}
	if root.sources == nil {
		root.sources = map[manifest.NamedResource]entryResolution{}
	}
	previous := root.sources[id]
	previous.merge(&binding)
	root.sources[id] = previous
	if binding.viewKey == "" {
		return p.session.sourceMaterial(id, binding.artifact), nil
	}
	if root.views == nil {
		root.views = map[string]*store.SourceArtifact{}
	}
	root.views[binding.viewKey] = binding.artifact
	return p.session.sourceMaterial(id, binding.artifact), nil
}

type occurrenceKey struct {
	declaration   selection.OccurrenceID
	identity      manifest.NamedResource
	object        manifest.BaseManifest
	witness       any
	requiresBuild bool
	input         *store.SourceArtifact
}

func occurrenceIdentity(occurrence *selection.OccurrenceFacts) occurrenceKey {
	return occurrenceKey{
		occurrence.Declaration,
		occurrence.Identity,
		occurrence.Object,
		occurrence.Witness,
		occurrence.RequiresBuild,
		occurrence.Mounted,
	}
}

//nolint:ireturn // Planning borrows the manifest catalog contract across kinds.
func partitionObject(occurrence *selection.OccurrenceFacts) manifest.BaseManifest {
	object := occurrence.Object
	if object == nil {
		object = occurrence.Catalog
	}
	if object != nil && object.Named().Namespace != occurrence.Identity.Namespace {
		object = object.WithNamespace(occurrence.Identity.Namespace)
	}
	return object
}

//nolint:ireturn // Scoped projections retain their manifest types.
func (s *partitionScope) source(id manifest.NamedResource) (
	*selection.OccurrenceFacts,
	manifest.BaseManifest,
	[]occurrenceKey,
) {
	var selected *selection.OccurrenceFacts
	var projected manifest.BaseManifest
	var producers []occurrenceKey
	project := func(object manifest.BaseManifest) bool {
		application, valid := manifest.ApplicationFor(object)
		if !valid || application.Declaration == nil || application.Source != id {
			return false
		}
		if projected == nil {
			projected = application.Declaration
		}
		return true
	}
	for candidate := range s.objects() {
		if candidate.Identity == id {
			selected = candidate
		}
		produced := project(partitionObject(candidate))
		for _, object := range candidate.TemplateObjects {
			produced = project(object) || produced
		}
		targets := candidate.Targets
		if candidate.NeedsCandidates {
			targets = manifest.ProducerTargets(partitionObject(candidate), id)
		}
		produced = produced || slices.ContainsFunc(targets, func(target manifest.ProducerTarget) bool {
			return target.NamedResource == id
		})
		if produced {
			producers = append(producers, occurrenceIdentity(candidate))
		}
	}

	return selected, projected, producers
}
