package discovery

import (
	"context"
	"iter"
	"path/filepath"
	"slices"
	"strings"

	"github.com/home-operations/flate/internal/selection"

	"github.com/home-operations/flate/internal/loader"
	"github.com/home-operations/flate/internal/source/safepath"
	"github.com/home-operations/flate/internal/store"
	"github.com/home-operations/flate/pkg/manifest"
)

type entryRoot struct {
	Path        string
	Scans       []string
	Occurrences []selection.OccurrenceFacts
	Anchors     []string
	pinned      []manifest.KSClaim
	views       map[string]*store.SourceArtifact
	parsed      *loader.ParsedInputs
}

type occurrenceProjection struct {
	path, namespace string
}

type sourceCoordinate struct {
	declaration selection.OccurrenceID
	identity    manifest.NamedResource
	root        string
}

type entryPlanner struct {
	session     *Session
	inputs      map[string]*store.SourceArtifact
	objects     map[occurrenceKey]store.Child
	resolved    map[sourceCoordinate]entryResolution
	projections map[occurrenceProjection][]selection.OccurrenceFacts
}

func (d *Session) partitionRoots(ctx context.Context) ([]entryRoot, error) {
	packages, err := d.loader.Packages(ctx)
	if err != nil {
		return nil, err
	}
	planner := entryPlanner{
		session:     d,
		inputs:      map[string]*store.SourceArtifact{d.cfg.Input.LocalPath: d.cfg.Input},
		objects:     map[occurrenceKey]store.Child{},
		resolved:    map[sourceCoordinate]entryResolution{},
		projections: map[occurrenceProjection][]selection.OccurrenceFacts{},
	}

	var roots []entryRoot
	for _, pkg := range packages {
		if !pkg.Independent {
			continue
		}
		root := entryRoot{
			Path: pkg.Path, Scans: pkg.Scans, views: map[string]*store.SourceArtifact{},
		}
		if err := planner.reach(ctx, &root, pkg.Occurrences); err != nil {
			return nil, err
		}
		// Independent roots can admit different source views at the same path.
		root.parsed = d.loader.ParsedInputs()
		roots = append(roots, root)
	}
	return d.unpinnedRoots(roots), nil
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
	root       string
}

type partitionScope struct {
	occurrences []selection.OccurrenceFacts
	parent      *partitionScope
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

func (p *entryPlanner) reach(ctx context.Context, root *entryRoot, seeds []selection.OccurrenceFacts) error {
	queue := make([]partitionVisit, 0, len(seeds))
	coordinates := map[selection.OccurrenceID]bool{}
	scope := &partitionScope{occurrences: seeds}
	for i := range seeds {
		queue = append(queue, partitionVisit{occurrence: &seeds[i], scope: scope, root: root.Path})
		coordinates[seeds[i].Declaration] = true
	}
	seen := map[occurrenceKey]bool{}
	for len(queue) != 0 {
		visit := queue[0]
		queue = queue[1:]
		if seen[occurrenceIdentity(visit.occurrence)] {
			continue
		}
		seen[occurrenceIdentity(visit.occurrence)] = true
		root.Occurrences = append(root.Occurrences, *visit.occurrence)
		if _, _, hasPath := visit.occurrence.SourcePath(); !hasPath && !visit.occurrence.Identity.KindInfo().Runnable {
			continue
		}
		object := partitionObject(visit.occurrence)
		application, _ := manifest.ApplicationFor(object)
		references := manifest.References(object, manifest.ReferenceRelations)
		if visit.occurrence.Witness != nil {
			references = occurrenceReferences(visit.occurrence)
		}
		for ref := range references {
			if !entrySourceReference(&ref) || ref.NamedResource == application.Source {
				continue
			}
			if _, sourceErr := p.source(ctx, root, &visit, ref.NamedResource); sourceErr != nil {
				return sourceErr
			}
		}
		children, err := p.follow(ctx, root, &visit, coordinates[visit.occurrence.Declaration])
		if err != nil {
			return err
		}
		queue = append(queue, children...)
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
	binding, err := p.source(ctx, root, visit, application.Source)
	if err != nil {
		return nil, err
	}
	input := binding.artifact
	if input == nil {
		return nil, nil
	}

	target := filepath.ToSlash(filepath.Clean(manifest.NormalizeClaimBase(application.Path)))
	absolute := filepath.Join(input.LocalPath, target)
	if !safepath.Contains(input.LocalPath, absolute) {
		return nil, nil
	}
	p.session.cfg.Store.SetArtifact(application.Source, input)
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
	key := occurrenceProjection{path: absolute}
	if producer, ok := object.(interface {
		EmissionPolicy() manifest.EmissionPolicy
	}); ok {
		key.namespace = producer.EmissionPolicy().Namespace
	}
	children, known := p.projections[key]
	if !known {
		children, err = p.session.loader.Occurrences(ctx, absolute, key.namespace)
		if err != nil {
			return nil, err
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
	next := make([]partitionVisit, len(children))
	for i := range children {
		next[i] = partitionVisit{occurrence: &children[i], scope: scope, root: visit.root}
	}
	return next, nil
}

func (p *entryPlanner) object(ctx context.Context, occurrence *selection.OccurrenceFacts) (store.Child, error) {
	if child, ok := p.objects[occurrenceIdentity(occurrence)]; ok {
		return child, nil
	}
	child, err := p.session.loader.OccurrenceObject(ctx, occurrence, p.inputs[occurrence.Declaration.Source])
	if err == nil {
		p.objects[occurrenceIdentity(occurrence)] = child
	}
	return child, err
}

func (p *entryPlanner) source(
	ctx context.Context,
	root *entryRoot,
	visit *partitionVisit,
	id manifest.NamedResource,
) (entryResolution, error) {
	object := partitionObject(visit.occurrence)
	application, _ := manifest.ApplicationFor(object)
	selected := visit.scope.source(id)

	coordinate := sourceCoordinate{identity: id, root: visit.root}
	group := entrySourceGroup{scoped: true, required: true}
	for i := range p.session.cfg.Declarations {
		declaration := &p.session.cfg.Declarations[i]
		if declaration.Obj.Named() == id {
			group.evidence = store.Child{Obj: declaration.Obj, Doc: declaration.Doc, Origin: declaration.Origin}
		}
	}
	if group.evidence.Obj == nil && selected != nil {
		coordinate.declaration = selected.Declaration
		coordinate.root = ""
		child, err := p.object(ctx, selected)
		if err != nil {
			return entryResolution{}, err
		}
		group.evidence = child
	} else if group.evidence.Obj == nil && id == application.Source {
		group.evidence.Obj = application.Declaration
		if application.Declaration != nil {
			coordinate.declaration = visit.occurrence.Declaration
			coordinate.root = ""
		}
	}
	binding, known := p.resolved[coordinate]
	if !known {
		admission := p.session.admission()
		if _, err := p.session.resolveEntrySource(ctx, admission, id, &group); err != nil {
			return entryResolution{}, err
		}
		binding = p.session.resolvedSources[id]
		if binding.artifact != nil {
			p.inputs[binding.artifact.LocalPath] = binding.artifact
		}
		p.resolved[coordinate] = binding
	}
	if binding.viewKey != "" {
		root.views[binding.viewKey] = binding.artifact
	}
	return binding, nil
}

type occurrenceKey struct {
	declaration   selection.OccurrenceID
	identity      manifest.NamedResource
	object        manifest.BaseManifest
	witness       any
	requiresBuild bool
}

func occurrenceIdentity(occurrence *selection.OccurrenceFacts) occurrenceKey {
	return occurrenceKey{
		occurrence.Declaration,
		occurrence.Identity,
		occurrence.Object,
		occurrence.Witness,
		occurrence.RequiresBuild,
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

func (s *partitionScope) source(id manifest.NamedResource) *selection.OccurrenceFacts {
	var selected *selection.OccurrenceFacts
	for scope := s; scope != nil; scope = scope.parent {
		for i := range scope.occurrences {
			candidate := &scope.occurrences[i]
			if candidate.Identity == id {
				selected = candidate
			}
		}
	}

	return selected
}
