package loader

import (
	"cmp"
	"context"
	"path/filepath"
	"slices"

	"github.com/home-operations/flate/internal/kustomizeinput"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source/safepath"
)

type visitContext struct {
	owner, source manifest.NamedResource
	context       graphContext
	file          string
}

type occurrenceContext struct {
	declaration   occurrenceID
	owner, source manifest.NamedResource
	context       graphContext
	file          string
}

func (c *occurrenceContext) substituted(facts *occurrenceFacts) bool {
	return facts.substituted || manifest.HasEnvsubstReference(c.context.namePrefix) ||
		manifest.HasEnvsubstReference(c.context.nameSuffix)
}

func (l *Loader) contextualDeclaration(file string, authored manifest.NamedResource) bool {
	for key := range l.scopes.contextual {
		occurrence := l.scopes.contextual[key]
		if key.file == file && occurrence.Authored == authored {
			return true
		}
	}
	return false
}

func (l *Loader) contextualParents() map[manifest.NamedResource]manifest.NamedResource {
	var parents map[manifest.NamedResource]manifest.NamedResource
	for key := range l.scopes.contextual {
		occurrence := l.scopes.contextual[key]
		id := occurrence.Identity
		if !id.KindInfo().Parent || occurrence.facts.uncertain {
			continue
		}
		owner, known := parents[id]
		if known && owner != key.owner {
			parents[id] = manifest.NamedResource{}
			continue
		}
		if parents == nil {
			parents = map[manifest.NamedResource]manifest.NamedResource{}
		}
		parents[id] = key.owner
	}
	return parents
}

//nolint:gocognit // Identity, namespace, and census reuse MUST share one occurrence projection.
func (l *Loader) projectOccurrence(
	o *authoredOccurrence,
	visit *graphVisit,
	version string,
) (authoredOccurrence, error) {
	key := occurrenceContext{o.Declaration, visit.owner, visit.source, visit.graphContext, visit.file}
	projected, known := l.scopes.contextual[key]
	cached := projected.facts

	if !known {
		projected = *o
		id, err := l.projectIdentity(o.Authored, visit, version, o.facts.namespaced)
		projected.Identity = id
		if err != nil {
			return projected, err
		}
		uncertain := id.Name == "" || identitySubstituted(id)
		owned := visit.owner != (manifest.NamedResource{})
		if uncertain != projected.facts.uncertain ||
			owned && projected.facts.structural != nil && projected.facts.structural.Named() != id {
			facts := *projected.facts
			facts.uncertain = uncertain
			if owned && facts.structural != nil && !uncertain {
				previous := l.locations[id]
				facts.projectStructure(id, &previous)
			}
			projected.facts = &facts
		}
	}
	previous, found := l.locations[projected.Identity]
	sameCensus := found && previous.facts.census != nil && previous.facts.census == projected.facts.census
	if structural := projected.facts.structural; structural != nil && o.facts.structural != nil {
		namespace := cmp.Or(structural.TargetNamespace, o.facts.structural.TargetNamespace)
		// Namespace prediction MUST survive later visits of the same immutable declaration.
		if namespace == "" && sameCensus && previous.facts.structural != nil {
			namespace = previous.facts.structural.TargetNamespace
		}
		if namespace != projected.facts.policy.Namespace {
			facts := *projected.facts
			if facts.projectNamespace(namespace) {
				target := *structural
				target.TargetNamespace = facts.policy.Namespace
				facts.structural = &target
				facts.catalog = o.facts.catalog
				projected.facts = &facts
			}
		}
	}
	unchanged := sameCensus && sameOccurrenceProjection(&previous, &projected)
	if projected.facts.structural != nil && unchanged &&
		projected.facts.structural != previous.facts.structural &&
		projected.facts.structural.ProjectionCompatible(previous.facts.structural) {
		facts := *projected.facts
		facts.structural = previous.facts.structural
		projected.facts = &facts
	}
	if known && projected.facts != cached {
		l.scopes.contextual[key] = projected
	}
	return projected, nil
}

func (l *Loader) resolveAuthored(ctx context.Context, repoRoot string) error {
	inputs := map[manifest.NamedResource]bool{}
	prediction := map[string]nsResolution{}
	full := false
	for {
		// Expanded applications MUST retain ordinary discovery of their producers.
		for key := range l.scopes.contextual {
			occurrence := l.scopes.contextual[key]
			if occurrence.facts.hasPath && !occurrence.facts.uncertain && key.substituted(occurrence.facts) {
				inputs[occurrence.Identity] = true
			}
		}
		l.refreshIndexParents(repoRoot)
		resolution := substitutionResolution{loader: l, applicationInputs: inputs, full: full}
		if err := resolution.resolveContextual(ctx); err != nil {
			return err
		}
		changed := resolution.contextualChanged
		// Absent bindings MUST retain discovery of possible input producers.
		needsInputs := resolution.needsInputs && !full
		if needsInputs {
			full, changed = true, true
		}
		for key := range l.scopes.contextual {
			_, present := resolution.contextual[key]
			changed = changed || !present
		}
		l.scopes.contextual = resolution.contextual
		if !changed {
			return nil
		}
		construction := l.construction
		l.projectAuthored(repoRoot, prediction)
		l.admit(repoRoot, prediction)
		if !needsInputs && l.construction == construction {
			return nil
		}
	}
}

func (p *substitutionResolution) resolveContextual(
	ctx context.Context,
) error {
	l := p.loader
	for i := range l.scopes.contributions {
		contribution := &l.scopes.contributions[i]
		if !contribution.active || contribution.membershipOnly {
			continue
		}
		for j := range contribution.visits {
			visit := contribution.visits[j]
			readable := visit.readable ||
				l.documents.compact && visit.deferDecode && safepath.Contains(visit.root, visit.file)
			if visit.role != kustomizeinput.Resource || visit.document != nil || !readable {
				continue
			}
			visit.owner, visit.source = contribution.owner, contribution.source
			if err := p.resolveVisit(ctx, &visit); err != nil {
				return err
			}
		}
	}
	return nil
}

func (p *substitutionResolution) resolveVisit(
	ctx context.Context,
	visit *graphVisit,
) error {
	if err := ctx.Err(); err != nil {
		return err //nolint:wrapcheck // Cancellation preserves the caller context error identity.
	}
	key := visitContext{visit.owner, visit.source, visit.graphContext, visit.file}
	if p.visited[key] {
		return nil
	}
	reader := p.loader.documents
	if reader.cache[visit.file].objects == nil {
		reader.restore(visit.file)
	}
	if cached := reader.cache[visit.file]; cached.objects != nil && !p.full && !p.applicationInputs[visit.owner] {
		reader.intent = cached.coverage
	}
	var objects []parsedObject
	var err error
	if reader.compact {
		objects, err = reader.censusFile(ctx, visit.file, manifest.ParseDocOptions{})
	} else {
		objects, err = reader.fileObjects(visit.file, manifest.ParseDocOptions{})
	}
	if err != nil {
		return err
	}
	for i := range objects {
		if err := p.resolveContextualObject(ctx, &objects[i], visit); err != nil {
			return err
		}
	}
	if p.visited == nil {
		p.visited = map[visitContext]bool{}
	}
	p.visited[key] = true
	return nil
}

func (p *substitutionResolution) resolveContextualObject(
	ctx context.Context,
	childValue *parsedObject,
	visit *graphVisit,
) error {
	l := p.loader
	child := *childValue
	if child.fact == nil && child.Obj != nil {
		child, _ = l.authoredOccurrence(visit.file, child.Obj.Named())
	}
	if child.fact == nil || child.outcome.Err != nil {
		return nil //nolint:nilerr // Production finalization MUST report malformed declarations with their resource context.
	}
	o := child.fact
	if !o.facts.indexed() {
		return nil
	}
	key := occurrenceContext{o.Declaration, visit.owner, visit.source, visit.graphContext, visit.file}
	hasSubstitutions := key.substituted(o.facts)
	if !hasSubstitutions && (!l.documents.compact || o.facts.structural == nil) {
		return nil
	}
	projected, err := p.resolveOccurrence(ctx, o, visit, child.version)
	if err != nil {
		return err
	}
	structuralEvidence := visit.deferDecode && !hasSubstitutions
	if !structuralEvidence && !projected.facts.uncertain && sameOccurrenceProjection(&projected, o) {
		return nil
	}
	old, known := l.scopes.contextual[key]
	if known && old.facts.census == projected.facts.census && sameOccurrenceProjection(&old, &projected) {
		projected = old
	} else if hasSubstitutions || projected.facts.uncertain || structuralEvidence {
		p.contextualChanged = true
	}
	if p.contextual == nil {
		p.contextual = map[occurrenceContext]authoredOccurrence{}
	}
	p.contextual[key] = projected
	return nil
}

func sameOccurrenceProjection(o, other *authoredOccurrence) bool {
	if o.Identity != other.Identity || o.facts.uncertain != other.facts.uncertain ||
		o.facts.source != other.facts.source {
		return false
	}
	return o.facts.path == other.facts.path && slices.Equal(o.facts.projection, other.facts.projection)
}

func (l *Loader) retainUnknownOwnerReach() {
	for key := range l.scopes.contextual {
		occurrence := l.scopes.contextual[key]
		if !occurrence.facts.uncertain || key.owner == (manifest.NamedResource{}) || !occurrence.facts.hasPath {
			continue
		}
		l.retainUnknownOccurrenceReach(key.owner, &occurrence)
	}
}

func (l *Loader) retainUnknownOccurrenceReach(ownerID manifest.NamedResource, occurrence *authoredOccurrence) {
	source, path, _ := occurrence.SourcePath()
	if source.Kind == "" || source.Name == "" {
		source = manifest.BootstrapSourceID
	}
	if manifest.HasEnvsubstReference(path) {
		path = ""
	}
	owner := l.Index.Records[ownerID]
	if owner.ContentRoots == nil {
		owner.ContentRoots = map[manifest.NamedResource][]string{}
		if known, ok := l.locations[ownerID]; ok {
			ownSource, _, _ := known.SourcePath()
			if ownSource.Kind == "" || ownSource.Name == "" {
				ownSource = manifest.BootstrapSourceID
			}
			owner.ContentRoots[ownSource] = slices.Clone(known.facts.roots)
		}
	}
	if path != "" {
		path = filepath.ToSlash(filepath.Clean(path))
	}
	owner.ContentRoots[source] = append(owner.ContentRoots[source], path)
	slices.Sort(owner.ContentRoots[source])
	owner.ContentRoots[source] = slices.Compact(owner.ContentRoots[source])
	owner.StaticOutput = false
	l.Index.Records[ownerID] = owner
}

func (p *substitutionResolution) resolveOccurrence(
	ctx context.Context,
	occurrence *authoredOccurrence,
	visit *graphVisit,
	version string,
) (authoredOccurrence, error) {
	o := *occurrence
	id, err := p.loader.projectIdentity(o.Authored, visit, version, o.facts.namespaced)
	if err != nil {
		return o, err
	}
	o.Identity = id
	substitute := !o.facts.substituteDisabled && visit.owner != (manifest.NamedResource{}) &&
		(o.facts.substituted || identitySubstituted(id))
	uncertain := id.Name == "" || identitySubstituted(id)
	if !substitute && (o.facts.structural == nil || o.facts.structural.Named() == id) &&
		uncertain == o.facts.uncertain {
		return o, nil
	}
	facts := *o.facts
	facts.projection = nil
	facts.uncertain = uncertain
	o.facts = &facts
	changed := false
	if substitute {
		changed = p.resolveScalars(ctx, visit.owner, &o)
	}
	if err := ctx.Err(); err != nil {
		return o, err //nolint:wrapcheck // Cancellation preserves the caller context error identity.
	}
	if !changed && o.Identity == occurrence.Identity && facts.source == occurrence.facts.source &&
		facts.uncertain == occurrence.facts.uncertain {
		o.facts = occurrence.facts
		return o, nil
	}
	if facts.structural != nil && !facts.uncertain {
		previous := p.loader.locations[o.Identity]
		facts.projectStructure(o.Identity, &previous)
		facts.roots = slices.Collect(manifest.RenderRoots(facts.structural))
	}
	return o, nil
}

func (f *occurrenceFacts) projectNamespace(namespace string) bool {
	if namespace == "" || manifest.HasEnvsubstReference(namespace) || namespace == f.policy.Namespace {
		return false
	}
	policy := *f.policy
	policy.Namespace = namespace
	f.policy = &policy
	return true
}

func (f *occurrenceFacts) projectStructure(id manifest.NamedResource, previous *authoredOccurrence) {
	structural := *f.structural
	structural.Name, structural.Namespace = id.Name, id.Namespace
	structural.Path = f.path
	if f.policy != nil {
		if f.policy.Namespace == "" {
			f.projectNamespace(structural.TargetNamespace)
		}
		structural.TargetNamespace = f.policy.Namespace
	}
	if structural.SourceRef.Name != "" {
		structural.SourceRef.Name = f.source.Name
		structural.SourceRef.Namespace = f.source.Namespace
	}
	projected := authoredOccurrence{Identity: id, facts: f}
	sameCensus := previous.facts != nil && previous.facts.census != nil && previous.facts.census == f.census
	if sameCensus && sameOccurrenceProjection(previous, &projected) &&
		structural.ProjectionCompatible(previous.facts.structural) {
		f.structural = previous.facts.structural
	} else {
		f.structural = new(structural)
	}
}

func (p *substitutionResolution) resolveScalars(
	ctx context.Context,
	owner manifest.NamedResource,
	occurrence *authoredOccurrence,
) bool {
	facts := occurrence.facts
	changed := false
	facts.uncertain = !p.resolveIdentity(ctx, owner, &occurrence.Identity)
	resolve := func(value string) string {
		if !manifest.HasEnvsubstReference(value) {
			return value
		}
		resolved, known := p.resolveValue(ctx, owner, value)
		if known {
			changed = changed || value != resolved
			value = resolved
		}
		facts.projection = append(facts.projection, value)
		return value
	}
	p.resolveIdentity(ctx, owner, &facts.source)
	facts.path = resolve(facts.path)
	facts.roots = projectStrings(facts.roots, resolve)
	facts.labels = projectLabels(facts.labels, resolve)
	facts.refs = projectReferences(facts.refs, resolve)
	facts.reverse = nil
	for i := range facts.refs {
		ref := &facts.refs[i]
		if ref.AffectedInput() {
			facts.reverse = append(facts.reverse, ref.NamedResource)
		}
	}
	facts.targets = projectTargets(facts.targets, resolve)
	facts.authoredTargets = projectTargets(facts.authoredTargets, resolve)
	if facts.structural != nil {
		structural := *facts.structural
		structural.Components = projectStrings(structural.Components, resolve)
		facts.structural = &structural
	}
	if facts.policy != nil && manifest.HasEnvsubstReference(facts.policy.Namespace) {
		policy := *facts.policy
		policy.Namespace = resolve(policy.Namespace)
		facts.policy = &policy
	}
	templatesChanged := p.resolveTemplates(ctx, owner, facts, resolve)
	return changed || templatesChanged
}

func (p *substitutionResolution) resolveTemplates(
	ctx context.Context,
	owner manifest.NamedResource,
	facts *occurrenceFacts,
	resolve func(string) string,
) bool {
	changed := false
	facts.templateInputs = slices.Clone(facts.templateInputs)
	for i := range facts.templateInputs {
		input := &facts.templateInputs[i]
		if identitySubstituted(input.Reader) {
			previous := input.Reader
			p.resolveIdentity(ctx, owner, &input.Reader)
			changed = changed || input.Reader != previous
			facts.projection = append(facts.projection, input.Reader.Name, input.Reader.Namespace)
		}
		input.Reference = projectReference(&input.Reference, resolve)
	}
	facts.templateObjects = slices.Clone(facts.templateObjects)
	for i, object := range facts.templateObjects {
		identity := object.Named()
		if !identitySubstituted(identity) {
			continue
		}
		if p.resolveIdentity(ctx, owner, &identity) && identity != object.Named() {
			changed = true
			facts.templateObjects[i] = manifest.CatalogObject(object, identity)
		}
		facts.projection = append(facts.projection, identity.Name, identity.Namespace)
	}
	return changed
}
