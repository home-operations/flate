package loader

import (
	"iter"
	"log/slog"
	"maps"
	"path/filepath"
	"slices"

	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

type materializerDemand struct {
	target, consumer   manifest.NamedResource
	source             string
	application        string
	ordering, required bool
}

func (c *admissionClosure) materializerCandidates(ordering bool) [2][]Occurrence {
	var candidates [2][]Occurrence
	objects := c.loader.Store.Objects()
	previous := c.loader.producerFacts.Load()
	var next map[manifest.BaseManifest]producerFact
	project := func(object manifest.BaseManifest) Occurrence {
		cached, known := next[object]
		if !known && previous != nil {
			cached, known = (*previous)[object]
		}
		if known {
			return Occurrence{Identity: cached.identity, Authored: object.Named(), facts: cached.facts}
		}
		occurrence := compactOccurrence(object, manifest.ProjectProducer(object))
		if next == nil {
			if previous == nil {
				capacity := c.producerFactCapacity(objects, isMaterializerObject)
				next = make(map[manifest.BaseManifest]producerFact, capacity)
			} else {
				next = maps.Clone(*previous)
			}
		}
		next[object] = producerFact{identity: occurrence.Identity, facts: occurrence.facts}
		return occurrence
	}
	c.collectIndexedMaterializers(&candidates, project)
	c.collectObjectMaterializers(&candidates, objects, ordering, project)
	c.publishProducerFacts(&previous, next)
	return candidates
}

func (c *admissionClosure) materializers(
	demandValue *materializerDemand,
	consumer manifest.NamedResource,
	snapshot [2][]Occurrence,
) bool {
	demand := *demandValue

	demand.consumer = consumer
	if pending, searched := c.searched[demand]; searched {
		return pending
	}
	if c.searched == nil {
		c.searched = map[materializerDemand]bool{}
	}
	c.searched[demand] = false
	entry, matches := c.materializerMatcher(&demand)
	if demand.application != "" && c.materializerApplicationKnown(entry, matches) {
		return false
	}
	if c.materializerOwnerPending(consumer, &demand, entry) {
		return true
	}
	target := demand.target
	if demand.application == "" && c.objects.ReferenceFacts(target, c.loader.Index, consumer).Data != nil {
		return false
	}

	candidates := func(yield func(manifest.NamedResource, Occurrence) bool) {
		c.yieldMaterializerCandidates(&demand, snapshot, yield)
	}
	pending := false
	var opaque []Occurrence
	//nolint:gocritic // The iterator supplies immutable value snapshots.
	for id, occurrence := range candidates {
		facts := c.candidateMaterializerFacts(&demand, consumer, id, &occurrence)
		if facts == nil {
			continue
		}
		match := c.materializerTargetsMatch(id, facts, &demand, matches)
		if !match {
			if c.opaqueMaterializerPending(&demand, id, facts, target) {
				opaque = append(opaque, occurrence)
			}
			continue
		}
		output2 := c.materializerOutputMatches(id, matches)
		if !output2.include {
			continue
		}
		pending = pending || !output2.complete
		c.add(id, admissionRole{needed: true, dirty: c.states[id].changed})
	}
	if !pending {
		pending = c.admitOpaqueMaterializers(consumer, target, opaque)
	}
	c.searched[demand] = pending
	return pending
}

// A named input borrows the scratch slot until the next call.
func (c *admissionClosure) matchingInputs(
	refValue *manifest.Reference,
	reverse bool,
) ([]manifest.NamedResource, error) {
	ref := *refValue
	if ref.Name != "" {
		c.inputScratch[0] = ref.NamedResource
		return c.inputScratch[:], nil
	}
	if ref.Selector == nil {
		return nil, nil
	}
	matcher, err := ref.Matcher(ref.Selector)
	if err != nil {
		return nil, err
	}
	var inputs []manifest.NamedResource
	target := ref.NamedResource
	matches := func(source, id manifest.NamedResource) {
		if id.Group != target.Group || id.Kind != target.Kind || id.Namespace != target.Namespace ||
			slices.Contains(inputs, id) {
			return
		}
		matched := c.selectorInputMatches(source, id, new(matcher), reverse)
		if matched {
			inputs = append(inputs, id)
		}
	}
	for _, object := range c.objects.ListObjects(ref.Group, ref.Kind) {
		id := object.Named()
		matches(c.source(id), id)
	}
	candidates := func(yield func(manifest.NamedResource) bool) {
		c.yieldInputCandidates(yield)
	}
	if c.loader.frozen {
		candidates = slices.Values(c.loader.inputObjects[ref.NamedResource])
	}
	for id := range candidates {
		matches(c.source(id), id)
	}
	if reverse && c.loader.sideKnown {
		c.matchComparedInputs(new(ref), matches)
	}
	if c.incoming.Obj != nil {
		id := c.incoming.Obj.Named()
		matches(c.source(id), id)
	}
	slices.SortFunc(inputs, manifest.NamedResource.Compare)
	return inputs, nil
}

func (c *admissionClosure) producerFactCapacity(
	objects []manifest.BaseManifest,
	isProducer func(manifest.BaseManifest) bool,
) int {
	capacity := 0
	for _, object := range objects {
		if isProducer(object) {
			capacity++
		}
	}
	for _, child := range c.loader.candidates {
		if isProducer(child.Obj) {
			capacity++
		}
	}
	return capacity
}

func (c *admissionClosure) publishProducerFacts(
	previous **map[manifest.BaseManifest]producerFact,
	next map[manifest.BaseManifest]producerFact,
) {
	if next != nil {
		for !c.loader.producerFacts.CompareAndSwap(*previous, &next) {
			*previous = c.loader.producerFacts.Load()
			if *previous != nil {
				maps.Copy(next, **previous)
			}
		}
	}
}

func (c *admissionClosure) collectIndexedMaterializers(
	candidates *[2][]Occurrence,
	project func(manifest.BaseManifest) Occurrence,
) {
	//nolint:gocritic // Map iteration supplies value snapshots without an additional lookup.
	for _, occurrence := range c.loader.locations {
		facts := occurrence.facts
		if facts == nil || facts.policy == nil && len(facts.targets) == 0 && !facts.needsCandidates ||
			facts.hasPath && facts.policy != nil && len(facts.roots) == 0 {
			continue
		}
		candidates[0] = append(candidates[0], occurrence)
	}
	for id, child := range c.loader.candidates {
		if _, located := c.loader.locations[id]; located || c.loader.Index.Records[id].Builder == "" {
			continue
		}
		if !isMaterializerObject(child.Obj) {
			continue
		}
		occurrence := project(child.Obj)
		if occurrence.facts.hasPath && occurrence.facts.policy != nil && len(occurrence.facts.roots) == 0 {
			continue
		}
		occurrence.Declaration.Source = c.declarationSource(id)
		candidates[0] = append(candidates[0], occurrence)
	}
}

func (c *admissionClosure) collectObjectMaterializers(
	candidates *[2][]Occurrence,
	objects []manifest.BaseManifest,
	ordering bool,
	project func(manifest.BaseManifest) Occurrence,
) {
	for _, object := range objects {
		id := object.Named()
		if c.skipIndexedMaterializer(id, ordering) {
			continue
		}
		if _, _, pathBearing := object.SourcePath(); !pathBearing && !ordering {
			continue
		}
		object = c.object(id)
		if object == nil {
			continue
		}
		if !isMaterializerObject(object) {
			continue
		}
		occurrence := project(object)
		if occurrence.facts.hasPath && occurrence.facts.policy != nil && len(occurrence.facts.roots) == 0 {
			continue
		}
		if !occurrence.facts.hasPath {
			occurrence.Declaration.Source = c.declarationSource(id)
		}
		candidates[1] = append(candidates[1], occurrence)
	}
}

func isMaterializerObject(object manifest.BaseManifest) bool {
	_, ok := object.(interface {
		EmissionPolicy() manifest.EmissionPolicy
	})
	return ok
}

func (c *admissionClosure) prioritizeOpaqueMaterializers(
	consumer manifest.NamedResource,
	priority map[manifest.NamedResource]bool,
) {
	seen := map[manifest.NamedResource]bool{}
	queue := []manifest.NamedResource{consumer}
	for len(queue) != 0 {
		id := queue[0]
		queue = queue[1:]
		if id == (manifest.NamedResource{}) || seen[id] {
			continue
		}
		seen[id] = true
		record := c.record(id)
		queue = append(queue, record.Parent, record.BuilderOwner)
		{
			productionValues := c.productions(id)
			for productionIndex := range productionValues {
				production := productionValues[productionIndex]
				queue = append(queue, production.Materializer(id))
			}
		}
		for ref := range c.references(id, c.object(id)) {
			if ref.Ordering {
				priority[ref.NamedResource] = true
				queue = append(queue, ref.NamedResource)
			}
		}
	}
}

func (c *admissionClosure) materializerTargetsMatch(
	id manifest.NamedResource,
	facts *occurrenceFacts,
	demand *materializerDemand,
	matches materializerMatch,
) bool {
	target := demand.target
	match := c.coversDeclaration(facts.catalog, target)
	if demand.application != "" {
		applies := func(object manifest.BaseManifest) bool {
			if object.Named().Namespace == "" {
				object = object.WithNamespace(id.Namespace)
			}
			return matches(object.Named(), object, nil)
		}
		match = match || slices.ContainsFunc(facts.templateObjects, applies) ||
			slices.ContainsFunc(c.loader.templateProjections[id].Objects, applies)
	}
	targets := facts.targets
	if extra := c.loader.templateProjections[id].Targets; len(extra) != 0 {
		targets = append(slices.Clone(targets), extra...)
	}
	if facts.needsCandidates {
		targets = append(
			slices.Clone(targets),
			manifest.ProducerTargets(facts.catalog.WithNamespace(id.Namespace), target)...)
	}
	for _, produced := range targets {
		ref := produced.NamedResource
		if (target.Kind == "" || ref.Group == target.Group && ref.Kind == target.Kind) &&
			(target.Name == "" || ref.Name == target.Name || manifest.HasEnvsubstReference(ref.Name)) &&
			(target.Namespace == "" || ref.Namespace == target.Namespace || ref.Namespace == "" || manifest.HasEnvsubstReference(ref.Namespace)) {
			match = true
			break
		}
	}
	return match
}

func (c *admissionClosure) materializerOwnerPending(
	consumer manifest.NamedResource,
	demandValue *materializerDemand,
	entry manifest.NamedResource,
) bool {
	demand := *demandValue
	for owner := range c.materializerOwners(consumer, &demand, entry) {
		if owner == (manifest.NamedResource{}) || owner == consumer ||
			demand.application == "" && !c.role(owner).needed {
			continue
		}
		if c.object(owner) == nil && !c.loader.IsFileIndexed(owner) {
			continue
		}
		if demand.application != "" {
			c.add(owner, admissionRole{needed: true, dirty: c.states[owner].changed})
		}
		status, _ := c.objects.GetStatus(owner)
		if status.Outcome() == store.OutcomeFailed {
			continue
		}
		output, ok := c.objects.GetArtifact(owner, store.ArtifactContent).(*store.Emission)
		if !ok || !output.Complete {
			c.searched[demand] = true
			return true
		}
	}
	return false
}

func (c *admissionClosure) yieldMaterializerCandidates(
	demandValue *materializerDemand,
	snapshot [2][]Occurrence,
	yield func(manifest.NamedResource, Occurrence) bool,
) {
	demand := *demandValue
	{
		occurrenceValues2 := snapshot[0]
		for occurrenceIndex2 := range occurrenceValues2 {
			occurrence := occurrenceValues2[occurrenceIndex2]
			if !yield(occurrence.Identity, occurrence) {
				return
			}
		}
	}
	if demand.source == "" {
		return
	}
	c.yieldObjectMaterializerCandidates(&demand, snapshot[1], yield)
}

func (c *admissionClosure) admitOpaqueMaterializers(consumer, target manifest.NamedResource, opaque []Occurrence) bool {
	if len(opaque) == 0 {
		return false
	}
	pending := false
	priority := map[manifest.NamedResource]bool{}
	c.prioritizeOpaqueMaterializers(consumer, priority)
	slices.SortFunc(opaque, func(a, b Occurrence) int { return a.Identity.Compare(b.Identity) })
	for tier := range 2 {
		var admitted []manifest.NamedResource
		{
			occurrenceValues5 := opaque
			for occurrenceIndex4 := range occurrenceValues5 {
				occurrence := occurrenceValues5[occurrenceIndex4]
				id := occurrence.Identity
				first := priority[id]
				{
					productionValues := c.productions(id)
					for productionIndex := range productionValues {
						production := productionValues[productionIndex]
						first = first || priority[production.Materializer(id)]
					}
				}
				if first != (tier == 0) {
					continue
				}
				if !c.role(id).needed {
					admitted = append(admitted, id)
				}
				c.add(id, admissionRole{needed: true, dirty: c.states[id].changed})
				pending = true
			}
		}
		if len(admitted) != 0 {
			slog.Debug(
				"loader: required input opaque tier",
				"consumer",
				consumer,
				"input",
				target,
				"tier",
				tier+1,
				"producers",
				admitted,
			)
		}
		if pending {
			break
		}
	}
	return pending
}

func (c *admissionClosure) skipIndexedMaterializer(id manifest.NamedResource, ordering bool) bool {
	_, indexed := c.loader.locations[id]
	return !ordering && (indexed || c.loader.candidates[id].Obj != nil && c.loader.Index.Records[id].Builder != "")
}

func (c *admissionClosure) selectorInputMatches(
	source, id manifest.NamedResource,
	matcher *manifest.ReferenceMatcher,
	reverse bool,
) bool {
	object := c.object(id)
	if object == nil {
		object = c.loader.candidates[id].Obj
	}
	matched := false
	if object != nil {
		if labeled, ok := object.(interface{ GetLabels() map[string]string }); ok {
			matched = matcher.Matches(id, labeled.GetLabels())
		}
	} else if occurrence := c.loader.locations[id]; occurrence.facts != nil {
		matched = matcher.Matches(id, occurrence.facts.labels)
	}
	if !matched && reverse && c.loader.sideKnown {
		if _, _, paired := c.delta(source); paired {
			opposite := c.loader.Options.Changes.OppositeDeclaration(c.loader.side, source, id, object)
			if labeled, ok := opposite.(interface{ GetLabels() map[string]string }); ok {
				matched = matcher.Matches(id, labeled.GetLabels())
			}
		}
	}
	return matched
}

func (c *admissionClosure) yieldInputCandidates(yield func(manifest.NamedResource) bool) {
	for id := range c.loader.locations {
		if !yield(id) {
			return
		}
	}
	for id := range c.loader.candidates {
		if _, located := c.loader.locations[id]; !located && !yield(id) {
			return
		}
	}
}

func (c *admissionClosure) matchComparedInputs(
	ref *manifest.Reference,
	matches func(source, id manifest.NamedResource),
) {
	for i := range c.comparisonSources().names {
		evidence := c.bindSource(c.comparisons.names[i])
		if !evidence.paired {
			continue
		}
		for id := range evidence.delta.Inputs(ref.NamedResource) {
			matches(evidence.source, id)
		}
	}
}

func (c *admissionClosure) materializerSourceMatches(
	demandValue *materializerDemand,
	consumer manifest.NamedResource,
	id manifest.NamedResource,
	occurrence *Occurrence,
) bool {
	demand := *demandValue
	if source, directory, applies := occurrence.SourcePath(); applies &&
		consumer != (manifest.NamedResource{}) {
		if source.Name == "" && c.loader.IsFileIndexed(id) {
			source = manifest.BootstrapSourceID
		}
		_, input, _ := c.delta(source)
		if input == nil || input.OriginPath(input.LocalPath) != demand.source ||
			demand.ordering && c.loader.documents.cache[filepath.Join(input.LocalPath, directory)].traversed {
			return false
		}
	}
	return true
}

func (c *admissionClosure) opaqueMaterializerPending(
	demandValue *materializerDemand,
	id manifest.NamedResource,
	facts *occurrenceFacts,
	target manifest.NamedResource,
) bool {
	demand := *demandValue
	templates := c.loader.templateProjections[id].Targets
	if demand.application != "" {
		templates = nil
	}
	if !demand.required || target.Name == "" || !facts.opaque(templates) || c.replace[id] {
		return false
	}
	status, _ := c.objects.GetStatus(id)
	output, _ := c.objects.GetArtifact(id, store.ArtifactContent).(*store.Emission)
	if status.Outcome() == store.OutcomeFailed && demand.application == "" {
		return false
	}
	complete := output != nil && output.Complete && output.OutputReads.Valid()
	if demand.application != "" && status.Outcome() != store.OutcomePassed {
		complete = false
	}
	return !complete
}

func (c *admissionClosure) acceptedMaterializerMatches(
	id manifest.NamedResource,
	matches materializerMatch,
) bool {
	present := false
	for child := range c.accepted[id] {
		record := c.accepted[id][child]
		present = present || matches(child, c.object(child), &record)
	}
	return present
}

func matchesMaterializerIdentity(target, id manifest.NamedResource) bool {
	return (target.Kind == "" || id.Group == target.Group && id.Kind == target.Kind) &&
		(target.Namespace == "" || id.Namespace == "" || id.Namespace == target.Namespace) &&
		(target.Name == "" || id.Name == target.Name)
}

func (c *admissionClosure) candidateMaterializerFacts(
	demandValue *materializerDemand,
	consumer manifest.NamedResource,
	id manifest.NamedResource,
	occurrenceValue *Occurrence,
) *occurrenceFacts {
	demand := *demandValue
	target := demand.target
	occurrence := *occurrenceValue
	if id == consumer || id == target || demand.source != "" && demand.source != occurrence.Declaration.Source {
		return nil
	}
	if consumer != (manifest.NamedResource{}) &&
		slices.ContainsFunc(c.productions(id), func(p manifest.Production) bool {
			return p.Owner == consumer || p.Declaration == consumer
		}) {
		return nil
	}
	facts := occurrence.facts
	if facts == nil {
		return nil
	}
	if !c.materializerSourceMatches(&demand, consumer, id, &occurrence) {
		return nil
	}
	unconstrained := target.Kind == "" || demand.application != ""
	fixedPath := facts.hasPath && !slices.ContainsFunc(facts.roots, manifest.HasEnvsubstReference)
	if unconstrained && fixedPath && !c.coversDeclaration(c.object(id), consumer) {
		return nil
	}
	return facts
}

type materializerOutput struct{ complete, include bool }

func (c *admissionClosure) materializerOutputMatches(
	id manifest.NamedResource,
	matches materializerMatch,
) materializerOutput {
	if c.replace[id] && !c.acceptedMaterializerMatches(id, matches) {
		return materializerOutput{}
	}
	complete := false
	if output, ok := c.objects.GetArtifact(id, store.ArtifactContent).(*store.Emission); ok && output.Complete {
		status, _ := c.objects.GetStatus(id)
		complete = status.Outcome() == store.OutcomePassed
		if complete &&
			!slices.ContainsFunc(
				output.Children,
				func(child store.Child) bool { return matches(child.Obj.Named(), child.Obj, nil) },
			) {
			return materializerOutput{complete: complete, include: false}
		}
	}
	return materializerOutput{complete: complete, include: true}
}

func (c *admissionClosure) materializerOwners(
	consumer manifest.NamedResource,
	demand *materializerDemand,
	entry manifest.NamedResource,
) iter.Seq[manifest.NamedResource] {
	return func(yield func(manifest.NamedResource) bool) {
		record := c.record(consumer)
		if !yield(record.Parent) || !yield(record.BuilderOwner) {
			return
		}
		if demand.application != "" {
			productionValues := c.productions(entry)
			for productionIndex := range productionValues {
				production := productionValues[productionIndex]
				if !yield(production.Materializer(entry)) {
					return
				}
			}
		}
		{
			productionValues := c.productions(consumer)
			for productionIndex := range productionValues {
				production := productionValues[productionIndex]
				if !yield(production.Materializer(consumer)) {
					return
				}
			}
		}
	}
}

func (c *admissionClosure) yieldObjectMaterializerCandidates(
	demand *materializerDemand,
	occurrences []Occurrence,
	yield func(manifest.NamedResource, Occurrence) bool,
) {
	target := demand.target
	{
		occurrenceValues3 := occurrences
		for occurrenceIndex3 := range occurrenceValues3 {
			occurrence := occurrenceValues3[occurrenceIndex3]
			id := occurrence.Identity
			if c.skipIndexedMaterializer(id, demand.ordering) {
				continue
			}
			_, _, pathBearing := occurrence.SourcePath()
			policy := occurrence.facts.policy
			if !pathBearing && !demand.ordering && demand.application == "" || policy == nil {
				continue
			}
			if demand.application == "" && !policy.MatchesNamespace(target) {
				continue
			}
			if demand.ordering &&
				!slices.ContainsFunc(
					c.productions(id),
					func(p manifest.Production) bool { return p.Owner != (manifest.NamedResource{}) },
				) {
				continue
			}
			if pathBearing {
				occurrence.Declaration.Source = demand.source
			}
			if !yield(id, occurrence) {
				return
			}
		}
	}
}
