package selection

import (
	"iter"
	"log/slog"
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

func (c *admissionClosure) materializerCandidates(ordering bool) [2][]OccurrenceFacts {
	mode := 0
	if ordering {
		mode = 1
	}
	candidates := c.candidates[mode]
	objects := c.session.store.Objects()
	slices.SortFunc(objects, func(a, b manifest.BaseManifest) int { return a.Named().Compare(b.Named()) })
	project := func(object manifest.BaseManifest) OccurrenceFacts {
		facts := c.facts.ProducerFacts(object)
		if facts.Derived {
			c.derive()
		}
		return facts.Occurrence
	}
	c.collectIndexedMaterializers(&candidates, project)
	c.collectObjectMaterializers(&candidates, objects, ordering, project)
	for i := range candidates {
		slices.SortFunc(candidates[i], func(a, b OccurrenceFacts) int { return a.Authored.Compare(b.Authored) })
	}
	return candidates
}

func (c *admissionClosure) materializers(
	demandValue *materializerDemand,
	consumer manifest.NamedResource,
	snapshot [2][]OccurrenceFacts,
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
	if demand.application != "" && c.proposal.entrySupported() {
		return false
	}
	if c.materializerOwnerPending(consumer, &demand, entry) {
		return true
	}
	target := demand.target
	if demand.application == "" && c.objects.ReferenceFacts(target, c.facts.SelectionIndex(), consumer).Data != nil {
		return false
	}

	candidates := func(yield func(manifest.NamedResource, OccurrenceFacts) bool) {
		c.yieldMaterializerCandidates(&demand, snapshot, yield)
	}
	pending := false
	var opaque []OccurrenceFacts
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
		c.admitMaterializer(id, &demand)
	}
	if !pending {
		pending = c.admitOpaqueMaterializers(consumer, &demand, opaque)
	}
	c.searched[demand] = pending
	return pending
}

func (c *admissionClosure) admitMaterializer(id manifest.NamedResource, demand *materializerDemand) {
	if demand.required && c.states[demand.target].requested {
		state := c.states[id]
		state.requested = true
		c.states[id] = state
	}
	c.add(id, admissionRole{needed: true, ordinary: demand.application == "", dirty: c.states[id].changed})
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
	if c.context.Frozen {
		candidates = slices.Values(c.context.InputObjects[ref.NamedResource])
	}
	for id := range candidates {
		matches(c.source(id), id)
	}
	if reverse && c.context.SideKnown {
		c.matchComparedInputs(new(ref), matches)
	}
	for i := range c.generation {
		if child := &c.generation[i]; child.Obj != nil {
			id := child.Obj.Named()
			matches(c.source(id), id)
		}
	}
	slices.SortFunc(inputs, manifest.NamedResource.Compare)
	return inputs, nil
}

func (c *admissionClosure) retainIndexedMaterializers(candidates *[2][]OccurrenceFacts) {
	retained := candidates[0][:0]
	for i := range candidates[0] {
		occurrence := candidates[0][i]
		if occurrence.Object != nil {
			id := occurrence.Object.Named()
			record, _ := c.facts.SelectionIndex().Lookup(id)
			if record.Builder == "" || record.Declaration != occurrence.Object {
				continue
			}
		} else if current, ok := c.catalogOccurrence(occurrence.Authored); !ok ||
			current.Witness != occurrence.Witness || current.Declaration != occurrence.Declaration {
			continue
		}
		retained = append(retained, occurrence)
	}
	clear(candidates[0][len(retained):])
	candidates[0] = retained
}

func (c *admissionClosure) collectIndexedMaterializers(
	candidates *[2][]OccurrenceFacts,
	project func(manifest.BaseManifest) OccurrenceFacts,
) {
	c.retainIndexedMaterializers(candidates)
	for id := range c.facts.SelectionIndex().Records {
		occurrence, located := c.catalogOccurrence(id)
		if !located {
			continue
		}
		facts := &occurrence
		if facts.Policy == nil && len(facts.Targets) == 0 && !facts.NeedsCandidates ||
			facts.HasPath && facts.Policy != nil && len(facts.Roots) == 0 {
			continue
		}
		if !slices.ContainsFunc(candidates[0], func(existing OccurrenceFacts) bool {
			return existing.Declaration == occurrence.Declaration && existing.Identity == occurrence.Identity &&
				existing.Witness == occurrence.Witness
		}) {
			candidates[0] = append(candidates[0], occurrence)
		}
	}
	c.collectPreparedMaterializers(candidates, project)
}

func (c *admissionClosure) collectPreparedMaterializers(
	candidates *[2][]OccurrenceFacts,
	project func(manifest.BaseManifest) OccurrenceFacts,
) {
	for id := range c.facts.SelectionIndex().Records {
		child := c.candidate(id)
		if _, located := c.catalogOccurrence(
			id,
		); child.Obj == nil || located ||
			c.facts.SelectionIndex().Records[id].Builder == "" {
			continue
		}
		if !isMaterializerObject(child.Obj) {
			continue
		}
		if slices.ContainsFunc(
			candidates[0],
			func(existing OccurrenceFacts) bool { return existing.Object == child.Obj },
		) {
			continue
		}
		occurrence := project(child.Obj)
		if occurrence.HasPath && occurrence.Policy != nil && len(occurrence.Roots) == 0 {
			continue
		}
		occurrence.Declaration.Source = c.declarationSource(id)
		candidates[0] = append(candidates[0], occurrence)
	}
}

func (c *admissionClosure) retainObjectMaterializers(
	candidates *[2][]OccurrenceFacts,
	objects []manifest.BaseManifest,
	ordering bool,
) {
	retained := candidates[1][:0]
	for i := range candidates[1] {
		occurrence := candidates[1][i]
		id := occurrence.Object.Named()
		index, present := slices.BinarySearchFunc(
			objects,
			id,
			func(object manifest.BaseManifest, target manifest.NamedResource) int {
				return object.Named().Compare(target)
			},
		)
		if !present || c.skipIndexedMaterializer(id, ordering) || objects[index] != occurrence.Object {
			continue
		}
		retained = append(retained, occurrence)
	}
	clear(candidates[1][len(retained):])
	candidates[1] = retained
}

func (c *admissionClosure) collectObjectMaterializers(
	candidates *[2][]OccurrenceFacts,
	objects []manifest.BaseManifest,
	ordering bool,
	project func(manifest.BaseManifest) OccurrenceFacts,
) {
	c.retainObjectMaterializers(candidates, objects, ordering)
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
		if slices.ContainsFunc(
			candidates[1],
			func(existing OccurrenceFacts) bool { return existing.Object == object },
		) {
			continue
		}
		occurrence := project(object)
		if occurrence.HasPath && occurrence.Policy != nil && len(occurrence.Roots) == 0 {
			continue
		}
		if !occurrence.HasPath {
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
		queue = append(queue, c.rootEvidence(id).Owner, record.Parent, record.BuilderOwner)
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
	facts *OccurrenceFacts,
	demand *materializerDemand,
	matches materializerMatch,
) bool {
	target := demand.target
	match := c.coversDeclaration(facts.Catalog, target)
	if demand.application != "" {
		applies := func(object manifest.BaseManifest) bool {
			if object.Named().Namespace == "" {
				object = object.WithNamespace(id.Namespace)
			}
			return matches(object.Named(), object, nil)
		}
		match = match || slices.ContainsFunc(facts.TemplateObjects, applies) ||
			slices.ContainsFunc(c.context.TemplateProjections[id].Objects, applies)
	}
	targets := facts.Targets
	if extra := c.context.TemplateProjections[id].Targets; len(extra) != 0 {
		targets = append(slices.Clone(targets), extra...)
	}
	if facts.NeedsCandidates {
		targets = append(
			slices.Clone(targets),
			manifest.ProducerTargets(facts.Catalog.WithNamespace(id.Namespace), target)...)
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
		if c.object(owner) == nil && !c.isFileIndexed(owner) {
			continue
		}
		if demand.application != "" {
			c.add(owner, admissionRole{needed: true, ordinary: true, dirty: c.states[owner].changed})
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
	snapshot [2][]OccurrenceFacts,
	yield func(manifest.NamedResource, OccurrenceFacts) bool,
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

func (c *admissionClosure) admitOpaqueMaterializers(
	consumer manifest.NamedResource,
	demand *materializerDemand,
	opaque []OccurrenceFacts,
) bool {
	if len(opaque) == 0 {
		return false
	}
	pending := false
	priority := map[manifest.NamedResource]bool{}
	c.prioritizeOpaqueMaterializers(consumer, priority)
	slices.SortFunc(opaque, func(a, b OccurrenceFacts) int { return a.Identity.Compare(b.Identity) })
	for tier := range 2 {
		var admitted []manifest.NamedResource
		{
			occurrenceValues5 := opaque
			for occurrenceIndex4 := range occurrenceValues5 {
				occurrence := occurrenceValues5[occurrenceIndex4]
				id := occurrence.Identity
				first := priority[id] || priority[c.rootEvidence(id).Owner]
				productions := c.productions(id)
				first = first || slices.ContainsFunc(productions, func(production manifest.Production) bool {
					return priority[production.Materializer(id)]
				})
				if first != (tier == 0) {
					continue
				}
				if !c.role(id).needed {
					admitted = append(admitted, id)
				}
				c.admitMaterializer(id, demand)
				pending = true
			}
		}
		if len(admitted) != 0 {
			slog.Debug(
				"loader: required input opaque tier",
				"consumer",
				consumer,
				"input",
				demand.target,
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
	_, indexed := c.catalogOccurrence(id)
	return !ordering && (indexed || c.candidate(id).Obj != nil && c.facts.SelectionIndex().Records[id].Builder != "")
}

func (c *admissionClosure) selectorInputMatches(
	source, id manifest.NamedResource,
	matcher *manifest.ReferenceMatcher,
	reverse bool,
) bool {
	object := c.object(id)
	if object == nil {
		object = c.candidate(id).Obj
	}
	matched := false
	if object != nil {
		if labeled, ok := object.(interface{ GetLabels() map[string]string }); ok {
			matched = matcher.Matches(id, labeled.GetLabels())
		}
	} else if occurrence, ok := c.catalogOccurrence(id); ok {
		matched = matcher.Matches(id, occurrence.Labels)
	}
	if !matched && reverse && c.context.SideKnown {
		if _, _, paired := c.delta(source); paired {
			opposite := c.proposal.Request.Changes.OppositeDeclaration(c.context.Side, source, id, object)
			if labeled, ok := opposite.(interface{ GetLabels() map[string]string }); ok {
				matched = matcher.Matches(id, labeled.GetLabels())
			}
		}
	}
	return matched
}

func (c *admissionClosure) yieldInputCandidates(yield func(manifest.NamedResource) bool) {
	for id := range c.facts.SelectionIndex().Records {
		if !yield(id) {
			return
		}
	}
}

func (c *admissionClosure) matchComparedInputs(
	ref *manifest.Reference,
	matches func(source, id manifest.NamedResource),
) {
	for source := range c.proposal.Request.Changes.Sources() {
		evidence := c.boundSource(source)
		if !evidence.Paired {
			continue
		}
		for id := range evidence.Delta.Inputs(ref.NamedResource) {
			matches(evidence.Source, id)
		}
	}
}

func (c *admissionClosure) materializerSourceMatches(
	demandValue *materializerDemand,
	consumer manifest.NamedResource,
	id manifest.NamedResource,
	occurrence *OccurrenceFacts,
) bool {
	demand := *demandValue
	if source, _, applies := occurrence.SourcePath(); applies &&
		consumer != (manifest.NamedResource{}) {
		if source.Name == "" && c.isFileIndexed(id) {
			source = manifest.BootstrapSourceID
		}
		_, input, _ := c.delta(source)
		if input == nil || input.OriginPath(input.LocalPath) != demand.source ||
			demand.ordering && occurrence.Traversed {
			return false
		}
	}
	return true
}

func (c *admissionClosure) opaqueMaterializerPending(
	demandValue *materializerDemand,
	id manifest.NamedResource,
	facts *OccurrenceFacts,
	target manifest.NamedResource,
) bool {
	demand := *demandValue
	templates := c.context.TemplateProjections[id].Targets
	if demand.application != "" || demand.consumer == (manifest.NamedResource{}) {
		templates = nil
	}
	if !demand.required || target.Name == "" || !opaqueOccurrence(facts, templates) || c.replace[id] {
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
	occurrenceValue *OccurrenceFacts,
) *OccurrenceFacts {
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
	facts := &occurrence
	if facts.Witness == nil && facts.Catalog == nil {
		return nil
	}
	if !c.materializerSourceMatches(&demand, consumer, id, &occurrence) {
		return nil
	}
	unconstrained := target.Kind == "" || demand.application != ""
	fixedPath := facts.HasPath && !slices.ContainsFunc(facts.Roots, manifest.HasEnvsubstReference)
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
	occurrences []OccurrenceFacts,
	yield func(manifest.NamedResource, OccurrenceFacts) bool,
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
			policy := occurrence.Policy
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
