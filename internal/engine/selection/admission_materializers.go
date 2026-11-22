package selection

import (
	"iter"
	"log/slog"
	"maps"
	"slices"

	"github.com/home-operations/flate/internal/engine/store"
	"github.com/home-operations/flate/pkg/manifest"
)

type materializerDemand struct {
	target, consumer   manifest.NamedResource
	key, use           string
	source             string
	ordering, required bool
	lookup             bool
}

func (c *admissionClosure) materializerCandidates(ordering bool) [2][]*OccurrenceFacts {
	mode := 0
	if ordering {
		mode = 1
	}
	if c.candidatesKnown[mode] {
		return c.candidates[mode]
	}
	candidates := c.candidates[mode]
	var objects []manifest.BaseManifest
	for kind := range manifest.Kinds() {
		if kind.Parent {
			objects = append(objects, c.session.store.ListObjects(kind.Group, kind.Kind)...)
		}
	}
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
		if i == 0 && c.catalog != nil && len(candidates[0]) == len(c.catalog.candidates) {
			continue
		}
		slices.SortFunc(candidates[i], func(a, b *OccurrenceFacts) int { return a.Authored.Compare(b.Authored) })
	}
	c.candidates[mode], c.candidatesKnown[mode] = candidates, true
	return candidates
}

//nolint:gocritic // The search key MUST remain independent while its consumer is assigned.
func (c *admissionClosure) materializers(
	demand materializerDemand,
	consumer manifest.NamedResource,
) bool {
	demand.consumer = consumer
	if pending, searched := c.searched[demand]; searched {
		return pending
	}
	if c.searched == nil {
		c.searched = map[materializerDemand]bool{}
	}
	c.searched[demand] = false
	if c.materializerPending(consumer, &demand) {
		return true
	}
	target := demand.target
	ref := manifest.Reference{
		Kind:      target.Kind,
		Group:     target.Group,
		Namespace: target.Namespace,
		Name:      target.Name,
		Key:       demand.key,
		Use:       demand.use,
	}
	if !demand.lookup && c.missing != nil &&
		c.missing.Admission(
			new(ref),
			new(c.objects.ReferenceFacts(target, c.facts.SelectionIndex(), consumer)),
		) == AvailableInput {
		return false
	}

	available := c.materializerCandidates(demand.ordering)
	candidates := func(yield func(manifest.NamedResource, OccurrenceFacts) bool) {
		c.yieldMaterializerCandidates(&demand, available, yield)
	}
	pending := false
	var opaque []OccurrenceFacts
	//nolint:gocritic // The iterator supplies immutable value snapshots.
	for id, occurrence := range candidates {
		facts := c.candidateMaterializerFacts(&demand, consumer, id, &occurrence)
		if facts == nil {
			continue
		}
		match := c.materializerTargetsMatch(id, facts, &demand)
		if !match && c.opaqueMaterializerPending(&demand, id, facts, target) {
			opaque = append(opaque, occurrence)
		}
		if !match {
			continue
		}
		pending = c.admitMatchedMaterializer(id, &demand) || pending
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
	c.add(id, admissionRole{needed: true, ordinary: true, dirty: c.states[id].changed})
}

// A named input borrows the scratch slot until the next call.
func (c *admissionClosure) matchingInputs(
	refValue *manifest.Reference,
	reverse bool,
) ([]manifest.NamedResource, bool, error) {
	ref := *refValue
	if manifest.HasEnvsubstReference(ref.Name) || manifest.HasEnvsubstReference(ref.Namespace) {
		return nil, false, nil
	}
	if ref.Name != "" {
		c.inputScratch[0] = ref.NamedResource
		return c.inputScratch[:], true, nil
	}
	if ref.Selector == nil {
		return nil, true, nil
	}
	if !selectorComplete(&ref) {
		return nil, false, nil
	}
	matcher, err := ref.Matcher(ref.Selector)
	if err != nil {
		return nil, true, err
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
	return inputs, true, nil
}

func (c *admissionClosure) retainIndexedMaterializers(candidates *[2][]*OccurrenceFacts) {
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
	candidates *[2][]*OccurrenceFacts,
	project func(manifest.BaseManifest) OccurrenceFacts,
) {
	if c.catalog != nil {
		candidates[0] = c.catalog.candidates
		c.collectPreparedMaterializers(candidates, project)
		return
	}
	c.retainIndexedMaterializers(candidates)
	for id := range maps.Keys(c.facts.SelectionIndex().Records) {
		occurrence, located := c.catalogOccurrence(id)
		if !located {
			continue
		}
		facts := &occurrence
		if facts.Policy == nil && len(facts.Targets) == 0 && !facts.NeedsCandidates ||
			facts.HasPath && facts.Policy != nil && len(facts.Roots) == 0 {
			continue
		}
		if !slices.ContainsFunc(candidates[0], func(existing *OccurrenceFacts) bool {
			return existing.Declaration == occurrence.Declaration && existing.Identity == occurrence.Identity &&
				existing.Witness == occurrence.Witness
		}) {
			candidates[0] = append(candidates[0], &occurrence)
		}
	}
	c.collectPreparedMaterializers(candidates, project)
}

func (c *admissionClosure) collectPreparedMaterializers(
	candidates *[2][]*OccurrenceFacts,
	project func(manifest.BaseManifest) OccurrenceFacts,
) {
	identities := maps.Keys(c.facts.SelectionIndex().Records)
	if c.catalog != nil {
		identities = slices.Values(c.catalog.materializers)
	}
	for id := range identities {
		child := c.facts.AdmissionCandidate(id)
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
			func(existing *OccurrenceFacts) bool { return existing.Object == child.Obj },
		) {
			continue
		}
		occurrence := project(child.Obj)
		if occurrence.HasPath && occurrence.Policy != nil && len(occurrence.Roots) == 0 {
			continue
		}
		occurrence.Declaration.Source = c.declarationSource(id)
		candidates[0] = append(candidates[0], &occurrence)
	}
}

func (c *admissionClosure) retainObjectMaterializers(
	candidates *[2][]*OccurrenceFacts,
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
	candidates *[2][]*OccurrenceFacts,
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
			func(existing *OccurrenceFacts) bool { return existing.Object == object },
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
		candidates[1] = append(candidates[1], &occurrence)
	}
}

func isMaterializerObject(object manifest.BaseManifest) bool {
	return manifest.KindOf(object).Parent
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
) bool {
	target := demand.target
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
			(target.Name == "" || ref.Name == target.Name) &&
			(target.Namespace == "" || ref.Namespace == target.Namespace || ref.Namespace == "") {
			return true
		}
	}
	if demand.ordering && c.session.request.Changed {
		record, _ := c.facts.SelectionIndex().Lookup(target)
		return slices.ContainsFunc(record.Productions, func(production manifest.Production) bool {
			return production.Materializer(target) == id
		})
	}
	return c.coversDeclaration(new(rootOwnerFact(facts.Catalog, c.rootReader())), target, targetedClaimScope) ||
		demand.lookup && c.lookupPackageSource(id, target, facts)
}

func (c *admissionClosure) materializerPending(consumer manifest.NamedResource, demand *materializerDemand) bool {
	for owner := range c.materializerOwners(consumer) {
		if owner == (manifest.NamedResource{}) || owner == consumer || !c.role(owner).needed {
			continue
		}
		if c.object(owner) == nil && !c.isFileIndexed(owner) {
			continue
		}
		status, _ := c.objects.GetStatus(owner)
		if status.Outcome() == store.OutcomeFailed {
			continue
		}
		output, ok := c.objects.GetArtifact(owner, store.ArtifactContent).(*store.Emission)
		if !ok || !output.Complete {
			c.searched[*demand] = true
			return true
		}
	}
	return false
}

func (c *admissionClosure) yieldMaterializerCandidates(
	demandValue *materializerDemand,
	snapshot [2][]*OccurrenceFacts,
	yield func(manifest.NamedResource, OccurrenceFacts) bool,
) {
	demand := *demandValue
	{
		occurrenceValues2 := snapshot[0]
		for occurrenceIndex2 := range occurrenceValues2 {
			occurrence := occurrenceValues2[occurrenceIndex2]
			if !yield(occurrence.Identity, *occurrence) {
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
	return !ordering &&
		(indexed || c.facts.AdmissionCandidate(id).Obj != nil && c.facts.SelectionIndex().Records[id].Builder != "")
}

func (c *admissionClosure) selectorInputMatches(
	source, id manifest.NamedResource,
	matcher *manifest.ReferenceMatcher,
	reverse bool,
) bool {
	object := c.object(id)
	if object == nil {
		object = c.facts.AdmissionCandidate(id).Obj
	}
	matched := false
	if occurrence, authored := c.projectedOccurrence(id, object); authored {
		matched = matcher.Matches(id, occurrence.Labels)
	} else if manifest.KindOf(object).TypedLabels {
		matched = matcher.Matches(id, object.GetLabels())
	}
	if !matched && reverse && c.context.SideKnown {
		if _, _, paired := c.delta(source); paired {
			opposite := c.proposal.Request.Changes.OppositeDeclaration(c.context.Side, source, id, object)
			if manifest.KindOf(opposite).TypedLabels {
				matched = matcher.Matches(id, opposite.GetLabels())
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
		for id := range c.comparisonInputs(evidence.Delta, ref.NamedResource) {
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
	if demand.consumer == (manifest.NamedResource{}) {
		templates = nil
	}
	if !demand.required || target.Name == "" || !opaqueOccurrence(facts, templates) || c.replace[id] {
		return false
	}
	status, _ := c.objects.GetStatus(id)
	output, _ := c.objects.GetArtifact(id, store.ArtifactContent).(*store.Emission)
	if status.Outcome() == store.OutcomeFailed {
		return false
	}
	return output == nil || !output.Complete || !output.OutputReads.Valid()
}

func (c *admissionClosure) acceptedMaterializerMatches(owner, target manifest.NamedResource) bool {
	for child := range c.accepted[owner] {
		if matchesMaterializerIdentity(target, child) {
			return true
		}
	}
	return false
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
	occurrence *OccurrenceFacts,
) *OccurrenceFacts {
	demand := *demandValue
	target := demand.target
	otherSource := !demand.lookup && demand.source != "" && demand.source != occurrence.Declaration.Source
	if id == consumer || id == target || otherSource {
		return nil
	}
	if consumer != (manifest.NamedResource{}) &&
		slices.ContainsFunc(c.productions(id), func(p manifest.Production) bool {
			return p.Owner == consumer || p.Declaration == consumer
		}) {
		return nil
	}
	facts := occurrence
	if facts.Witness == nil && facts.Catalog == nil {
		return nil
	}
	if !c.materializerSourceMatches(&demand, consumer, id, occurrence) {
		return nil
	}
	unconstrained := target.Kind == ""
	fixedPath := facts.HasPath && !slices.ContainsFunc(facts.Roots, manifest.HasEnvsubstReference)
	if unconstrained && fixedPath &&
		!c.coversDeclaration(new(rootOwnerFact(c.object(id), c.rootReader())), consumer, targetedClaimScope) {
		return nil
	}
	return facts
}

type materializerOutput struct{ complete, include bool }

func (c *admissionClosure) materializerOutputMatches(
	id, target manifest.NamedResource,
) materializerOutput {
	if c.replace[id] && !c.acceptedMaterializerMatches(id, target) {
		return materializerOutput{}
	}
	complete := false
	if output, ok := c.objects.GetArtifact(id, store.ArtifactContent).(*store.Emission); ok && output.Complete {
		status, _ := c.objects.GetStatus(id)
		complete = status.Outcome() == store.OutcomePassed
		if complete &&
			!slices.ContainsFunc(
				output.Children,
				func(child store.Child) bool { return matchesMaterializerIdentity(target, child.Obj.Named()) },
			) {
			return materializerOutput{complete: complete, include: false}
		}
	}
	return materializerOutput{complete: complete, include: true}
}

func (c *admissionClosure) materializerOwners(consumer manifest.NamedResource) iter.Seq[manifest.NamedResource] {
	return func(yield func(manifest.NamedResource) bool) {
		record := c.record(consumer)
		if !yield(record.Parent) || !yield(record.BuilderOwner) {
			return
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
	occurrences []*OccurrenceFacts,
	yield func(manifest.NamedResource, OccurrenceFacts) bool,
) {
	target := demand.target
	{
		occurrenceValues3 := occurrences
		for occurrenceIndex3 := range occurrenceValues3 {
			occurrence := *occurrenceValues3[occurrenceIndex3]
			id := occurrence.Identity
			if c.skipIndexedMaterializer(id, demand.ordering) {
				continue
			}
			_, _, pathBearing := occurrence.SourcePath()
			policy := occurrence.Policy
			if !pathBearing && !demand.ordering || policy == nil {
				continue
			}
			if !policy.MatchesNamespace(target) {
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
