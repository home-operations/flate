package selection

import (
	"iter"
	"path/filepath"
	"slices"
	"strings"

	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

func (c *admissionClosure) selectIncoming(id manifest.NamedResource) error {
	role := c.role(id)
	if c.incoming(id).Entry != nil && !role.needed {
		return nil
	}
	eligible := c.session.request.Matches(id)
	if c.decision.unrestricted {
		c.add(id, admissionRole{needed: true, selected: eligible, dirty: true, ordinary: true})
		return nil
	}
	var affected, footprintRead bool
	if eligible && !c.session.request.Changed {
		role.needed, role.selected, role.dirty = true, true, true
	}
	role = c.productionRole(id, role, eligible)
	if (!role.selected || !role.dirty) && (eligible || role.needed || c.session.request.MatchesScope(id)) {
		affected, footprintRead = c.affected(id, c.incoming(id).Obj), true
	}
	if (!role.selected || !role.dirty) &&
		(eligible || role.needed || c.session.request.MatchesScope(id)) &&
		(affected || c.changedInput(id, id)) {
		role.needed = true
		if eligible {
			role.selected, role.dirty = true, true
		}
	}
	return c.completeSelectIncoming(
		&completeSelectIncomingState{
			id:            &id,
			role:          &role,
			eligible:      &eligible,
			affected:      &affected,
			footprintRead: &footprintRead,
		},
	)
}

func (c *admissionClosure) productionRole(id manifest.NamedResource, role admissionRole, eligible bool) admissionRole {
	{
		productionValues := c.productions(id)
		for productionIndex := range productionValues {
			production := productionValues[productionIndex]
			parent := c.role(production.Owner)
			inherited := parent.selected && parent.dirty && c.selectsChild(id)
			inherited = inherited && (!production.Owner.ReportHidden() || !c.isFileIndexed(id))
			selected := inherited || c.changedProduction(production) && eligible
			if selected &&
				(!c.session.request.Changed || !c.unchangedDeclaration(id, production.Owner, c.incoming(id).Obj)) {
				role.needed, role.selected, role.dirty = true, true, true
			}
		}
	}

	return role
}

func (c *admissionClosure) reverseInputRole(
	id manifest.NamedResource,
	role admissionRole,
	eligible bool,
) admissionRole {
	for ref := range c.references(id, c.incoming(id).Obj) {
		if !c.reverse(id, c.incoming(id).Obj, new(ref)) {
			continue
		}
		inputs, known, err := c.matchingInputs(new(ref), true)
		if err != nil || !known {
			continue
		}
		for _, input := range inputs {
			if !c.role(input).dirty && !c.affected(input, c.object(input)) && !c.changedInput(id, input) {
				continue
			}
			if comparison := c.outputChanged(
				id,
				new(ref),
				c.incoming(id).Obj,
			); comparison.Known &&
				!comparison.Changed {
				continue
			}
			role.needed = true
			if eligible {
				role.selected, role.dirty = true, true
			}
		}
	}
	return role
}

func (c *admissionClosure) sourceRole(id manifest.NamedResource, role admissionRole) admissionRole {
	source, path, applies := c.incoming(id).Obj.SourcePath()
	if !applies || role.needed {
		return role
	}
	if output, ok := c.objects.GetArtifact(id, store.ArtifactContent).(*store.Emission); ok && output.Complete &&
		output.OutputReads.Valid() {
		return role
	}
	if c.incoming(id).Entry != nil {
		return role
	}
	if c.namespaceProductionDemand(id) {
		role.needed, role.ordinary = true, true
	}
	//nolint:gocritic // The iterator supplies immutable value snapshots.
	for target, record := range c.coveredDeclarations(source, path) {
		c.objects.WorkScope().Admission().Add(&manifest.WorkCounts{FileEntries: 1})
		needed := c.role(target).needed
		if !needed || record.BuilderOwner != (manifest.NamedResource{}) ||
			!c.coversDeclaration(new(c.rootOwnerFacts(c.incoming(id).Obj)), target) {
			continue
		}
		evidence := c.declarationEvidence(target)
		if evidence.accepted {
			continue
		}
		if record.Parent != (manifest.NamedResource{}) && record.Parent != id &&
			c.objects.GetDeclaration(target) != nil {
			continue
		}
		if c.rootEvidence(target).Admitted {
			continue
		}
		role.needed = true
		break
	}

	return role
}

func (c *admissionClosure) selectInputReaders(id manifest.NamedResource) error {
	key := id
	key.Name = ""
	for _, readers := range [][]manifest.NamedResource{c.context.InputReaders[id], c.context.InputReaders[key]} {
		for _, reader := range readers {
			if !c.session.request.Matches(reader) {
				continue
			}
			if err := c.selectInputReader(id, reader); err != nil {
				return err
			}
		}
	}
	return nil
}

type completeSelectIncomingState struct {
	id            *manifest.NamedResource
	role          *admissionRole
	eligible      *bool
	affected      *bool
	footprintRead *bool
}

func (c *admissionClosure) completeSelectIncoming(scope *completeSelectIncomingState) error {
	if (!scope.role.selected || !scope.role.dirty) &&
		((*scope.eligible) || scope.role.needed || c.session.request.MatchesScope((*scope.id))) {
		(*scope.role) = c.reverseInputRole((*scope.id), (*scope.role), (*scope.eligible))
	}
	(*scope.role) = c.sourceRole((*scope.id), (*scope.role))
	c.add((*scope.id), (*scope.role))
	if !scope.role.dirty && !(*scope.footprintRead) {
		(*scope.affected) = c.affected((*scope.id), c.incoming(*scope.id).Obj)
	}
	if scope.role.dirty || (*scope.affected) {
		if err := c.selectInputReaders((*scope.id)); err != nil {
			return err
		}
	}
	return nil
}

func (c *admissionClosure) selectInputReader(id, reader manifest.NamedResource) error {
	object := c.object(reader)
	for ref := range c.references(reader, object) {
		if !c.reverse(reader, object, new(ref)) || ref.Group != id.Group || ref.Kind != id.Kind ||
			ref.Namespace != id.Namespace {
			continue
		}
		inputs, known, err := c.matchingInputs(new(ref), true)
		if err != nil {
			return err
		}
		if !known || !slices.Contains(inputs, id) {
			continue
		}
		if comparison := c.outputChanged(reader, new(ref), object); comparison.Known && !comparison.Changed {
			continue
		}
		c.add(reader, admissionRole{needed: true, selected: true, dirty: true})
	}
	return nil
}

func (c *admissionClosure) namespaceProductionDemand(id manifest.NamedResource) bool {
	if !slices.ContainsFunc(c.session.request.Roots, func(root manifest.NamedResource) bool {
		return root.Name == "" && root.Namespace != ""
	}) {
		return false
	}
	object := c.object(id)
	if object == nil {
		return false
	}
	if _, _, applies := object.SourcePath(); !applies {
		return false
	}
	namespaceSelected := manifest.KindOf(object).Parent &&
		slices.ContainsFunc(c.session.request.Roots, func(root manifest.NamedResource) bool {
			return root.Name == "" && root.Namespace != "" && object.EmissionPolicy().MatchesNamespace(root)
		})
	if !namespaceSelected {
		return false
	}
	neededProducer := slices.ContainsFunc(c.productions(id), func(p manifest.Production) bool {
		return p.Owner != (manifest.NamedResource{}) && c.role(p.Owner).needed
	})
	if !neededProducer {
		return false
	}
	return !c.session.request.Changed || c.affected(id, c.object(id)) || c.changedInput(id, id)
}

func (c *admissionClosure) coveredDeclarations(
	source manifest.NamedResource,
	path string,
) iter.Seq2[manifest.NamedResource, manifest.DiscoveryRecord] {
	if c.catalog == nil {
		return c.facts.SelectionIndex().Files()
	}
	return func(yield func(manifest.NamedResource, manifest.DiscoveryRecord) bool) {
		if source.Name == "" {
			source = manifest.BootstrapSourceID
		}
		delta, input, _ := c.delta(source)
		if input == nil {
			return
		}
		root := input.OriginPath(filepath.Join(input.LocalPath, path))
		if !containsPath(input.OriginPath(input.LocalPath), root) {
			return
		}
		records := c.facts.SelectionIndex().Records
		if !c.catalog.yieldCoveredFiles(records, root, yield) {
			return
		}
		for id := range delta.Inputs(manifest.NamedResource{}) {
			record := records[id]
			if record.File != "" && record.Path == "" && record.BuilderOwner == (manifest.NamedResource{}) &&
				!yield(id, record) {
				return
			}
		}
	}
}

func (c *admissionCatalog) yieldCoveredFiles(
	records map[manifest.NamedResource]manifest.DiscoveryRecord,
	root string,
	yield func(manifest.NamedResource, manifest.DiscoveryRecord) bool,
) bool {
	compare := func(id manifest.NamedResource, path string) int {
		return strings.Compare(records[id].Path, path)
	}
	first, _ := slices.BinarySearchFunc(c.files, root, compare)
	for _, id := range c.files[first:] {
		record := records[id]
		if record.Path != root {
			break
		}
		if !yield(id, record) {
			return false
		}
	}
	prefix := strings.TrimSuffix(root, string(filepath.Separator)) + string(filepath.Separator)
	first, _ = slices.BinarySearchFunc(c.files, prefix, compare)
	for _, id := range c.files[first:] {
		record := records[id]
		if !strings.HasPrefix(record.Path, prefix) {
			break
		}
		if !yield(id, record) {
			return false
		}
	}
	return true
}
