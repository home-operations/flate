package loader

import (
	"slices"

	"github.com/home-operations/flate/pkg/manifest"
)

func (c *admissionClosure) selectIncoming() error {
	id := c.incoming.Obj.Named()
	role := c.role(id)
	eligible := c.loader.Options.Selection.Matches(id)
	var affected, footprintRead bool
	if eligible && !c.loader.Options.Selection.Changed {
		role.needed, role.selected, role.dirty = true, true, true
	}
	role = c.productionRole(id, role, eligible)
	if (!role.selected || !role.dirty) && (eligible || role.needed || c.loader.Options.Selection.MatchesScope(id)) {
		affected, footprintRead = c.affected(id, c.incoming.Obj), true
	}
	if (!role.selected || !role.dirty) &&
		(eligible || role.needed || c.loader.Options.Selection.MatchesScope(id)) &&
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
			inherited = inherited && (!production.Owner.ReportHidden() || !c.loader.IsFileIndexed(id))
			selected := inherited || c.changedProduction(production) && eligible
			if selected && !c.unchangedDeclaration(id, production.Owner, c.incoming.Obj) {
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
	for ref := range c.references(id, c.incoming.Obj) {
		if !c.reverse(id, c.incoming.Obj, new(ref)) {
			continue
		}
		inputs, err := c.matchingInputs(new(ref), true)
		if err != nil {
			continue
		}
		for _, input := range inputs {
			if !c.role(input).dirty && !c.affected(input, c.object(input)) && !c.changedInput(id, input) {
				continue
			}
			if comparison := c.outputChanged(id, new(ref), c.incoming.Obj); comparison.Known && !comparison.Changed {
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
	if _, _, applies := c.incoming.Obj.SourcePath(); !applies || role.needed {
		return role
	}
	if producer, ok := c.incoming.Obj.(interface {
		EmissionPolicy() manifest.EmissionPolicy
	}); ok && !c.loader.Options.Selection.Changed &&
		slices.ContainsFunc(c.loader.Options.Selection.Roots, func(root manifest.NamedResource) bool {
			return root.Name == "" && producer.EmissionPolicy().MatchesNamespace(root)
		}) &&
		slices.ContainsFunc(c.productions(id), func(p manifest.Production) bool {
			return p.Owner != (manifest.NamedResource{}) && c.role(p.Owner).needed
		}) {
		role.needed = true
	}
	//nolint:gocritic // The iterator supplies immutable value snapshots.
	for target, record := range c.loader.Index.Files() {
		c.objects.WorkScope().Admission().Add(&manifest.WorkCounts{FileEntries: 1})
		needed := c.loader.Options.Filter.Membership(target).Needed
		if !needed || record.BuilderOwner != (manifest.NamedResource{}) ||
			!c.coversDeclaration(c.incoming.Obj, target) {
			continue
		}
		if record.Parent != (manifest.NamedResource{}) && record.Parent != id &&
			c.objects.GetDeclaration(target) != nil {
			continue
		}
		if record.Parent == (manifest.NamedResource{}) &&
			c.loader.AdmitsRoot(c.loader.CatalogObject(target), record.Path) {
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
	for _, readers := range [][]manifest.NamedResource{c.loader.inputReaders[id], c.loader.inputReaders[key]} {
		for _, reader := range readers {
			if !c.loader.Options.Selection.Matches(reader) {
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
		((*scope.eligible) || scope.role.needed || c.loader.Options.Selection.MatchesScope((*scope.id))) {
		(*scope.role) = c.reverseInputRole((*scope.id), (*scope.role), (*scope.eligible))
	}
	(*scope.role) = c.sourceRole((*scope.id), (*scope.role))
	c.add((*scope.id), (*scope.role))
	if !scope.role.dirty && !(*scope.footprintRead) {
		(*scope.affected) = c.affected((*scope.id), c.incoming.Obj)
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
		inputs, err := c.matchingInputs(new(ref), true)
		if err != nil {
			return err
		}
		if !slices.Contains(inputs, id) {
			continue
		}
		if comparison := c.outputChanged(reader, new(ref), object); comparison.Known && !comparison.Changed {
			continue
		}
		c.add(reader, admissionRole{needed: true, selected: true, dirty: true})
	}
	return nil
}
