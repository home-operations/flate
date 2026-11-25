package selection

import (
	"slices"

	"github.com/home-operations/flate/internal/engine/store"
	"github.com/home-operations/flate/pkg/manifest"
)

func (c *admissionClosure) qualifySelectionProducers() {
	request := c.session.request
	restricted := request.RestrictedPackageLookup()
	if !restricted && !request.Changed {
		return
	}
	c.qualifyLookupInputs()
	for _, identity := range c.identities {
		id := identity.id
		record, _ := c.facts.SelectionIndex().Lookup(id)
		switch {
		case restricted && record.Builder != "" && request.Matches(id) && !c.outsideEntry(record.BuilderOwner, id):
			c.add(record.BuilderOwner, admissionRole{needed: true, ordinary: true})
			if c.namedSelection(id) {
				c.proposal.lookups = append(c.proposal.lookups, record.BuilderOwner)
			}
		case record.Builder == "":
			c.qualifyPendingSelection(id, &record, restricted)
		}
	}
}

func (c *admissionClosure) qualifyPendingSelection(
	id manifest.NamedResource, record *manifest.DiscoveryRecord, restricted bool,
) {
	facts, known := c.catalogOccurrence(id)
	if !known || !facts.RequiresBuild {
		return
	}
	request := c.session.request
	changed := request.Changed && request.MatchesScope(id)
	for owner, production := range PendingPackageProductions(id, record) {
		lookup := false
		if ownerFacts, known := c.catalogOccurrence(owner); restricted && known {
			source, _, applies := ownerFacts.SourcePath()
			lookup = slices.ContainsFunc(request.Roots, func(root manifest.NamedResource) bool {
				return root.Name != "" && root.Group == id.Group && root.Kind == id.Kind &&
					LookupPackageMatches(root, ownerFacts.Policy, source, applies)
			})
		}
		if lookup || changed && c.changedProduction(*production) {
			c.qualifySelectionProduction(id, owner, production, lookup)
		}
	}
}

func (c *admissionClosure) qualifySelectionProduction(
	id, owner manifest.NamedResource, production *manifest.Production, lookup bool,
) {
	c.add(owner, admissionRole{needed: true, ordinary: true, dirty: c.states[owner].changed})
	if lookup {
		c.proposal.lookups = append(c.proposal.lookups, owner)
	}
	if !c.proposal.Request.construction {
		return
	}
	c.requestProducer(id, production)
}

func (c *admissionClosure) admitMatchedMaterializer(
	id manifest.NamedResource,
	demand *materializerDemand,
) bool {
	output := c.materializerOutputMatches(id, demand.target)
	if !output.include {
		return false
	}
	c.admitMaterializer(id, demand)
	if demand.lookup && id.KindInfo().Structural {
		c.proposal.lookups = append(c.proposal.lookups, id)
	}
	return !output.complete
}

// LookupPackageMatches consumes source reach evidence and the qualified producer's namespace policy.
func LookupPackageMatches(
	target manifest.NamedResource, policy *manifest.EmissionPolicy, source manifest.NamedResource, reachable bool,
) bool {
	return reachable && source.KindInfo().SourceArtifact && policy != nil && policy.MatchesNamespace(target)
}

func (c *admissionClosure) lookupPackageSource(id, target manifest.NamedResource, facts *OccurrenceFacts) bool {
	if !id.KindInfo().Structural {
		return false
	}
	source, _, applies := facts.SourcePath()
	if !LookupPackageMatches(target, facts.Policy, source, applies) {
		return false
	}
	bound := c.boundSource(source)
	if bound.PendingGeneration || bound.Input != nil && bound.Input.Provider == store.ProviderGenerator {
		return true
	}
	generated := func(production manifest.Production) bool {
		return production.Declaration != (manifest.NamedResource{}) && production.Declaration != source
	}
	record, _ := c.facts.SelectionIndex().Lookup(source)
	return slices.ContainsFunc(c.productions(source), generated) || slices.ContainsFunc(record.Productions, generated)
}

func (c *admissionClosure) lookupEligible(id manifest.NamedResource) bool {
	if c.incoming(id).Obj != nil && c.owner != (manifest.NamedResource{}) && c.owner != id {
		return !c.outsideEntry(c.owner, id)
	}
	record, _ := c.indexedRecord(id)
	independent := Root(*c.capturedRoot(id)).InitialIndependent
	var owner manifest.NamedResource
	if !independent {
		owner = record.BuilderOwner
		if owner == (manifest.NamedResource{}) {
			owner, _ = c.session.store.Parent(id, c.facts.SelectionIndex())
		}
	}
	return c.scopes.scopes.lookupAllowsCandidate(id, owner, func() bool {
		return independent || owner == (manifest.NamedResource{}) && c.objects.GetDeclaration(id) != nil
	}, func() []manifest.Production {
		return c.productions(id)
	})
}

func (c *admissionClosure) qualifyLookupInputs() {
	for i := range c.proposal.Request.LookupSources {
		source := &c.proposal.Request.LookupSources[i]
		if source.Materializer != (manifest.NamedResource{}) {
			c.demandMaterialization(source.Materializer)
			continue
		}
		productions := c.productions(source.Source)
		for j := range productions {
			owner := productions[j].Materializer(source.Source)
			if owner != (manifest.NamedResource{}) && owner != source.Source {
				c.demandMaterialization(owner)
			}
		}
	}
	if c.scopes.scopes != nil && !c.context.Frozen {
		for owner, scope := range *c.scopes.scopes {
			if !scope.Current() && c.object(owner) != nil {
				c.add(owner, admissionRole{needed: true, ordinary: true})
			}
		}
	}
}
