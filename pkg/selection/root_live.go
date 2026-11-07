package selection

import (
	"path/filepath"
	"slices"

	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

func (c *admissionClosure) rootEvidence(id manifest.NamedResource) RootResult {
	facts := c.facts.RootEvidence(id, c.proposal.reads)
	if !facts.Captured {
		return Root(facts)
	}
	facts.Record = c.record(id)
	facts.Productions = c.productions(id)
	facts.LiveProductions = c.objects.Productions(id, &currentRootIndex)
	facts.Declaration = c.objects.GetDeclaration(id)
	facts.Incoming = c.incoming(id).Obj != nil
	facts.Needed = c.role(id).needed
	facts.StaticOwner = c.states[id].staticOwner
	object, policy := c.objects.Lookup(id)
	facts.Supplied = !facts.Incoming && object != nil && policy.Origin == manifest.SuppliedDeclaration &&
		facts.Declaration == object &&
		manifest.AdmitsRoot(object)
	if facts.Supplied {
		facts.Stale = facts.Stale || !c.proposal.reads.Valid()
		return Root(facts)
	}
	facts.DeclarationSource = c.declarationSource(id)
	for i, source := range [...]manifest.NamedResource{c.source(id), manifest.BootstrapSourceID} {
		_, input, _ := c.delta(source)
		support := RootSourceSupport{
			Source:    source,
			Allowed:   c.objects.SourceAdmission(id, source),
			Available: input != nil,
		}
		if input != nil {
			support.Path, support.Origin = input.LocalPath, input.OriginPath(input.LocalPath)
		}
		facts.Sources[i] = support
	}
	if application, valid := manifest.ApplicationFor(c.catalogObject(id)); valid {
		facts.Scope.Application = true
		facts.Scope.ApplicationSource, facts.Scope.ApplicationPath = application.Source, application.Path
		location := c.sourceView(application.Source, c.context.Root)
		facts.Scope.LocalApplication = !location.Pinned && !location.External
		facts.Scope.Claim = c.fileCoordinate(filepath.Join(location.Origin, application.Path)) + "/"
	}
	c.captureRootOwners(&facts)
	facts.CoveringOwners = c.ownerCandidates
	facts.Stale = facts.Stale || !c.proposal.reads.Valid()
	return Root(facts)
}

func (c *admissionClosure) captureRootOwners(facts *RootEvidence) {
	if facts.Record.Path != "" && !c.ownersKnown {
		c.collectOwners()
	}
	pending := c.pendingRootOwners(facts)
	for cursor := 0; cursor < len(pending); cursor++ {
		owner := pending[cursor]
		index := slices.IndexFunc(c.ownerCandidates, func(f RootCoveringOwner) bool { return f.Identity == owner })
		if index < 0 {
			c.ownerCandidates = append(c.ownerCandidates, RootCoveringOwner{Identity: owner})
			index = len(c.ownerCandidates) - 1
		}
		candidate := &c.ownerCandidates[index]
		if facts.Identity.KindInfo().Parent || candidate.Object != nil {
			candidate.Productions = c.productions(owner)
		}
		if facts.Occurrence.Declaration != (OccurrenceID{}) &&
			slices.ContainsFunc(facts.Productions, func(p manifest.Production) bool { return p.Owner == owner }) {
			candidate.Output, _ = c.objects.GetArtifact(owner, store.ArtifactContent).(*store.Emission)
			candidate.Applications, candidate.Replaced = c.accepted[owner], c.replace[owner]
		}
		if !facts.Identity.KindInfo().Parent {
			continue
		}
		for i := range candidate.Productions {
			production := &candidate.Productions[i]
			ancestor := production.Materializer(owner)
			if ancestor != (manifest.NamedResource{}) && ancestor != facts.Identity &&
				!slices.Contains(pending, ancestor) {
				pending = append(pending, ancestor)
			}
		}
	}
}

func (c *admissionClosure) pendingRootOwners(facts *RootEvidence) []manifest.NamedResource {
	var pending []manifest.NamedResource
	for i := range facts.Productions {
		production := &facts.Productions[i]
		for _, owner := range [...]manifest.NamedResource{production.Materializer(facts.Identity), production.Owner} {
			if owner != (manifest.NamedResource{}) && owner != facts.Identity && !slices.Contains(pending, owner) {
				pending = append(pending, owner)
			}
		}
	}
	for i := range c.ownerCandidates {
		candidate := &c.ownerCandidates[i]
		if rootCoversPath(facts, candidate) && !slices.Contains(pending, candidate.Identity) {
			pending = append(pending, candidate.Identity)
		}
	}
	return pending
}

//nolint:gochecknoglobals // Empty immutable index requests current store productions without indexed ancestry.
var currentRootIndex manifest.DiscoveryIndex

func (c *admissionClosure) rootOwnerFacts(id manifest.NamedResource, object manifest.BaseManifest) RootCoveringOwner {
	facts := RootCoveringOwner{Identity: id, Object: object}
	if object == nil {
		return facts
	}
	source, path, applies := object.SourcePath()
	if !applies {
		return facts
	}
	if source.Name == "" {
		source = manifest.BootstrapSourceID
	}
	facts.Source = source
	location := c.sourceView(source, c.context.Root)
	facts.Pinned, facts.External = location.Pinned, location.External
	facts.Claim = c.fileCoordinate(filepath.Join(location.Origin, path)) + "/"
	_, input, _ := c.delta(source)
	if input != nil {
		facts.Input = input
		facts.Root = input.OriginPath(input.LocalPath)
		facts.Path = input.OriginPath(filepath.Join(input.LocalPath, path))
	}
	return facts
}

func (c *admissionClosure) hasOwner(matches func(manifest.BaseManifest) bool) bool {
	if !c.ownersKnown {
		c.collectOwners()
	}
	for i := range c.ownerCandidates {
		owner := c.ownerCandidates[i].Object
		if owner == nil || !manifest.KindOf(owner).Structural {
			continue
		}
		if matches(owner) && slices.ContainsFunc(c.productions(owner.Named()), func(p manifest.Production) bool {
			return p.Owner != (manifest.NamedResource{}) && p.Owner != owner.Named()
		}) {
			return true
		}
	}
	return false
}

func (c *admissionClosure) collectOwners() {
	var prospective map[manifest.NamedResource]bool
	include := func(id manifest.NamedResource, productions []manifest.Production) {
		if id.KindInfo().Structural && slices.ContainsFunc(productions, func(p manifest.Production) bool {
			return p.Owner != (manifest.NamedResource{}) && p.Owner != id
		}) {
			if prospective == nil {
				prospective = make(map[manifest.NamedResource]bool)
			}
			id.Namespace = ""
			prospective[id] = true
		}
	}
	for source := range c.proposal.Request.Changes.Sources() {
		delta, _, _ := c.proposal.Request.Changes.ForSource(source)
		for id, input := range delta.Inputs(manifest.NamedResource{}) {
			include(id, input.Productions)
		}
	}
	for _, records := range c.accepted {
		for id := range records {
			include(id, records[id].Productions)
		}
	}
	for info := range manifest.Kinds() {
		if !info.Structural {
			continue
		}
		candidates := c.objects.ListProductionCandidates(info.Group, info.Kind, c.facts.SelectionIndex(), prospective)
		for _, object := range candidates {
			c.ownerCandidates = append(c.ownerCandidates, c.rootOwnerFacts(object.Named(), object))
		}
	}
	c.ownersKnown = true
}
