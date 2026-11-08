package selection

import (
	"path/filepath"
	"slices"

	"github.com/home-operations/flate/internal/store"
	"github.com/home-operations/flate/pkg/manifest"
)

func (c *admissionClosure) rootEvidence(id manifest.NamedResource) RootResult {
	facts := c.rootSupport(id)
	facts.Needed = c.role(id).needed
	return Root(facts)
}

func (c *admissionClosure) rootSupport(id manifest.NamedResource) RootEvidence {
	facts := *c.capturedRoot(id)
	if !facts.Captured {
		return facts
	}
	record := c.record(id)
	facts.Record = RootRecordFrom(&record)
	facts.Declaration = c.objects.GetDeclaration(id)
	facts.Incoming = c.incoming(id).Obj != nil
	object, policy := c.objects.Lookup(id)
	facts.Supplied = !facts.Incoming && object != nil && policy.Origin == manifest.SuppliedDeclaration &&
		facts.Declaration == object &&
		manifest.AdmitsRoot(object)
	if facts.Supplied {
		facts.Stale = facts.Stale || !c.proposal.reads.Valid()
		return facts
	}
	if !Root(facts).NeedsOwnerSupport && !facts.Scope.NativeEntry {
		return facts
	}
	facts.Productions = c.productions(id)
	facts.LiveProductions = c.objects.Productions(id, &currentRootIndex)
	facts.StaticOwner = c.states[id].staticOwner
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
		location := c.boundSource(application.Source)
		facts.Scope.LocalApplication = !location.Pinned && !location.External
		facts.Scope.Claim = c.fileCoordinate(filepath.Join(location.Origin, application.Path)) + "/"
	}
	if Root(facts).NeedsOwnerSupport {
		c.captureRootOwners(&facts)
		facts.CoveringOwners = c.ownerCandidates
	}
	facts.Stale = facts.Stale || !c.proposal.reads.Valid()
	return facts
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

func (c *admissionClosure) rootOwnerFacts(object manifest.BaseManifest) RootCoveringOwner {
	facts := RootCoveringOwner{Object: object}
	if object == nil {
		return facts
	}
	facts.Identity = object.Named()
	source, path, applies := object.SourcePath()
	if !applies {
		return facts
	}
	if source.Name == "" {
		source = manifest.BootstrapSourceID
	}
	facts.Source = source
	_, input, _ := c.delta(source)
	if input != nil {
		facts.Input = input
		facts.Root = input.OriginPath(input.LocalPath)
		facts.Path = input.OriginPath(filepath.Join(input.LocalPath, path))
		if !containsPath(facts.Root, facts.Path) {
			facts.Path = ""
		}
	}
	return facts
}

func (c *admissionClosure) hasOwner(matches func(*RootCoveringOwner) bool) bool {
	if !c.ownersKnown {
		c.collectOwners()
	}
	for i := range c.ownerCandidates {
		owner := &c.ownerCandidates[i]
		if owner.Object == nil || !manifest.KindOf(owner.Object).Structural {
			continue
		}
		if matches(owner) && slices.ContainsFunc(c.productions(owner.Identity), func(p manifest.Production) bool {
			return p.Owner != (manifest.NamedResource{}) && p.Owner != owner.Identity
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
			c.ownerCandidates = append(c.ownerCandidates, c.rootOwnerFacts(object))
		}
	}
	c.ownersKnown = true
}
