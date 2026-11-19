package selection

import (
	"slices"
	"strings"

	"github.com/home-operations/flate/internal/engine/store"
	"github.com/home-operations/flate/pkg/manifest"
)

func (c *admissionClosure) rootEvidence(id manifest.NamedResource) RootResult {
	facts := c.rootSupport(id)
	facts.Needed = c.role(id).needed
	return Root(facts)
}

func (c *admissionClosure) rootSupport(id manifest.NamedResource) RootEvidence {
	physical := c.capturedRoot(id)
	captured := c.capturedRoots[id]
	if !captured.supported {
		facts := *physical
		record := c.record(id)
		facts.Record = RootRecordFrom(&record)
		facts.Incoming = c.incoming(id).Obj != nil
		facts.Productions = c.productions(id)
		facts.StaticOwner = c.states[id].staticOwner
		facts.DeclarationSource = c.declarationSource(id)
		object := c.catalogObject(id)
		if child := c.incoming(id); child.Obj != nil {
			object = child.Obj
		}
		c.captureRootApplication(&facts, object)
		facts = CaptureRoot(
			facts,
			c.rootReader(),
			c.facts.SelectionIndex(),
		)
		c.captureRootApplication(&facts, object)
		captured.support, captured.supported = facts, true
	}
	facts := captured.support
	facts.Stale = facts.Stale || !c.proposal.reads.Valid()
	return facts
}

type admissionRootReader struct {
	admissionReader

	closure *admissionClosure
}

func (c *admissionClosure) rootReader() *admissionRootReader {
	c.rootReads.admissionReader, c.rootReads.closure = c.objects, c
	return &c.rootReads
}

func (r *admissionRootReader) GetDeclaration(id manifest.NamedResource) manifest.BaseManifest {
	if child := r.closure.incoming(id); child.Obj != nil {
		return child.Obj
	}
	return r.admissionReader.GetDeclaration(id)
}

func (r *admissionRootReader) ListProductionCandidates(
	group, kind string,
	index *manifest.DiscoveryIndex,
	prospective map[manifest.NamedResource]bool,
) []manifest.BaseManifest {
	candidates := r.admissionReader.ListProductionCandidates(group, kind, index, prospective)
	for i, object := range candidates {
		candidates[i] = r.closure.object(object.Named())
	}
	candidates = slices.DeleteFunc(candidates, func(object manifest.BaseManifest) bool { return object == nil })
	for i := range r.closure.generation {
		object := r.closure.generation[i].Obj
		if object == nil {
			continue
		}
		id := object.Named()
		bare := id
		bare.Namespace = ""
		if id.Group != group || id.Kind != kind || !prospective[id] && !prospective[bare] ||
			slices.ContainsFunc(candidates, func(candidate manifest.BaseManifest) bool {
				return candidate.Named() == id
			}) {
			continue
		}
		candidates = append(candidates, object)
	}
	slices.SortFunc(candidates, func(a, b manifest.BaseManifest) int { return a.Named().Compare(b.Named()) })
	return candidates
}

func (r *admissionRootReader) Productions(
	id manifest.NamedResource,
	index *manifest.DiscoveryIndex,
) []manifest.Production {
	if index == &currentRootIndex {
		productions, _ := r.closure.mergeHistoricalProductions(id, r.admissionReader.Productions(id, index))
		return productions
	}
	return r.closure.productions(id)
}

func (r *admissionRootReader) GetArtifact(id manifest.NamedResource, fields store.ArtifactRead) store.Artifact {
	if id.KindInfo().SourceArtifact || id == manifest.BootstrapSourceID {
		_, input, _ := r.closure.delta(id)
		if input == nil {
			return nil
		}
		return input
	}
	return r.admissionReader.GetArtifact(id, fields)
}

func (r *admissionRootReader) RootOwners() *[]RootCoveringOwner {
	if !r.closure.ownersKnown {
		r.closure.collectOwners()
	}
	if r.closure.ownerCandidates == nil {
		r.closure.ownerCandidates = []RootCoveringOwner{}
	}
	return &r.closure.ownerCandidates
}

func (r *admissionRootReader) rootReplacement(
	id manifest.NamedResource,
) (map[manifest.NamedResource]manifest.DiscoveryRecord, bool) {
	return r.closure.accepted[id], r.closure.replace[id]
}

func (r *admissionRootReader) rootOwnerStorage(id manifest.NamedResource) []RootCoveringOwner {
	captured := r.closure.capturedRoots[id]
	if captured == nil || captured.ownersUsed {
		return nil
	}
	// Earlier evidence MUST keep its owner snapshot after a recapture.
	captured.ownersUsed = true
	return captured.owners[:0]
}

// Owner claim keys MUST stay within the current prerequisite snapshot.
type ownerClaimKey struct {
	object           manifest.BaseManifest
	identity, source manifest.NamedResource
	input            *store.SourceArtifact
	root, path       string
}

func (c *admissionClosure) rootOwnerClaims(owner *RootCoveringOwner) *manifest.DiscoveryIndex {
	key := ownerClaimKey{
		object: owner.Object, identity: owner.Identity, source: owner.Source,
		input: owner.Input, root: owner.Root, path: owner.Path,
	}
	if claims, ok := c.ownerClaims[key]; ok {
		return claims
	}
	owners := [1]RootCoveringOwner{*owner}
	index := c.facts.SelectionIndex()
	captureOwnerClaims(owners[:], index, c.context.Root)
	claims := owners[0].claims
	if claims == index {
		return claims
	}
	if c.ownerClaims == nil {
		c.ownerClaims = map[ownerClaimKey]*manifest.DiscoveryIndex{}
	}
	c.ownerClaims[key] = claims
	return claims
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
		for id, input := range c.comparisonInputs(delta, manifest.NamedResource{}) {
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
		candidates := c.rootReader().ListProductionCandidates(
			info.Group, info.Kind, c.facts.SelectionIndex(), prospective,
		)
		for _, object := range candidates {
			c.ownerCandidates = append(c.ownerCandidates, rootOwnerFact(object, c.rootReader()))
		}
	}
	root := c.context.Root
	if input, _ := c.rootReader().GetArtifact(manifest.BootstrapSourceID, store.ArtifactContent).(*store.SourceArtifact); input != nil {
		root = input.LocalPath
	}
	captureOwnerClaims(c.ownerCandidates, c.facts.SelectionIndex(), root)
	c.ownersKnown = true
}

func (c *admissionClosure) captureRootApplication(facts *RootEvidence, object manifest.BaseManifest) {
	application, valid := manifest.ApplicationFor(object)
	if !valid {
		return
	}
	facts.Scope.Application = true
	facts.Scope.ApplicationSource, facts.Scope.ApplicationPath = application.Source, application.Path
	location := c.boundSource(application.Source)
	facts.Scope.LocalApplication = !location.Pinned && !location.External
	previousClaim := facts.Scope.Claim
	facts.Scope.Claim = ""
	coordinate, known := location.Input.IndexPath(c.context.Root, application.Path)
	if !known {
		facts.Scope.LocalApplication = false
		return
	}
	claims := facts.Scope.Claims
	if len(claims) == 0 || len(claims[coordinate+"/"]) == 0 {
		return
	}
	if previousClaim != "" && strings.TrimSuffix(previousClaim, "/") == coordinate {
		facts.Scope.Claim = previousClaim
	} else {
		facts.Scope.Claim = coordinate + "/"
	}
}
