package selection

import (
	"cmp"
	"encoding/json"
	"path/filepath"
	"slices"

	"github.com/home-operations/flate/internal/engine/store"
	"github.com/home-operations/flate/pkg/manifest"
)

func (c *admissionClosure) applicationDeclaration(id manifest.NamedResource) ([]manifest.NamedResource, OccurrenceID) {
	var chain []manifest.NamedResource
	for id != (manifest.NamedResource{}) && !id.ReportHidden() && !slices.Contains(chain, id) {
		chain = append(chain, id)
		occurrence, known := c.catalogOccurrence(id)
		if _, _, applies := occurrence.SourcePath(); known && applies {
			location := c.boundSource(c.source(id))
			if location.Pinned || location.External {
				break
			}
			input := occurrence.Mounted
			if input != nil && c.context.EntryPath != "" {
				return chain, occurrence.Declaration
			}
			break
		}
		root := c.rootEvidence(id)
		id = cmp.Or(root.Owner, c.declarationEvidence(id).owner)
	}
	return nil, OccurrenceID{}
}

func (c *admissionClosure) prepareApplicationDemand(route *EntryRoute, literalChanged bool) {
	named := slices.ContainsFunc(c.session.request.Roots, func(id manifest.NamedResource) bool { return id.Name != "" })
	switch {
	case route.Bound && route.Winners == nil && (!route.Changed || !c.session.request.Changed):
		return
	case named:
		if !route.possibleSelectedApplication() {
			return
		}
	case !c.session.request.Changed || !c.session.request.Matches(manifest.NamedResource{}):
		return
	case literalChanged || !route.Projected && !route.ProjectedPatches:
		return
	}
	for _, identity := range c.identities {
		chain, query := c.applicationDemand(identity.id, route, named)
		if query == (OccurrenceID{}) || !c.acquireApplication(chain, query, route) {
			continue
		}
		if c.entryPending == nil {
			c.entryPending = map[manifest.NamedResource]bool{}
		}
		for _, owner := range chain {
			c.entryPending[owner] = true
		}
	}
}

func (c *admissionClosure) applicationDemand(
	id manifest.NamedResource, route *EntryRoute, named bool,
) ([]manifest.NamedResource, OccurrenceID) {
	if named && !c.session.RequestsNamedRoot(id) {
		return nil, OccurrenceID{}
	}
	if !named && (!c.role(id).needed || !c.states[id].changed) {
		return nil, OccurrenceID{}
	}
	chain, query := c.applicationDeclaration(id)
	if query == (OccurrenceID{}) || named {
		return chain, query
	}
	if !containsPath(c.context.EntryPath, filepath.Join(query.Source, query.File)) ||
		!route.ProjectedPatches && !c.fileChanged(c.source(chain[len(chain)-1]), query.File) {
		return nil, OccurrenceID{}
	}
	return chain, query
}

func (r *EntryRoute) possibleSelectedApplication() bool {
	return r.Selected || r.Changed || !r.Bound && r.Projected
}

func (c *admissionClosure) acquireApplication(
	chain []manifest.NamedResource,
	query OccurrenceID,
	route *EntryRoute,
) bool {
	id, declaration := chain[0], chain[len(chain)-1]
	if !route.Bound && route.Winners == nil {
		source := c.source(declaration)
		occurrence, _ := c.catalogOccurrence(declaration)
		c.acquire(Demand{Identity: id, Application: query, Source: source, Artifact: occurrence.Mounted})
		return true
	}
	if route.Changed && c.session.request.Changed {
		c.add(id, admissionRole{needed: true, selected: true, dirty: true})
	}
	var producer manifest.NamedResource
	if route.Winners != nil {
		producer = (*route.Winners)[query]
	}
	if producer == (manifest.NamedResource{}) || producer == declaration {
		return false
	}
	for _, owner := range chain[1:] {
		c.add(owner, admissionRole{needed: true, ordinary: true, dirty: c.states[owner].changed})
	}
	c.add(producer, admissionRole{needed: true, ordinary: true, dirty: c.states[producer].changed})
	output, _ := c.objects.GetArtifact(producer, store.ArtifactContent).(*store.Emission)
	status, _ := c.objects.GetStatus(producer)
	return (output == nil || !output.Complete || !output.OutputReads.Valid()) &&
		status.Outcome() == store.OutcomePending
}

func (c *admissionClosure) acquireEntryPath() bool {
	if c.pendingEntrySources() {
		return true
	}
	application := c.proposal.entry.Application
	root := manifest.NewEntryKustomization(application.Source, application.Path, manifest.NamedResource{}).Named()
	_, acquired := c.acquire(Demand{Identity: root, Source: application.Source, Root: application.Path})
	return !acquired
}

func (c *admissionClosure) demandEntryPath(
	id, source manifest.NamedResource,
	record *manifest.DiscoveryRecord,
) bool {
	route := c.proposal.entry
	if route == nil || route.Application == nil || route.Selected || c.proposal.entrySupported() {
		return false
	}
	if !c.context.PlainEntry || !containsPath(c.context.EntryPath, record.Path) {
		return false
	}
	if c.session.request.Changed && !route.Projected && !c.fileChanged(c.source(id), record.File) {
		return false
	}
	location := c.boundSource(source)
	input := location.Input
	if !input.WorkingTree() || !input.Reads.Valid() ||
		input.Provider == store.ProviderInput || input == location.ConfiguredInput {
		return false
	}
	if input == c.boundSource(manifest.BootstrapSourceID).Input {
		return false
	}
	return c.acquireEntryPath()
}

func parseApplication(application string) (manifest.BaseManifest, bool) {
	var document map[string]any
	if json.Unmarshal([]byte(application), &document) != nil {
		return nil, false
	}
	parsed, err := manifest.ParseDoc(document, manifest.ParseDocOptions{})
	return parsed.Object, err == nil
}

func (c *admissionClosure) rootMatches(root string) bool {
	var identities []manifest.NamedResource
	for i := range c.context.Roots {
		if facts := &c.context.Roots[i]; facts.Path == root {
			identities = facts.Identities
			break
		}
	}
	matches := c.rootRequired(root) || slices.ContainsFunc(
		identities,
		func(id manifest.NamedResource) bool { return c.session.request.Matches(id) },
	)
	if !matches {
		delta, _, _ := c.delta(manifest.BootstrapSourceID)
		for id, input := range c.comparisonInputs(delta, manifest.NamedResource{}) {
			if c.session.request.Matches(id) &&
				slices.ContainsFunc(input.Declarations, func(file string) bool {
					return containsPath(c.context.Root, filepath.Join(c.context.Root, root)) &&
						containsPath(
							filepath.Join(c.context.Root, root),
							filepath.Join(c.context.Root, filepath.FromSlash(file)),
						)
				}) {
				matches = true
				break
			}
		}
	}

	return matches
}
