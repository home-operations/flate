package selection

import (
	"context"
	"iter"
	"maps"
	"slices"
	"strings"

	"github.com/home-operations/flate/pkg/manifest"
)

// RefreshHistory MUST retain the settled identities while capturing each receipt's current authority.
func (s *Session) RefreshHistory(
	ctx context.Context,
	previous *Admission,
	request PrepareRequest, //nolint:gocritic // The proposal retains the caller's immutable request snapshot.
) (*Admission, error) {
	if err := ctx.Err(); err != nil {
		return nil, err //nolint:wrapcheck // Cancellation preserves the caller's error identity.
	}
	var preparation *Preparation
	if request.Preparation != nil {
		preparation = *request.Preparation
	}
	if preparation == nil {
		preparation = &Preparation{closure: admissionClosure{session: s, facts: s.facts}}
		if request.Preparation != nil {
			*request.Preparation = preparation
		}
	}
	if preparation.closure.session != s {
		return nil, manifest.NewError(manifest.ErrInput, nil, "admission preparation belongs to a different session")
	}
	currentSource := s.facts.SourceFacts(manifest.BootstrapSourceID, nil)
	if request.Changes == nil {
		request.Changes = currentSource.Comparison
	}
	proposal := &Admission{
		Needed: previous.Needed, Selected: previous.Selected, Dirty: previous.Dirty, Ordinary: previous.Ordinary,
		Blocked: previous.Blocked, Namespaces: previous.Namespaces,
		EntryReceipt: previous.EntryReceipt, producers: previous.producers, producerOwner: previous.producerOwner,
		Revision: s.Snapshot().version(), Request: request, preparation: preparation, entry: s.entry.Load(),
	}
	if request.Preparation == nil {
		proposal.local = preparation
	}
	if proposal.entry != nil && proposal.entry.Bound {
		proposal.EntryReceipt = nil
	}
	proposal.reads = &proposal.owned
	owner := manifest.NamedResource{}
	if request.Reads != nil {
		owner = request.Reads.ID()
	}
	proposal.reads.Init(s.store, owner, nil)
	if previous.entry != proposal.entry || previous.entryReads != nil && !proposal.entrySupported() {
		proposal.Retire()
		return s.Prepare(ctx, request)
	}
	preparation.acceptAcquired(request.Acquired)
	closure := admissionClosure{
		session: s, facts: s.facts, objects: proposal.reads, proposal: proposal,
		configuredInput: currentSource.ConfiguredInput,
	}
	for _, id := range proposal.Needed {
		if err := ctx.Err(); err != nil {
			proposal.Retire()
			return nil, err //nolint:wrapcheck // Cancellation preserves the caller's error identity.
		}
		closure.requestHistory(id)
	}
	return proposal, nil
}

func (c *admissionClosure) requestHistory(id manifest.NamedResource) {
	if !id.KindInfo().Render {
		return
	}
	object := c.object(id)
	if object == nil {
		return
	}
	record, _ := c.facts.SelectionIndex().Lookup(id)
	source := c.facts.SourceFacts(manifest.BootstrapSourceID, c.proposal.reads)
	if record.StaticOutput && c.configuredInput == source.Input {
		return
	}
	c.acquire(Demand{Identity: id, History: true, Witness: object, Artifact: source.Input})
}

func (c *admissionClosure) requestEntryReceipt() {
	route := c.proposal.entry
	if len(c.historyOwners) == 0 || route == nil || route.Application == nil {
		return
	}
	entry := route.Application
	native := manifest.NewEntryKustomization(entry.Source, entry.Path, manifest.NamedResource{}).Named()
	if (len(c.historyOwners[native]) != 0 || len(c.historyOwners[route.Previous]) != 0) &&
		!c.proposal.entrySupported() {
		c.proposal.EntryReceipt = entry
	}
}

func (c *admissionClosure) readHistory() {
	c.accepted = map[manifest.NamedResource]map[manifest.NamedResource]manifest.DiscoveryRecord{}
	c.replace = map[manifest.NamedResource]bool{}
	for owner, history := range c.history {
		if history.Records == nil {
			continue
		}
		c.replace[owner] = history.Complete
		records := map[manifest.NamedResource]manifest.DiscoveryRecord{}
		//nolint:gocritic // The iterator supplies immutable value snapshots.
		for id, record := range history.Records {
			c.resolveHistoricalRecord(owner, &record)
			records[id] = record
		}
		c.accepted[owner] = records
	}
}

func (c *admissionClosure) indexedRecord(id manifest.NamedResource) (manifest.DiscoveryRecord, bool) {
	record, indexed := c.facts.SelectionIndex().Lookup(id)
	if !indexed && id.Namespace != "" {
		if occurrence, ok := c.catalogOccurrence(id); ok && occurrence.Authored.Namespace == "" {
			bare := id
			bare.Namespace = ""
			record, _ = c.facts.SelectionIndex().Lookup(bare)
		}
	}
	return record, indexed
}

func (c *admissionClosure) record(id manifest.NamedResource) manifest.DiscoveryRecord {
	record, _ := c.indexedRecord(id)
	return c.declarationRecord(id, &record)
}

func (c *admissionClosure) contentRecord(id manifest.NamedResource) manifest.DiscoveryRecord {
	record, indexed := c.indexedRecord(id)
	var entry *admissionCatalogEntry
	if c.catalog != nil {
		entry = c.catalog.entries[id]
	}
	c.recordRoots(id, &record, indexed, entry)
	if c.nativeExcluded(id, entry) {
		record.ContentRoots = nil
	}
	return c.declarationRecord(id, &record)
}

func (c *admissionClosure) declarationRecord(
	id manifest.NamedResource,
	record *manifest.DiscoveryRecord,
) manifest.DiscoveryRecord {
	if incoming := c.incoming(id); incoming.Obj != nil {
		record.Origin = incoming.Origin
	} else if !record.Origin.Policy(id, nil).Root && c.session.store.Policy(id).Root {
		record.Origin = manifest.RepositoryDeclaration
	}
	if len(c.accepted) == 0 {
		return *record
	}
	return c.completeRecord(&id, record)
}

func (c *admissionClosure) historicalReferences(id manifest.NamedResource) iter.Seq[manifest.Reference] {
	return func(yield func(manifest.Reference) bool) {
		for _, records := range c.accepted {
			for _, ref := range records[id].Refs {
				reference := c.historicalReference(id, ref)
				if !yield(reference) {
					return
				}
			}
		}
	}
}

func (c *admissionClosure) completeRecord(
	id *manifest.NamedResource,
	record *manifest.DiscoveryRecord,
) manifest.DiscoveryRecord {
	if record.File == "" && len(record.Productions) != 0 && len(c.productions((*id))) == 0 {
		record.Refs, record.ContentRoots, record.Parent = nil, nil, manifest.NamedResource{}
	}
	for owner, records := range c.accepted {
		historical, ok := records[(*id)]
		if !ok {
			continue
		}
		record.Origin = historical.Origin
		record.Refs = append(slices.Clone(record.Refs), historical.Refs...)
		// An entry's exact current coverage MUST supersede historical subtree coverage.
		if record.ContentRoots != nil && id.ReportHidden() {
			continue
		}
		record.ContentRoots = maps.Clone(record.ContentRoots)
		if record.ContentRoots == nil {
			record.ContentRoots = map[manifest.NamedResource][]string{}
		}
		for source, roots := range historical.ContentRoots {
			if owner != (*id) && c.replace[(*id)] {
				continue
			}
			record.ContentRoots[source] = append(slices.Clone(record.ContentRoots[source]), roots...)
		}
	}
	return (*record)
}

func (c *admissionClosure) resolveHistoricalRecord(owner manifest.NamedResource, record *manifest.DiscoveryRecord) {
	record.ContentRoots = maps.Clone(record.ContentRoots)
	for source, roots := range record.ContentRoots {
		resolved := make([]string, 0, len(roots))
		for _, root := range roots {
			base, ok, _ := c.resolveRoot(source, root)
			if !ok {
				c.replace[owner] = false
				base = root
				if strings.Contains(root, "/../") || strings.HasSuffix(root, "/..") {
					base = "."
				}
			}
			resolved = append(resolved, base)
		}
		record.ContentRoots[source] = resolved
	}
}

func (c *admissionClosure) historicalReference(id, ref manifest.NamedResource) manifest.Reference {
	reference := manifest.Reference{NamedResource: ref, Content: true}
	if object := c.object(id); object != nil {
		for declared := range manifest.References(object, manifest.ReferenceRelations) {
			if declared.NamedResource == ref {
				reference = declared
				if !declared.Ordering {
					break
				}
			}
		}
	}
	return reference
}

//nolint:gocritic // Acquisition keeps the resolved path, availability, and completion separate.
func (c *admissionClosure) resolveRoot(source manifest.NamedResource, root string) (string, bool, bool) {
	facts := c.boundSource(source)
	acquired, known := c.acquire(
		Demand{Source: source, Root: root, History: true, Artifact: facts.Input, Witness: facts.Declaration},
	)
	return acquired.Root, known && acquired.Available, known
}

// Construction coverage MUST expire when its native package leaves the construction frontier.
func (c *admissionClosure) nativeExcluded(id manifest.NamedResource, entry *admissionCatalogEntry) bool {
	if c.catalog != nil {
		return entry != nil && entry.nativeExcluded
	}
	if !id.ReportHidden() {
		return false
	}
	roots := c.context.Roots
	if len(roots) == len(c.nativeRoots) && (len(roots) == 0 || &roots[0] == &c.nativeRoots[0]) {
		return c.excludedNative[id]
	}
	c.nativeRoots = roots
	clear(c.excludedNative)
	for i := range roots {
		root := &roots[i]
		if Root(RootEvidence{Native: root}).Construction {
			continue
		}
		if c.excludedNative == nil {
			c.excludedNative = map[manifest.NamedResource]bool{}
		}
		owner := manifest.NewEntryKustomization(manifest.BootstrapSourceID, root.Path, manifest.NamedResource{}).Named()
		c.excludedNative[owner] = true
	}
	return c.excludedNative[id]
}

func (c *admissionClosure) recordRoots(
	id manifest.NamedResource,
	record *manifest.DiscoveryRecord,
	indexed bool,
	entry *admissionCatalogEntry,
) {
	if record.ContentRoots != nil {
		return
	}
	if c.catalog != nil && indexed {
		if entry != nil && entry.occurrence.CatalogKnown && len(entry.occurrence.Roots) != 0 {
			record.ContentRoots = entry.contentRoots
		}
		return
	}
	source, roots := c.facts.OccurrenceRoots(id)
	if len(roots) == 0 {
		return
	}
	if source.Name == "" || source.Kind == "" {
		source = manifest.BootstrapSourceID
	}
	record.ContentRoots = map[manifest.NamedResource][]string{source: roots}
}
