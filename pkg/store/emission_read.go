package store

import (
	"cmp"
	"slices"

	"github.com/home-operations/flate/pkg/manifest"
)

// ReportHidden classifies the coherent representative's auxiliary provenance.
func (s *Store) ReportHidden(id manifest.NamedResource) bool {
	return !s.Policy(id).Report
}

func (sh *shard) reportHiddenLocked(id manifest.NamedResource) bool {
	return !sh.policyLocked(id).Report
}

// EmissionOwner returns the generated producer supplying the representative for id.
// Static, hidden or absent representatives have an empty owner. This individual
// read does not form an atomic pair with a separate GetEmission call.
func (s *Store) EmissionOwner(id manifest.NamedResource) manifest.NamedResource {
	sh := s.shardFor(id)
	sh.mu.RLock()
	var view capture
	sh.captureLocked(id, new(sh.cells[id]), &view)
	sh.mu.RUnlock()
	if view.owner != nil {
		return view.owner.id
	}
	return manifest.NamedResource{}
}

// GetDeclaration returns the effective input authorized by static or accepted support.
//
//nolint:ireturn // The manifest contract requires polymorphic results.
func (s *Store) GetDeclaration(id manifest.NamedResource) manifest.BaseManifest {
	sh := s.shardFor(id)
	sh.mu.RLock()
	var view capture
	sh.captureLocked(id, new(sh.cells[id]), &view)
	sh.mu.RUnlock()
	return new(view).declaration()
}

// Discovery preserves a distinct authored File only for unchanged builder output;
// raw self evidence MUST NOT override another builder's transformed declaration.
func rawDeclarationAuthority(index *manifest.DiscoveryIndex, id manifest.NamedResource, classified bool) bool {
	record, _ := index.Lookup(id)
	self := record.BuilderOwner == (manifest.NamedResource{}) || record.BuilderOwner == id ||
		record.Authored == id && record.File != "" && record.Builder != "" && record.File != record.Builder
	return self && (classified || len(record.Productions) == 0) ||
		slices.ContainsFunc(record.Productions, func(p manifest.Production) bool {
			return p.Owner == (manifest.NamedResource{}) || self && p.Owner == id
		})
}

//nolint:ireturn // The manifest contract requires polymorphic results.
func (cv *capture) declaration() manifest.BaseManifest {
	c := *cv
	if c.independent || c.accepted || c.classified || c.owner != nil || c.child.Origin == manifest.SuppliedDeclaration {
		return c.child.Obj
	}
	return nil
}

// GetEmission borrows valid accepted inventory and binds an attempt to its current input.
// The captured document remains available while declaration authority is parked.
// Render identity is available through Emission.Replay.
// OutputReads preserves validity evidence for withdrawal without exposing invalid inventory.
// Returned slices and their contents MUST be copied before mutation.
//
//nolint:ireturn // The manifest contract requires polymorphic results.
func (s *Store) GetEmission(id manifest.NamedResource) (manifest.BaseManifest, map[string]any, Emission) {
	return s.getEmission(nil, id)
}

//nolint:ireturn // The manifest contract requires polymorphic results.
func (s *Store) getEmission(
	reads *ReadSet,
	id manifest.NamedResource,
) (manifest.BaseManifest, map[string]any, Emission) {
	var obj manifest.BaseManifest
	var view capture
	if reads != nil {
		obj = reads.GetDeclaration(id)
		view = reads.input(id).view
	}
	sh := s.shardFor(id)
	sh.mu.RLock()
	if reads == nil {
		sh.captureLocked(id, new(sh.cells[id]), &view)
	}
	var prev *Emission
	head := sh.cells[id].links
	if slot := sh.slots[id]; slot != nil && slot.visible.Load() {
		prev = slot.current.Load()
	}
	sh.mu.RUnlock()
	if reads == nil {
		obj = new(view).declaration()
	}
	e := Emission{
		Reads:    reads,
		input:    obj,
		inputDoc: view.child.Doc,
		previous: prev,
		inputHead: inputStamp{
			owner:      view.owner,
			generation: view.generation,
			revision:   view.revision,
			binding:    view.source,
		},
	}
	if head != nil {
		e.renderCache = &head.rendered
	}
	if prev != nil {
		e.OutputReads = prev.OutputReads
		if prev.OutputReads.Valid() {
			e.Children = prev.Children
			e.Manifests = prev.Manifests
			e.Unreadable = prev.Unreadable
			if reads == nil || prev.Reads == reads {
				e.Failures = prev.Failures
			}
			e.Render = prev.Render
		}
	}
	if obj == nil {
		return nil, view.child.Doc, e
	}
	if c := s.coverage[id]; c != nil {
		e.coverage = c.records
	}
	e.bindSource(s, obj, reads, id)
	return obj, view.child.Doc, e
}

// ReferenceFacts preserves visible data separately from accepted authority and relations.
type ReferenceFacts struct {
	Data                        manifest.Data
	Materializers               []manifest.NamedResource
	Generated, Offline, Private bool
	Accepted                    bool
	Productions                 []manifest.Production
	ReferenceData               []manifest.ReferenceData
}

// Productions merges discovery evidence with accepted reverse memberships into a read-only snapshot.
func (s *Store) Productions(id manifest.NamedResource, index *manifest.DiscoveryIndex) []manifest.Production {
	var result productionResult
	s.visitProductions(id, index, &result)
	return result.out
}

// IncomingProductions projects prospective evidence without the stored target's state.
//
//nolint:gocritic // The SDK query accepts a value snapshot independent of caller storage.
func (s *Store) IncomingProductions(child Child, index *manifest.DiscoveryIndex) []manifest.Production {
	result := productionResult{incoming: &child}
	s.visitProductions(child.Obj.Named(), index, &result)
	return result.out
}

// ReferenceFacts selects visible data and source-qualified production evidence.
func (s *Store) ReferenceFacts(
	id manifest.NamedResource,
	index *manifest.DiscoveryIndex,

	consumer manifest.NamedResource,
) ReferenceFacts {
	result := productionResult{facts: true}
	s.visitProductions(id, index, &result)
	var candidates []manifest.ReferenceData
	if len(result.out) != 0 {
		candidates = s.referenceData(id, index, result.out, nil)
	}
	facts := ReferenceFacts{
		Data:          result.data,
		Accepted:      result.accepted,
		Productions:   result.out,
		ReferenceData: candidates,
	}
	return facts.resolve(
		id,

		&referenceResolution{consumer: consumer, reference: result.reference},
	)
}

func (s *Store) referenceData(
	id manifest.NamedResource,
	index *manifest.DiscoveryIndex,
	productions []manifest.Production,
	reads *ReadSet,
) []manifest.ReferenceData {
	if index == nil {
		index = s.index.Load()
	}
	records := referenceRecords(index, id)
	var out []manifest.ReferenceData
	add := func(candidate manifest.ReferenceData) {
		candidate = s.declaredReferenceData(id, reads, new(candidate))
		if len(candidate.DeclaredKeys) != 0 || candidate.OfflineData != nil {
			if object, ok := candidate.OfflineData.(manifest.BaseManifest); ok &&
				object.Named().Namespace != id.Namespace {
				//nolint:errcheck // Namespace projection retains the source Data implementation and its assertion contract.
				candidate.OfflineData = object.WithNamespace(id.Namespace).(manifest.Data)
			}
			out = append(out, candidate)
		}
	}
	for _, candidates := range records {
		{
			candidateValues := candidates
			for candidateIndex := range candidateValues {
				candidate := candidateValues[candidateIndex]
				if !slices.Contains(productions, candidate.Production) {
					continue
				}

				candidate = s.sourceQualifiedReferenceData(reads, new(candidate))
				add(candidate)
			}
		}
	}
	{
		pValues2 := productions
		for pIndex2 := range pValues2 {
			p := pValues2[pIndex2]
			if p.Declaration == (manifest.NamedResource{}) ||
				slices.ContainsFunc(records[0], func(c manifest.ReferenceData) bool { return c.Production == p }) ||
				slices.ContainsFunc(records[1], func(c manifest.ReferenceData) bool { return c.Production == p }) {
				continue
			}
			add(manifest.ReferenceData{Production: p})
		}
	}
	return out
}

// RenderedParent returns the smallest eligible accepted or discovered rendered owner.
// A nil index uses the installed discovery index.
func (s *Store) RenderedParent(
	id manifest.NamedResource,
	index *manifest.DiscoveryIndex,
) (manifest.NamedResource, bool) {
	result := productionResult{id: id, parentOnly: true}
	s.visitProductions(id, index, &result)
	return result.parent, result.parent != (manifest.NamedResource{})
}

// SourceAdmission rejects initial source support after accepted ownership retires it.
func (s *Store) SourceAdmission(id, source manifest.NamedResource) bool {
	result := productionResult{source: source, open: true}
	s.visitProductions(id, nil, &result)
	return result.open
}

type productionResult struct {
	source        manifest.NamedResource
	open          bool
	qualified     bool
	reference     *Declaration
	data          manifest.Data
	out           []manifest.Production
	id            manifest.NamedResource
	parent        manifest.NamedResource
	matched       int
	facts         bool
	accepted      bool
	owned         bool
	parentOnly    bool
	reads         *ReadSet
	captureTarget bool
	incoming      *Child
	watch         bool
}

func (r *productionResult) reset(evidence []manifest.Production) {
	r.out, r.matched, r.owned, r.parent = evidence, 0, false, manifest.NamedResource{}
}

func (r *productionResult) add(p *manifest.Production) {
	if r.source != (manifest.NamedResource{}) {
		return
	}
	if r.parentOnly {
		if p.Owner != (manifest.NamedResource{}) && p.Owner != r.id && p.Authored == (manifest.NamedResource{}) &&
			p.Declaration == (manifest.NamedResource{}) &&
			(r.parent == (manifest.NamedResource{}) || p.Owner.Compare(r.parent) < 0) {
			r.parent = p.Owner
		}
		return
	}
	if !r.owned && r.matched < len(r.out) && (&r.out[r.matched] == p || r.out[r.matched] == *p) {
		r.matched++
		return
	}
	if !r.owned {
		if slices.Contains(r.out[:r.matched], *p) {
			return
		}
		r.out = slices.Clone(r.out[:r.matched])
		r.owned = true
	}
	r.out = append(r.out, *p)
}

// result MUST be fresh for each traversal.
func (s *Store) visitProductions(id manifest.NamedResource, index *manifest.DiscoveryIndex, result *productionResult) {
	reads := result.reads
	if reads != nil && result.captureTarget {
		reads.GetObject(id)
	}
	if index == nil {
		index = s.index.Load()
	}
	var evidence, bare []manifest.Production
	provisional := false
	if index != nil {
		evidence = index.Records[id].Productions
		provisional = bootstrapSeed(evidence)
		if id.Namespace != "" {
			key := id
			key.Namespace = ""
			bare = index.Records[key].Productions
			if reads != nil && result.captureTarget && len(bare) != 0 {
				reads.GetObject(key)
			}
		}
	}
	if result.incoming != nil {
		incoming := result.incomingProductions(id, reads, bare)
		evidence, bare = incoming.evidence, incoming.bare
	}
	result.out = evidence
	sh := s.shardFor(id)
	sh.mu.RLock()
	result.classifySource(new(sh.cells[id]), provisional)
	for i := range evidence {
		if result.incoming == nil {
			result.addVisibleProduction(s, id, &evidence[i], reads)
		} else {
			result.add(&evidence[i])
		}
	}
	s.addBareProductions(id, result, reads, [2][]manifest.Production{evidence, bare})
	if result.facts {
		result.captureReferenceLocked(sh, id)
	}
	if result.incoming == nil {
		sh.addProductionMembers(id, result, reads, provisional)
	}
	sh.mu.RUnlock()
	if reads != nil {
		reads.activateProducers()
	}
	result.finishProductions()
}

func (r *productionResult) classifySource(cell *objectCell, provisional bool) {
	r.qualified = cell.static != nil &&
		(cell.classified && cell.binding != nil && cell.binding.Ref.NamedResource == r.source ||
			provisional && r.source == manifest.BootstrapSourceID)
	if r.qualified && cell.retired {
		r.open = false
	}
}

func sameProductionContext(
	coverage map[manifest.NamedResource]*contributor,
	aValue *manifest.Production,
	bValue *manifest.Production,
) bool {
	a := *aValue
	b := *bValue
	if a.Source != b.Source {
		return false
	}
	ac, bc := coverage[a.Owner], coverage[b.Owner]
	return ac == nil || bc == nil || ac.binding.equal(&bc.binding)
}

//nolint:gocritic // The slices callback contract requires value parameters.
func compareProduction(a, b manifest.Production) int {
	return cmp.Or(a.Owner.Compare(b.Owner), a.Declaration.Compare(b.Declaration),
		a.Source.Compare(b.Source), a.Authored.Compare(b.Authored),
		cmp.Compare(a.Path, b.Path), cmp.Compare(a.File, b.File))
}

// SetDiscoveryIndex installs frozen source-qualified evidence before reconciliation.
// Discovery is never rebuilt on the render path.
func (s *Store) SetDiscoveryIndex(index *manifest.DiscoveryIndex) {
	if index == nil || s.index.Load() == index {
		return
	}
	coverage := s.discoveryCoverage(index)
	for _, contributor := range coverage {
		slices.SortFunc(contributor.records, func(a, b footprint) int { return a.id.Compare(b.id) })
	}
	s.lockAll()
	s.installDiscoveryCoverageLocked(index, coverage)
	s.unlockAll()
}

func (s *Store) contributionBinding(id manifest.NamedResource) sourceBinding {
	if c := s.coverage[id]; c != nil {
		return c.binding
	}
	return sourceBinding{}
}

// Bootstrap promotion alone supplies no independent authority against accepted output.
func bootstrapSeed(productions []manifest.Production) bool {
	if slices.ContainsFunc(productions, func(p manifest.Production) bool {
		return p.Owner == (manifest.NamedResource{}) && p.Declaration == (manifest.NamedResource{}) &&
			p.Source == manifest.BootstrapSourceID && p.Path == "" && p.File == ""
	}) {
		return !slices.ContainsFunc(productions, func(p manifest.Production) bool {
			return p.Owner == (manifest.NamedResource{}) &&
				(p.Source != manifest.BootstrapSourceID || p.Path != "" || p.File != "")
		})
	}
	return len(productions) > 0 && !slices.ContainsFunc(productions, func(p manifest.Production) bool {
		return p.Owner != (manifest.NamedResource{}) || p.Declaration != (manifest.NamedResource{}) ||
			p.Source != manifest.BootstrapSourceID
	})
}

func (e *Emission) bindSource(s *Store, obj manifest.BaseManifest, reads *ReadSet, id manifest.NamedResource) {
	getArtifact := s.GetArtifact
	if reads != nil {
		getArtifact = reads.GetArtifact
	}
	for ref := range manifest.References(obj, manifest.ReferenceRelations) {
		if ref.Use == sourceReferenceUse {
			e.binding.source = ref.NamedResource
			if a, ok := getArtifact(ref.NamedResource, ArtifactContent).(*SourceArtifact); ok {
				e.binding.artifact = a
				if a.Content == "" {
					e.binding.mutable = a.mutableMaterial()
				}
			}
			break
		}
	}
	if e.binding.source.Name == "" && s.contributionBinding(id).source == manifest.BootstrapSourceID {
		e.binding.source = manifest.BootstrapSourceID
		if a, ok := getArtifact(manifest.BootstrapSourceID, ArtifactContent).(*SourceArtifact); ok {
			e.binding.artifact = a
			if a.Content == "" {
				e.binding.mutable = a.mutableMaterial()
			}
		}
	}
}

func (s *Store) discoveryCoverage(index *manifest.DiscoveryIndex) map[manifest.NamedResource]*contributor {
	coverage := map[manifest.NamedResource]*contributor{}
	//nolint:gocritic // Map iteration supplies value snapshots without an additional lookup.
	for id, r := range index.Records {
		if len(r.Productions) == 0 {
			continue
		}
		record := footprint{
			id: id, record: r,
			independent: rawDeclarationAuthority(index, id, false),
			classified:  rawDeclarationAuthority(index, id, true),
		}
		s.addDiscoveryCoverage(coverage, &record)
	}
	return coverage
}

func (s *Store) installDiscoveryCoverageLocked(
	index *manifest.DiscoveryIndex,
	coverage map[manifest.NamedResource]*contributor,
) {
	if s.index.Load() == nil {
		// Every non-independent declaration MUST occur in a contributor footprint.
		for owner, c := range coverage {
			{
				recordValues := c.records
				for recordIndex := range recordValues {
					record := recordValues[recordIndex]
					sh := s.shardFor(record.id)
					if cell, exists := sh.cells[record.id]; exists {
						cell.independent = record.independent
						cell.classified = cell.classified && record.classified
						sh.cells[record.id] = cell
					}
				}
			}
			sh := s.shardFor(owner)
			slot := sh.slots[owner]
			if slot == nil {
				slot = &producerSlot{id: owner, links: sh.newHeadLocked(0)}
				slot.visible.Store(sh.objectLocked(owner) != nil)
				slot.hidden.Store(sh.cells[owner].hidden)
				sh.slots[owner] = slot
			}
			c.slot = slot
		}
		s.coverage = coverage
		s.index.Store(index)
	}
}

func (s *Store) addDiscoveryCoverage(
	coverage map[manifest.NamedResource]*contributor,
	record *footprint,
) {
	id, r := record.id, &record.record
	{
		pValues5 := r.Productions
		for pIndex4 := range pValues5 {
			p := pValues5[pIndex4]
			if p.Owner == (manifest.NamedResource{}) || p.Owner == id {
				continue
			}
			c := coverage[p.Owner]
			if c == nil {
				c = &contributor{binding: sourceBinding{source: p.Source}}
				coverage[p.Owner] = c
			}
			// Each identity is visited once, so repeated owner evidence can only match the tail.
			if len(c.records) == 0 || c.records[len(c.records)-1].id != id {
				c.records = append(c.records, *record)
			}
			if a, ok := s.GetArtifact(p.Source, ArtifactContent).(*SourceArtifact); ok {
				c.binding.artifact = a
				if a.Content == "" {
					c.binding.mutable = a.mutableMaterial()
				}
			}
		}
	}
}

func (r *productionResult) finishProductions() {
	if !r.owned {
		r.out = r.out[:r.matched]
		for i := 1; i < len(r.out); i++ {
			if compareProduction(r.out[i-1], r.out[i]) >= 0 {
				r.out = slices.Clone(r.out)
				r.owned = true
				break
			}
		}
	}
	if r.owned {
		slices.SortFunc(r.out, compareProduction)
		r.out = slices.Compact(r.out)
	}
}

func (sh *shard) addProductionMembers(
	id manifest.NamedResource,
	result *productionResult,
	reads *ReadSet,
	provisional bool,
) {
	cell := sh.cells[id]
	if cell.hidden && cell.reference == nil {
		result.reset(nil)
		return
	}
	// A directly bound declaration can precede its discovery record.
	if cell.static != nil && cell.binding != nil && cell.support == nil && len(result.out) == 0 && cell.bound() {
		result.add(&manifest.Production{Source: cell.binding.Ref.NamedResource})
	}
	if cell.static != nil && cell.retired && (provisional || cell.classified) {
		result.reset(nil)
	}
	for _, m := range sh.members[id] {
		e := m.slot.current.Load()
		if reads != nil {
			reads.sample(id, m.slot, e, result.watch)
		}
		if !m.slot.visible.Load() {
			continue
		}
		entry := &e.entries[m.position]
		if m.declaration {
			p := manifest.Production{
				Owner:       m.slot.id,
				Declaration: entry.child.Obj.Named(),
				Source:      entry.binding.source,
			}
			result.add(&p)
			continue
		}
		if !slices.ContainsFunc(
			entry.child.Productions,
			func(p manifest.Production) bool { return p.Owner == m.slot.id },
		) {
			p := manifest.Production{Owner: m.slot.id, Source: entry.binding.source}
			result.add(&p)
		}
		for i := range entry.child.Productions {
			result.add(&entry.child.Productions[i])
		}
	}
}

func (s *Store) addBareProductions(
	id manifest.NamedResource,
	result *productionResult,
	reads *ReadSet,
	productions [2][]manifest.Production,
) {
	evidence, bare := productions[0], productions[1]
	sameFamily := func(a, b manifest.Production) bool {
		return a.Source == b.Source &&
			(result.incoming != nil || a.Path == "" || a.File == b.File && a.Authored == b.Authored)
	}
	var first *manifest.Production
	{
		pValues4 := bare
		for i := range pValues4 {
			p := pValues4[i]
			if bareProductionExcluded(new(p), id) {
				continue
			}
			matches := slices.ContainsFunc(evidence, func(e manifest.Production) bool {
				return sameFamily(e, p) && sameProductionContext(s.coverage, new(e), new(p))
			})
			if len(evidence) == 0 {
				matches = first == nil ||
					sameFamily(*first, p) && sameProductionContext(s.coverage, new(*first), new(p))
			}
			if !matches {
				matches = slices.ContainsFunc(bare[:i], func(e manifest.Production) bool {
					return e.Path != "" && e.Authored.Namespace == "" && sameFamily(e, p) &&
						s.coverage[e.Owner] == nil &&
						(len(evidence) == 0 && sameFamily(*first, e) || slices.ContainsFunc(evidence, func(d manifest.Production) bool {
							return sameFamily(d, e)
						}))
				})
			}
			if matches {
				if first == nil {
					first = &bare[i]
				}
				result.addVisibleProduction(s, id, &bare[i], reads)
			}
		}
	}
}

func (r *productionResult) addVisibleProduction(
	s *Store,
	id manifest.NamedResource,
	p *manifest.Production,
	reads *ReadSet,
) {
	if c := s.coverage[p.Owner]; c != nil {
		accepted := c.slot.current.Load()
		if reads != nil {
			reads.sample(id, c.slot, accepted, r.watch)
		}
		if r.source != (manifest.NamedResource{}) && !r.qualified && p.Source == r.source &&
			accepted.shadows(new(c.binding)) {
			r.open = false
		}
		if c.slot.hidden.Load() || accepted.shadows(new(c.binding)) {
			return
		}
	}
	r.add(p)
}

type incomingProductionEvidence struct {
	evidence []manifest.Production
	bare     []manifest.Production
}

func (r *productionResult) incomingProductions(
	id manifest.NamedResource,
	reads *ReadSet,
	bare []manifest.Production,
) incomingProductionEvidence {
	evidence := r.incoming.Productions
	if len(evidence) == 0 || slices.ContainsFunc(evidence, func(p manifest.Production) bool {
		return p.Path != "" && manifest.Resolve(p.Authored, id.Namespace, "") == id
	}) {
		bare = nil
	}
	if reads != nil {
		for _, productions := range [2][]manifest.Production{evidence, bare} {
			{
				pValues3 := productions
				for pIndex3 := range pValues3 {
					p := pValues3[pIndex3]
					reads.GetArtifact(p.Source, ArtifactContent)
				}
			}
		}
	}
	return incomingProductionEvidence{evidence: evidence, bare: bare}
}

func bareProductionExcluded(pValue *manifest.Production, id manifest.NamedResource) bool {
	p := *pValue
	return p.Path == "" || p.Authored.Namespace != "" ||
		p.Authored != (manifest.NamedResource{}) && manifest.Resolve(p.Authored, id.Namespace, "") != id
}

func (s *Store) declaredReferenceData(
	id manifest.NamedResource,
	reads *ReadSet,
	candidateValue *manifest.ReferenceData,
) manifest.ReferenceData {
	candidate := *candidateValue
	p := candidate.Production
	if p.Declaration == (manifest.NamedResource{}) {
		return candidate
	}
	view := s.captureReferenceDeclaration(reads, new(p))
	object := view.child.Obj
	if object == nil {
		return candidate
	}
	candidate.DeclaredKeys = nil
	binding := view.source
	if (view.owner == nil || view.owner.id == p.Owner) &&
		(p.Source == (manifest.NamedResource{}) || binding.source == (manifest.NamedResource{}) || p.Source == binding.source) {
		for _, target := range manifest.ProducerTargets(object, id) {
			if target.NamedResource == id {
				candidate.DeclaredKeys = target.DeclaredKeys
				break
			}
		}
	}
	return candidate
}

func (s *Store) captureReferenceDeclaration(reads *ReadSet, pValue *manifest.Production) capture {
	p := *pValue
	var view capture
	if reads != nil {
		reads.GetObject(p.Declaration)
		view = reads.input(p.Declaration).view
	} else {
		sh := s.shardFor(p.Declaration)
		sh.mu.RLock()
		sh.captureLocked(p.Declaration, new(sh.cells[p.Declaration]), &view)
		sh.mu.RUnlock()
	}
	return view
}

func (s *Store) sourceQualifiedReferenceData(
	reads *ReadSet,
	candidateValue *manifest.ReferenceData,
) manifest.ReferenceData {
	candidate := *candidateValue
	getArtifact := s.GetArtifact
	if reads != nil {
		getArtifact = reads.GetArtifact
	}
	if c := s.coverage[candidate.Production.Owner]; c != nil &&
		c.binding.source == candidate.Production.Source &&
		c.binding.artifact != nil {
		artifact, _ := getArtifact(candidate.Production.Source, ArtifactContent).(*SourceArtifact)
		binding := sourceBinding{source: candidate.Production.Source, artifact: artifact}
		if artifact != nil && artifact.Content == "" {
			binding.mutable = artifact.mutableMaterial()
		}
		if !c.binding.equal(&binding) {
			candidate.DeclaredKeys, candidate.OfflineData = nil, nil
		}
	}
	return candidate
}

func referenceRecords(index *manifest.DiscoveryIndex, id manifest.NamedResource) [2][]manifest.ReferenceData {
	var records [2][]manifest.ReferenceData
	if index != nil {
		records[0] = index.Records[id].ReferenceData
		if id.Namespace != "" {
			bare := id
			bare.Namespace = ""
			records[1] = index.Records[bare].ReferenceData
		}
	}
	return records
}

func (r *productionResult) captureReferenceLocked(sh *shard, id manifest.NamedResource) {
	var view capture
	sh.captureLocked(id, new(sh.cells[id]), &view)
	if view.child.Origin.Policy(id, nil).Root {
		r.data, _ = view.child.Obj.(manifest.Data)
	}
	r.reference = view.reference
	r.accepted = view.accepted || view.owner != nil
}
