package store

import (
	"cmp"
	"slices"

	"github.com/home-operations/flate/pkg/manifest"
)

// ReportHidden classifies the coherent representative's auxiliary provenance.
func (s *Store) ReportHidden(id manifest.NamedResource) bool {
	sh := s.shardFor(id)
	sh.mu.RLock()
	hidden := sh.reportHiddenLocked(id)
	sh.mu.RUnlock()
	return hidden
}

func (sh *shard) reportHiddenLocked(id manifest.NamedResource) bool {
	if id.ReportHidden() {
		return true
	}
	if !sh.cells[id].auxiliary {
		return false
	}
	cell := sh.cells[id]
	return !cell.hidden && (!cell.rooted || !cell.bound())
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

func declarationIndependent(index *manifest.DiscoveryIndex, id manifest.NamedResource) bool {
	record, _ := index.Lookup(id)
	return len(record.Productions) == 0 || slices.ContainsFunc(record.Productions, func(p manifest.Production) bool {
		return p.Owner == (manifest.NamedResource{}) || p.Owner == id
	})
}

//nolint:ireturn // The manifest contract requires polymorphic results.
func (cv *capture) declaration() manifest.BaseManifest {
	c := *cv
	if c.independent || c.accepted || c.owner != nil {
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
			binding:    view.sourceBinding(),
		},
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
	Data          manifest.Data
	Supplied      manifest.Data
	SupplyPath    string
	Accepted      bool
	Productions   []manifest.Production
	ReferenceData []manifest.ReferenceData
}

// Productions merges discovery evidence with accepted reverse memberships into a read-only snapshot.
func (s *Store) Productions(id manifest.NamedResource, index *manifest.DiscoveryIndex) []manifest.Production {
	var result productionResult
	s.visitProductions(id, index, &result)
	return result.out
}

// IncomingProductions projects prospective evidence without the stored target's state.
func (s *Store) IncomingProductions(child Child, index *manifest.DiscoveryIndex) []manifest.Production {
	result := productionResult{incoming: &child}
	s.visitProductions(child.Obj.Named(), index, &result)
	return result.out
}

// ReferenceFacts selects visible data and source-qualified production evidence.
func (s *Store) ReferenceFacts(id manifest.NamedResource, index *manifest.DiscoveryIndex) ReferenceFacts {
	result := productionResult{facts: true}
	s.visitProductions(id, index, &result)
	data, path := s.suppliedObject(id)
	var candidates []manifest.ReferenceData
	if len(result.out) != 0 {
		candidates = s.referenceData(id, index, result.out, nil)
	}
	return ReferenceFacts{
		Data:          result.data,
		Accepted:      result.accepted,
		Productions:   result.out,
		ReferenceData: candidates,
		Supplied:      data,
		SupplyPath:    path,
	}
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
	var records [2][]manifest.ReferenceData
	if index != nil {
		records[0] = index.Records[id].ReferenceData
		if id.Namespace != "" {
			bare := id
			bare.Namespace = ""
			records[1] = index.Records[bare].ReferenceData
		}
	}
	var out []manifest.ReferenceData
	add := func(candidate manifest.ReferenceData) {
		p := candidate.Production
		if p.Declaration != (manifest.NamedResource{}) {
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
			if object := view.child.Obj; object != nil {
				candidate.DeclaredKeys = nil
				binding := view.sourceBinding()
				if (view.owner == nil || view.owner.id == p.Owner) &&
					(p.Source == (manifest.NamedResource{}) || binding.source == (manifest.NamedResource{}) || p.Source == binding.source) {
					for _, target := range manifest.ProducerTargets(object, id) {
						if target.NamedResource == id {
							candidate.DeclaredKeys = target.DeclaredKeys
							break
						}
					}
				}
			}
		}
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

type productionResult struct {
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
		evidence = result.incoming.Productions
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
	}
	result.out = evidence
	sh := s.shardFor(id)
	sh.mu.RLock()
	visible := func(p *manifest.Production) {
		if c := s.coverage[p.Owner]; c != nil {
			accepted := c.slot.current.Load()
			if reads != nil {
				reads.sample(id, c.slot, accepted, result.watch)
			}
			if c.slot.hidden.Load() || accepted.shadows(new(c.binding)) {
				return
			}
		}
		result.add(p)
	}
	for i := range evidence {
		if result.incoming == nil {
			visible(&evidence[i])
		} else {
			result.add(&evidence[i])
		}
	}
	sameFamily := func(a, b manifest.Production) bool {
		return a.Source == b.Source &&
			(result.incoming != nil || a.Path == "" || a.File == b.File && a.Authored == b.Authored)
	}
	var first *manifest.Production
	{
		pValues4 := bare
		for i := range pValues4 {
			p := pValues4[i]
			if p.Path == "" || p.Authored.Namespace != "" ||
				p.Authored != (manifest.NamedResource{}) && manifest.Resolve(p.Authored, id.Namespace, "") != id {
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
				visible(&bare[i])
			}
		}
	}
	if result.facts {
		var view capture
		sh.captureLocked(id, new(sh.cells[id]), &view)
		result.data, _ = view.child.Obj.(manifest.Data)
		result.accepted = view.accepted || view.owner != nil
	}
	if result.incoming == nil {
		cell := sh.cells[id]
		if cell.hidden {
			result.reset(nil)
		} else {
			if cell.static != nil && cell.retired && provisional {
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
	}
	sh.mu.RUnlock()
	if reads != nil {
		reads.activateProducers()
	}
	if !result.owned {
		result.out = result.out[:result.matched]
		for i := 1; i < len(result.out); i++ {
			if compareProduction(result.out[i-1], result.out[i]) >= 0 {
				result.out = slices.Clone(result.out)
				result.owned = true
				break
			}
		}
	}
	if result.owned {
		slices.SortFunc(result.out, compareProduction)
		result.out = slices.Compact(result.out)
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
		if ref.Use == "source" {
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
	{
		//nolint:gocritic // Map iteration supplies value snapshots without an additional lookup.
		for id, r := range index.Records {
			if len(r.Productions) == 0 {
				continue
			}
			independent := declarationIndependent(index, id)
			s.addDiscoveryCoverage(coverage, id, &r, independent)
		}
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
	id manifest.NamedResource,
	r *manifest.DiscoveryRecord,
	independent bool,
) {
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
				c.records = append(c.records, footprint{id: id, record: *r, independent: independent})
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
