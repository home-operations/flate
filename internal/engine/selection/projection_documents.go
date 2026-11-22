package selection

import (
	"cmp"
	"errors"
	"fmt"
	"iter"
	"slices"

	"github.com/home-operations/flate/internal/engine/store"
	"github.com/home-operations/flate/pkg/manifest"
)

func validateProjectionBatches(sides []selectionSide, complete bool) error {
	var inputErr error
	for i := range sides {
		if !complete || sides[i].run.Absent {
			continue
		}
		for j, batch := range sides[i].run.Manifests {
			if _, err := batch.IdentityAccessor(); err != nil {
				inputErr = errors.Join(inputErr, fmt.Errorf("run %d batch %d: %w", i, j, err))
			}
		}
	}
	return inputErr
}

func prepareCollections(
	sides []selectionSide,
	request Request,
	complete bool,
	collections *[comparisonSides]selectionCollection,
) {
	roots, labels := request.Roots, request.Labels
	paired := len(sides) == comparisonSides && sides[0].run.pairsOutput(&sides[1].run)
	var canPairDocuments [comparisonSides]bool
	for i := range sides {
		run := &sides[i].run
		var other *selectionSide
		if len(sides) == comparisonSides {
			other = &sides[1-i]
		}
		if !complete || run.Absent {
			sides[i].result = &Result{LookupFailed: len(run.LookupFailures) != 0}
		} else {
			collections[i] = sides[i].collect(roots, labels, other, paired)
			canPairDocuments[i] = len(run.Manifests) != 0 && len(sides[i].lookupFailures) == 0
		}
	}
	if paired && canPairDocuments[0] && canPairDocuments[1] {
		collections[0].peer = &collections[1]
		collections[1].peer = &collections[0]
	}
}

func (s *selectionSide) collect(
	roots []manifest.NamedResource,
	labels map[string]string,
	other *selectionSide,
	paired bool,
) selectionCollection {
	s.result = &Result{Counts: make(map[string]int, len(roots)), Bindings: store.Bindings(s.run.Index)}
	c := selectionCollection{
		side:   s,
		intent: Request{Roots: roots},
		labels: labels,
		other:  other,
		scope:  s.run.entryScope(),
		paired: paired,
	}
	if batches := s.run.Manifests; len(batches) != 0 {
		c.positions = make(map[manifest.NamedResource]int, len(batches))
		for i, batch := range batches {
			if batch.Parent != nil {
				c.positions[batch.Parent.Named()] = i
			}
		}
	}
	c.collectObjects()
	if roots[0].Name != "" && !s.result.LookupMatched {
		s.lookupFailures = s.run.LookupEvidence
	}
	s.result.LookupFailed = len(s.run.LookupFailures) != 0 || len(s.lookupFailures) != 0
	return c
}

type selectionCollection struct {
	side      *selectionSide
	intent    Request
	labels    map[string]string
	other     *selectionSide
	emitters  []manifest.BaseManifest
	scope     map[manifest.NamedResource]bool
	positions map[manifest.NamedResource]int
	peer      *selectionCollection
	local     map[manifest.NamedResource]struct{}
	indexed   bool
	paired    bool
}

func (c *selectionCollection) collectObjects() {
	for _, root := range c.intent.Roots {
		for _, obj := range c.rootObjects(root) {
			c.collectObject(root, obj)
		}
	}
}

func (c *selectionCollection) rootObjects(root manifest.NamedResource) []manifest.BaseManifest {
	s := c.side
	objects := s.run.Store.ListObjects(root.Group, root.Kind)
	stored := len(objects)
	for id, record := range s.run.Index.Files() { //nolint:gocritic // The iterator supplies independent value snapshots.
		if id.Group == root.Group && id.Kind == root.Kind {
			if obj := s.unchangedDeclaration(id, &record); obj != nil {
				objects = append(objects, obj)
			}
		}
	}
	if len(objects) != stored {
		slices.SortFunc(objects, func(a, b manifest.BaseManifest) int { return a.Named().Compare(b.Named()) })
	}
	return objects
}

func (c *selectionCollection) collectObject(root manifest.NamedResource, obj manifest.BaseManifest) {
	s := c.side
	id := obj.Named()
	if !s.lookupAllowsObject(id) {
		return
	}
	if !s.run.Store.Policy(id).Report && !id.ReportHidden() {
		return
	}
	if c.scope != nil && !c.scope[id] {
		if s.run.Emissions != nil && (root.Name == "" || c.intent.Matches(id)) && c.canPair(obj) {
			c.emitters = append(c.emitters, obj)
		}
		return
	}
	if c.other != nil {
		s.prepareExternal(root, obj)
	}
	if !c.selectRoot(root, obj) {
		return
	}
	s.result.Counts[root.Kind]++
	s.roots = append(s.roots, id)
	if !matchesLabels(obj, c.labels) {
		return
	}
	s.result.Objects = append(s.result.Objects, obj)
	if version := s.chartVersion(obj); version != "" {
		if s.result.ChartVersions == nil {
			s.result.ChartVersions = make(map[manifest.NamedResource]string)
		}
		s.result.ChartVersions[id] = version
	}
}

func (c *selectionCollection) selectRoot(root manifest.NamedResource, obj manifest.BaseManifest) bool {
	s := c.side
	id := obj.Named()
	if id.ReportHidden() && manifest.KindOf(obj).Render {
		if root.Name == "" {
			s.roots = append(s.roots, id)
			c.emitters = append(c.emitters, obj)
		}
		return false
	}
	if !c.intent.Matches(id) || !s.includes(id.Namespace) {
		if root.Name == "" && s.run.Decision.restricted() && (s.run.Decision.Reconcile(id) || c.canPair(obj)) {
			c.emitters = append(c.emitters, obj)
		}
		return false
	}
	if root.Name != "" {
		s.result.LookupMatched = true
	}
	if !s.selectedObject(obj, c.counterpart()) {
		if c.canPair(obj) {
			c.emitters = append(c.emitters, obj)
		}
		return false
	}
	return true
}

func (c *selectionCollection) canPair(obj manifest.BaseManifest) bool {
	if !c.paired {
		return false
	}
	_, completed := c.positions[obj.Named()]
	return completed && matchesLabels(obj, c.labels)
}

func (c *selectionCollection) counterpart() *Snapshot {
	if c.other == nil {
		return nil
	}
	return &c.other.run
}

func (s *selectionSide) selectedObject(obj manifest.BaseManifest, other *Snapshot) bool {
	id := obj.Named()
	if !s.run.Decision.restricted() || manifest.KindOf(obj).Discovery {
		return true
	}
	if s.run.outputsProducer(id, other) || s.run.Store.GetObject(id) == nil {
		return true
	}
	parent, owned := s.run.Store.Parent(id, s.run.Index)
	parentSelected := s.run.Decision.Output(parent)
	return owned && parentSelected
}

func (c *selectionCollection) batches() iter.Seq[batchSelection] {
	return func(yield func(batchSelection) bool) {
		s := c.side
		for _, objects := range [2][]manifest.BaseManifest{s.result.Objects, c.emitters} {
			for _, obj := range objects {
				i, ok := c.positions[obj.Named()]
				if ok && !yield(batchSelection{
					collection: c, object: obj, batch: s.run.Manifests[i], owners: s.run.owners[obj.Named()],
				}) {
					return
				}
			}
		}
	}
}

func (c *selectionCollection) localDocuments() map[manifest.NamedResource]struct{} {
	var selected map[manifest.NamedResource]struct{}
	for batch := range c.batches() {
		batch.prepare(true)
		if !batch.documentScope && !matchesLabels(batch.object, c.labels) {
			continue
		}
		identity, _ := batch.batch.IdentityAccessor()
		for i, doc := range batch.batch.Documents {
			id := identity(i)
			owner := batch.owners[i]
			if id.Name == "" || id.Kind == "" || !c.side.includes(id.Namespace) ||
				!batch.selects(doc, id, &owner) {
				continue
			}
			if selected == nil {
				selected = make(map[manifest.NamedResource]struct{})
			}
			selected[id] = struct{}{}
		}
	}
	return selected
}

func (c *selectionCollection) hasPeerDocument(id manifest.NamedResource) bool {
	if c.peer == nil {
		return false
	}
	if !c.indexed {
		c.local, c.peer.local = c.localDocuments(), c.peer.localDocuments()
		c.indexed, c.peer.indexed = true, true
	}
	_, found := c.peer.local[id]
	return found
}

func (c *selectionCollection) collectBatches() {
	s := c.side
	if len(s.run.Manifests) == 0 {
		return
	}
	s.result.Manifests = make([]manifest.DocumentBatch, 0, len(s.result.Objects)+1)
	var selectedOwners batchOwners
	for batch := range c.batches() {
		batch.prepare(false)
		batch.selectDocuments()
		if len(batch.batch.Documents) != 0 {
			s.result.Manifests = append(s.result.Manifests, batch.batch)
			selectedOwners.set(batch.object.Named(), batch.owners)
		}
	}
	s.result.Manifests = collapseOverlaps(s.result.Manifests, selectedOwners)
	s.result.Producers = selectedOwners.producers(s.result.Manifests)
}

type batchSelection struct {
	collection    *selectionCollection
	object        manifest.BaseManifest
	batch         manifest.DocumentBatch
	owners        documentOwners
	documentScope bool
	allDocuments  bool
	namespaced    bool
	compared      bool
	local         bool
}

func (b *batchSelection) prepare(local bool) {
	b.local = local
	s, other := b.collection.side, b.collection.other
	id := b.object.Named()
	b.documentScope = manifest.KindOf(b.object).Parent && b.object.EmissionPolicy().DocumentScope
	if !b.documentScope {
		b.allDocuments = s.run.Decision.Output(id)
		if !local {
			b.allDocuments = s.run.outputsProducer(id, b.collection.counterpart())
		}
		return
	}
	b.namespaced = s.namespaces != nil && (other == nil || other.run.Absent || other.namespaces != nil)
	b.compared = other == nil || other.run.Absent
	if !b.compared {
		key, _ := store.Bindings(s.run.Index).Slot(id)
		producer, _ := store.Bindings(other.run.Index).Producer(key)
		b.compared = other.run.Decision.Reconcile(producer)
	}
}

func (b *batchSelection) selects(
	doc map[string]any,
	id manifest.NamedResource,
	owner *documentOwner,
) bool {
	c := b.collection
	s := c.side
	producer, child := b.object.Named(), id
	if owner.producer != nil {
		producer = owner.producer.ID
	}
	if owner.declaration != nil {
		child = cmp.Or(*owner.declaration, id)
	}
	if !s.run.EntryScopes.Scope(producer).Allows(child) {
		return false
	}
	member := s.run.Decision.Output(id)
	if b.includesDocument(doc, id, owner, member) {
		return true
	}
	if b.local || member || c.peer == nil {
		return false
	}
	if c.intent.Roots[0].Namespace != "" && !s.includes(id.Namespace) {
		return false
	}
	return b.includesDocument(doc, id, owner, true) && c.hasPeerDocument(id)
}

func (b *batchSelection) includesDocument(
	doc map[string]any,
	id manifest.NamedResource,
	owner *documentOwner,
	member bool,
) bool {
	c := b.collection
	s, other := c.side, c.other
	if owner.peerOnly && !member {
		return false
	}
	if !b.documentScope {
		if c.paired && owner.producer != nil {
			emitted := s.run.Decision.Output(owner.producer.ID) && s.includes(id.Namespace)
			return !b.local && b.allDocuments || emitted || member
		}
		return b.allDocuments || member
	}
	if !b.scopeIncludes(id, member) {
		return false
	}
	if s.run.Decision.restricted() && !b.namespaced && !member && !b.comparesDeclaration(id) {
		return false
	}
	if selectOutput(doc, c.intent.Roots, s.namespaces, c.labels) {
		return true
	}
	return !b.local && other != nil && !other.run.Absent &&
		selectOutput(doc, c.intent.Roots, other.namespaces, c.labels)
}

func (b *batchSelection) selectDocuments() {
	if b.allDocuments && b.collection.side.run.EntryScopes == nil && (!b.collection.paired || len(b.owners) == 0) {
		return
	}
	identity, _ := b.batch.IdentityAccessor()
	documents, owners := b.batch.Documents, b.owners
	b.batch.Documents, b.batch.Identities, b.owners = nil, nil, nil
	for i, doc := range documents {
		id := identity(i)
		owner := owners[i]
		if b.selects(doc, id, &owner) {
			b.owners.record(len(b.batch.Documents), &owner)
			b.batch.Documents = append(b.batch.Documents, doc)
			b.batch.Identities = append(b.batch.Identities, id)
		}
	}
}

func (b *batchSelection) comparesDeclaration(id manifest.NamedResource) bool {
	return b.compared && slices.ContainsFunc(b.collection.intent.Roots, func(root manifest.NamedResource) bool {
		return root.Group == id.Group && root.Kind == id.Kind
	})
}

func (s *selectionSide) lookupAllowsObject(id manifest.NamedResource) bool {
	scopes := s.run.EntryScopes
	if scopes == nil {
		return true
	}
	if owner := s.run.Store.EmissionOwner(id); owner != (manifest.NamedResource{}) && owner != id {
		return scopes.Scope(owner).Allows(id)
	}
	record, _ := s.run.Index.Lookup(id)
	owner := record.BuilderOwner
	if owner == (manifest.NamedResource{}) {
		owner, _ = s.run.Store.Parent(id, s.run.Index)
	}
	return scopes.lookupAllowsCandidate(id, owner, func() bool {
		return owner == (manifest.NamedResource{}) ||
			s.run.Facts != nil && Root(s.run.Facts.RootEvidence(id, nil)).InitialIndependent
	}, func() []manifest.Production {
		return s.run.Store.Productions(id, s.run.Index)
	})
}
