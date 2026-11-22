package store

import "github.com/home-operations/flate/pkg/manifest"

type declarationRole uint8

const (
	repositoryRole declarationRole = iota
	suppliedRole
	constructionRole
)

// DeclarationPolicy includes the authority of provisional inputs.
type DeclarationPolicy struct {
	role                    declarationRole
	Root, Reconcile, Report bool
}

// Supplied reports whether the invocation supplied the declaration.
func (p DeclarationPolicy) Supplied() bool {
	return p.role == suppliedRole
}

// RootCandidate includes supplied references that can support root selection.
func (p DeclarationPolicy) RootCandidate() bool {
	return p.Root || p.Supplied()
}

// Child retains declaration authority when a captured object enters a generation.
func (p DeclarationPolicy) Child(obj manifest.BaseManifest) Child {
	child := Child{Obj: obj, policy: p}
	if p.Supplied() {
		child.Origin = manifest.SuppliedDeclaration
	}
	return child
}

func (p DeclarationPolicy) reference() bool {
	return p.role == constructionRole || p.Supplied()
}

// DeclarationContent MUST come from loader comparison of an immutable document.
type DeclarationContent struct {
	Exact            string
	WithoutNamespace string
	Namespace        string
}

// Declaration associates an immutable input object with its origin and provenance.
type Declaration struct {
	Obj       manifest.BaseManifest
	Doc       map[string]any
	Origin    manifest.DeclarationOrigin
	policy    DeclarationPolicy
	InputPath string
	Ordinal   int
	Warnings  []manifest.Warning `json:"-"`
}

func (d *Declaration) child() Child {
	child := d.policy.Child(d.Obj)
	child.Doc, child.Warnings = d.Doc, d.Warnings
	return child
}

// AddDeclaration admits an immutable declaration under its origin's authority.
//
//nolint:gocritic // Immutable value inputs preserve snapshot semantics.
func (s *Store) AddDeclaration(child Declaration) {
	for warningIndex := range child.Warnings {
		warning := child.Warnings[warningIndex]
		s.AddWarning(warning)
	}
	if child.Origin == manifest.RepositoryDeclaration {
		s.addObject(&objectUpdate{obj: child.Obj, document: child.Doc, explicit: true, warnings: child.Warnings})
		return
	}
	child.policy = (Child{Obj: child.Obj, Origin: child.Origin}).Policy(child.Obj.Named())
	s.addReference(&child)
}

// AddConstructionDeclaration supplies an include before discovery grants repository authority.
func (s *Store) AddConstructionDeclaration(obj manifest.BaseManifest, doc map[string]any) {
	info := manifest.KindOf(obj)
	policy := DeclarationPolicy{
		role:      constructionRole,
		Reconcile: info.IsSource() && info.Runnable,
	}
	s.addReference(&Declaration{Obj: obj, Doc: doc, policy: policy})
}

func (s *Store) addReference(child *Declaration) {
	id := child.Obj.Named()
	sh := s.shardFor(id)
	sh.mu.Lock()
	cell := sh.cells[id]
	before := cell.reference
	if before != nil && before.Obj == child.Obj {
		sameLocation := before.InputPath == child.InputPath && before.Ordinal == child.Ordinal
		sameInput := sameLocation && before.policy.role == child.policy.role
		if sameInput && warningsEqual(before.Warnings, child.Warnings) &&
			manifest.ValueEqual(before.Doc, child.Doc) {
			sh.mu.Unlock()
			return
		}
	}
	bucket := sh.byName[kindID{id.Group, id.Kind}]
	sh.requireCapacityLocked(cell.authority, bucket.authority)
	const declarationFacets = 2
	if !sh.canAdmit(declarationFacets) {
		sh.mu.Unlock()
		panic("store: authority generation exhausted")
	}
	if cell.generation == 0 {
		sh.admitAbsenceLocked(id, objectFacet, nil)
		cell.links = sh.newHeadLocked(0)
	}
	if bucket.links == nil {
		bucket.links = sh.newHeadLocked(bucket.generation)
	}
	if before != nil && cell.policy.reference() && before.Obj != child.Obj {
		sh.clearFacetsLocked(id)
	}
	cell.reference = child
	cell.publication++
	cell.authority = sh.advanceLocked(cell.current())
	obj, policy := sh.captureLocked(id, new(cell), nil)
	cell.policy = policy
	sh.cells[id] = cell
	if bucket.update(nameID{id.Namespace, id.Name}, true, true, bucket.generation) {
		sh.admitAbsenceLocked(manifest.NamedResource{Group: id.Group, Kind: id.Kind}, bucketFacet, nil)
		bucket.authority = sh.enqueueLocked(bucket.authority)
		sh.byName[kindID{id.Group, id.Kind}] = bucket
	}
	event := fireUnderLock(s, EventObjectAdded, id)
	event.payload = obj
	sh.mu.Unlock()
	event.run()
}

// Lookup returns the coherent representative and its declaration roles.
func (s *Store) Lookup(id manifest.NamedResource) (manifest.BaseManifest, DeclarationPolicy) {
	sh := s.shardFor(id)
	sh.mu.RLock()
	obj, policy := sh.captureLocked(id, new(sh.cells[id]), nil)
	sh.mu.RUnlock()
	if obj == nil {
		policy = (Child{}).Policy(id)
	}
	return obj, policy
}

// Policy projects the coherent representative's declaration roles.
func (s *Store) Policy(id manifest.NamedResource) DeclarationPolicy {
	sh := s.shardFor(id)
	sh.mu.RLock()
	policy := sh.policyLocked(id)
	sh.mu.RUnlock()
	return policy
}

func (sh *shard) policyLocked(id manifest.NamedResource) DeclarationPolicy {
	cell := sh.cells[id]
	if (cell.policy.Root || cell.policy.reference()) && cell.bound() {
		return cell.policy
	}
	obj, policy := sh.captureLocked(id, new(cell), nil)
	if obj == nil {
		return (Child{}).Policy(id)
	}
	return policy
}

// Policy derives execution roles from an incoming or captured declaration envelope.
//
//nolint:gocritic // The value receiver supports literal declarations before store admission.
func (c Child) Policy(id manifest.NamedResource) DeclarationPolicy {
	if c.policy.role == constructionRole {
		return c.policy
	}
	policy := c.Origin.Policy(id, c.Obj)
	roles := DeclarationPolicy{
		Root:      policy.Root,
		Reconcile: policy.Reconcile,
		Report:    policy.Report && (!c.Generated || c.VisibleGeneration),
	}
	if c.Origin == manifest.SuppliedDeclaration {
		roles.role = suppliedRole
	}
	return roles
}

// Lookup MUST return the object and policy from one captured authority.
func (r *ReadSet) Lookup(id manifest.NamedResource) (manifest.BaseManifest, DeclarationPolicy) {
	input := r.input(id)
	obj := r.Capture(input)
	if obj == nil {
		return nil, input.view.child.input().Policy(id)
	}
	return obj, input.view.child.policy
}

// Policy MUST retain the origin captured with the object.
func (r *ReadSet) Policy(id manifest.NamedResource) DeclarationPolicy {
	_, policy := r.Lookup(id)
	return policy
}
