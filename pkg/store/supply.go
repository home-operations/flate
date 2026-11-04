package store

import "github.com/home-operations/flate/pkg/manifest"

// Declaration associates an immutable input object with its origin and provenance.
type Declaration struct {
	Obj       manifest.BaseManifest
	Doc       map[string]any
	Origin    manifest.DeclarationOrigin
	policy    manifest.DeclarationPolicy
	InputPath string
	Ordinal   int
	Warnings  []manifest.Warning `json:"-"`
}

func (d *Declaration) child() Child {
	return Child{Obj: d.Obj, Doc: d.Doc, Origin: d.Origin, policy: d.policy, Warnings: d.Warnings}
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
		s.AddObject(child.Obj, child.Doc)
		return
	}
	id := child.Obj.Named()
	child.policy = child.child().Policy(id)
	sh := s.shardFor(id)
	sh.mu.Lock()
	cell := sh.cells[id]
	if cell.reference != nil &&
		cell.reference.Obj == child.Obj &&
		cell.reference.InputPath == child.InputPath &&
		cell.reference.Ordinal == child.Ordinal {
		sh.mu.Unlock()
		return
	}
	bucket := sh.byName[kindID{id.Group, id.Kind}]
	sh.requireCapacityLocked(cell.authority, bucket.authority)
	if cell.generation == 0 {
		sh.admitAbsenceLocked(id, objectFacet)
		cell.links = sh.newHeadLocked(0)
	}
	if bucket.links == nil {
		sh.admitAbsenceLocked(manifest.NamedResource{Group: id.Group, Kind: id.Kind}, bucketFacet)
		bucket.links = sh.newHeadLocked(bucket.generation)
	}
	cell.reference = &child
	cell.publication++
	cell.authority = sh.advanceLocked(cell.current())
	obj, policy := sh.captureLocked(id, new(cell), nil)
	cell.policy = policy
	sh.cells[id] = cell
	if bucket.update(nameID{id.Namespace, id.Name}, true, true, bucket.generation) {
		bucket.authority = sh.enqueueLocked(bucket.authority)
		sh.byName[kindID{id.Group, id.Kind}] = bucket
	}
	event := fireUnderLock(s, EventObjectAdded, id)
	event.payload = obj
	sh.mu.Unlock()
	event.run()
}

// Lookup returns the coherent representative and its declaration roles.
//
//nolint:ireturn // The heterogeneous store MUST preserve each manifest's concrete type.
func (s *Store) Lookup(id manifest.NamedResource) (manifest.BaseManifest, manifest.DeclarationPolicy) {
	sh := s.shardFor(id)
	sh.mu.RLock()
	obj, policy := sh.captureLocked(id, new(sh.cells[id]), nil)
	sh.mu.RUnlock()
	if obj == nil {
		policy = manifest.RepositoryDeclaration.Policy(id, nil)
	}
	return obj, policy
}

// Policy projects the coherent representative's declaration roles.
func (s *Store) Policy(id manifest.NamedResource) manifest.DeclarationPolicy {
	sh := s.shardFor(id)
	sh.mu.RLock()
	policy := sh.policyLocked(id)
	sh.mu.RUnlock()
	return policy
}

func (sh *shard) policyLocked(id manifest.NamedResource) manifest.DeclarationPolicy {
	cell := sh.cells[id]
	if (cell.policy.Root || cell.policy.Origin != manifest.RepositoryDeclaration) && cell.bound() {
		return cell.policy
	}
	obj, policy := sh.captureLocked(id, new(cell), nil)
	if obj == nil {
		return manifest.RepositoryDeclaration.Policy(id, nil)
	}
	return policy
}

// Policy derives execution roles from an incoming or captured declaration envelope.
//
//nolint:gocritic // Immutable value inputs preserve snapshot semantics.
func (c Child) Policy(id manifest.NamedResource) manifest.DeclarationPolicy {
	policy := c.Origin.Policy(id, c.Obj)
	policy.Report = policy.Report && !c.Generated
	return policy
}

// Lookup MUST return the object and policy from one captured authority.
//
//nolint:ireturn // The heterogeneous store MUST preserve each manifest's concrete type.
func (r *ReadSet) Lookup(id manifest.NamedResource) (manifest.BaseManifest, manifest.DeclarationPolicy) {
	input := r.input(id)
	obj := r.Capture(input)
	if obj == nil {
		return nil, input.view.child.Policy(id)
	}
	return obj, input.view.child.policy
}

// Policy MUST retain the origin captured with the object.
func (r *ReadSet) Policy(id manifest.NamedResource) manifest.DeclarationPolicy {
	_, policy := r.Lookup(id)
	return policy
}
