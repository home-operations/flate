package store

import (
	"io/fs"
	"reflect"
	"slices"

	"github.com/home-operations/flate/pkg/manifest"
)

// Artifact is a marker interface implemented by every artifact type.
// Controllers type-assert to the concrete type they expect.
type Artifact interface {
	artifact()
}

// SourceArtifact is the unified working-tree artifact produced by
// source fetchers (GitRepository, OCIRepository, Bucket, ExternalArtifact, …).
// Kind identifies which CR kind produced it so consumers that care
// (e.g. the helm-controller's local-git registration) can filter
// without the previous per-kind type union.
//
// Mirrors Flux's meta.Artifact contract: URL is the upstream address,
// Revision is the human-readable identifier (branch:main, tag:v1.2.3,
// commit sha), Digest is the content-addressed verification value,
// Size is the artifact size in bytes when known, and Metadata holds
// kind-specific annotations (OCI image annotations, bucket ETag…).
type SourceArtifact struct {
	// Reads retains the declaration and material that produced this acquisition.
	Reads     *ReadSet
	Kind      string
	URL       string
	LocalPath string
	Revision  string
	Digest    string
	Size      int64
	Metadata  map[string]string

	// LocalRoot is the selected input's ownership origin, which may be synthetic.
	// Only an admitted bootstrap uses its original checkout root for Git metadata.
	LocalRoot string

	// Key identifies an immutable acquisition family; Revision and Digest select its generation.
	// Empty Key with FS denotes mutable content requiring cache validation.
	// Nonempty Key without FS requires acquisition before reading.
	Key string
	// InputAlias marks source shaping derived from the selected bootstrap input,
	// rather than an authored committed source pin.
	InputAlias bool
	// FS supplies acquired content; LocalPath and LocalRoot are coordinates only.
	// Nil FS is unavailable or existence-only, regardless of Key or LocalPath.
	FS fs.FS
	// Warnings is immutable acquisition evidence, excluded from content identity.
	Warnings []manifest.Warning
}

func (*SourceArtifact) artifact() {}

type artifactRecord struct {
	value Artifact
	authority
}

// SetArtifact stores fetched content or a rendered inventory and notifies live readers.
// Equal payloads are deduplicated; hidden writes remain private.
func (s *Store) SetArtifact(id manifest.NamedResource, artifact Artifact) {
	sh := s.shardFor(id)
	sh.mu.Lock()
	prev, exists := sh.artifacts[id]
	if exists && sameArtifact(prev.value, artifact) {
		var retired *ReadSet
		if a, ok := artifact.(*SourceArtifact); ok && a.Reads != nil {
			if before, ok := prev.value.(*SourceArtifact); ok && before.Reads != a.Reads {
				retired = before.Reads
				prev.value = artifact
				sh.artifacts[id] = prev
			}
		}
		sh.mu.Unlock()
		retired.Retire()
		return
	}
	sh.requireCapacityLocked(prev.authority)
	if prev.generation == 0 {
		if !sh.canAdmit(1) {
			sh.mu.Unlock()
			panic("store: authority generation exhausted")
		}
		sh.admitAbsenceLocked(id, artifactFacet)
	}
	prev.authority, prev.value = sh.advanceLocked(prev.authority), artifact
	sh.artifacts[id] = prev
	if sh.derived && sh.cells[id].hidden {
		sh.mu.Unlock()
		return
	}
	dispatch := fireUnderLock(s, EventArtifactUpdated, id)
	dispatch.payload = artifact
	sh.mu.Unlock()
	dispatch.run()
}

// GetArtifact returns the artifact for id, or nil if none was set.
func (s *Store) GetArtifact(id manifest.NamedResource) Artifact {
	sh := s.shardFor(id)
	sh.mu.RLock()
	if sh.derived && sh.cells[id].hidden {
		sh.mu.RUnlock()
		return nil
	}
	var view capture
	sh.captureLocked(id, sh.cells[id], &view)
	artifact := sh.artifacts[id].value
	if view.payload != nil {
		artifact = view.payload
	}
	e, emitted := artifact.(*Emission)
	sh.mu.RUnlock()
	if emitted && !emissionCurrent(e, view) {
		return nil
	}
	return artifact
}

// Family and generation metadata pin payload identity; never walk a filesystem under the shard lock.
func sameArtifact(prev, next Artifact) bool {
	if prev == next {
		return true
	}
	if a, ok := prev.(*Emission); ok {
		b, ok := next.(*Emission)
		return ok && a != nil && b != nil && a.Complete == b.Complete &&
			(a.Children == nil) == (b.Children == nil) &&
			slices.EqualFunc(a.Children, b.Children, childEqual) && a.sameContent(b)
	}
	a, aok := prev.(*SourceArtifact)
	b, bok := next.(*SourceArtifact)
	if !aok || !bok {
		return reflect.DeepEqual(prev, next)
	}
	if (a.FS == nil) != (b.FS == nil) || a.FS != nil && (a.Key == "" || b.Key == "") {
		return false
	}
	ac, bc := *a, *b
	ac.FS, bc.FS = nil, nil
	ac.Reads, bc.Reads = nil, nil
	ac.Warnings, bc.Warnings = nil, nil
	return reflect.DeepEqual(ac, bc)
}

func emissionCurrent(e *Emission, view capture) bool {
	input := view.child
	if !reconcileDocumentEqual(e.input, e.inputDoc, input.Doc) || e.inputHead.binding != view.source {
		return false
	}
	if e.input == nil || e.input == input.Obj {
		return true
	}
	if e.validated != nil {
		if validated := e.validated.Load(); validated != nil && *validated == input.Obj {
			return true
		}
	}
	if !semanticEqual(Child{Obj: e.input, Doc: e.inputDoc}, input) {
		return false
	}
	if e.validated != nil {
		validated := new(manifest.BaseManifest)
		*validated = input.Obj
		e.validated.Store(validated)
	}
	return true
}

func payloadEqual(a, b *SourceArtifact) bool {
	if a == nil || b == nil {
		return a == b
	}
	return a == b || sameArtifact(a, b)
}

func (sh *shard) clearFacetsLocked(id manifest.NamedResource) {
	sh.requireCapacityLocked(sh.conditions[id].stamp(), sh.artifacts[id].authority)
	if record := sh.conditions[id]; len(record.conditions) > 0 || record.failure != nil {
		record.advanceLocked(sh)
		record.conditions, record.failure = record.conditions[:0], nil
		sh.conditions[id] = record
	}
	if record := sh.artifacts[id]; record.value != nil {
		record.authority, record.value = sh.advanceLocked(record.authority), nil
		sh.artifacts[id] = record
	}
}
