package store

import (
	"io/fs"
	"reflect"

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

// --- Store operations on artifacts ---

// SetArtifact stores fetched content or a rendered inventory and notifies live readers.
// Equal payloads are deduplicated; hidden writes remain private.
func (s *Store) SetArtifact(id manifest.NamedResource, artifact Artifact) {
	sh := s.shardFor(id)
	sh.mu.Lock()
	prev, exists := sh.artifacts[id]
	// Dedup cheapest-first: pointer identity (the hot path when a
	// fetcher caches its own SourceArtifact and re-publishes the same
	// pointer every refresh tick, skipping reflection entirely) before
	// the reflect.DeepEqual fallback for distinct-pointer equal content.
	if exists && (prev == artifact || sameArtifact(prev, artifact)) {
		sh.mu.Unlock()
		return
	}
	sh.artifacts[id] = artifact
	if sh.hidden[id] {
		sh.mu.Unlock()
		return
	}
	dispatch := fireUnderLock(s, EventArtifactUpdated, id, artifact)
	sh.mu.Unlock()
	dispatch()
}

// GetArtifact returns the artifact for id, or nil if none was set.
func (s *Store) GetArtifact(id manifest.NamedResource) Artifact {
	sh := s.shardFor(id)
	sh.mu.RLock()
	if sh.hidden[id] {
		sh.mu.RUnlock()
		return nil
	}
	artifact := sh.artifacts[id]
	e, emitted := artifact.(*Emission)
	var input Child
	if emitted && e.input != nil {
		input, _ = sh.representativeLocked(id)
	}
	sh.mu.RUnlock()
	if emitted && !emissionCurrent(e, input) {
		return nil
	}
	return artifact
}

// Family and generation metadata pin payload identity; never walk a filesystem under the shard lock.
func sameArtifact(prev, next Artifact) bool {
	a, aok := prev.(*SourceArtifact)
	b, bok := next.(*SourceArtifact)
	if !aok || !bok {
		return reflect.DeepEqual(prev, next)
	}
	if (a.FS != nil || b.FS != nil) && ((a.FS == nil) != (b.FS == nil) || a.Key == "" || b.Key == "") {
		return false
	}
	ac, bc := *a, *b
	ac.FS, bc.FS = nil, nil
	ac.Warnings, bc.Warnings = nil, nil
	return reflect.DeepEqual(ac, bc)
}

func emissionCurrent(e *Emission, input Child) bool {
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
