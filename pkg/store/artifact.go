package store

import (
	"context"
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

// SourceArtifact retains content and metadata acquired by a source owner.
type SourceArtifact struct {
	// Reads retains the declaration and material that produced this acquisition.
	Reads     *ReadSet
	Kind      string
	URL       string
	LocalPath string
	Revision  string
	// Content identifies the exact acquired filesystem generation and shaping.
	Content  string
	Digest   string
	Size     int64
	Metadata map[string]string

	// LocalRoot is the selected input's ownership origin, which may be synthetic.
	// Only an admitted bootstrap uses its original checkout root for Git metadata.
	LocalRoot string

	// Key identifies an acquisition family; Content identifies its admitted generation.
	// Empty Content with FS denotes mutable content requiring cache validation.
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
	// UsedLocalSSHAgent records successful acquisition without affecting content identity.
	UsedLocalSSHAgent bool
	// ContentFingerprint lazily identifies selected bytes at their source owner.
	// Nil or an empty result supplies no equality evidence; acquisition identity stays separate.
	ContentFingerprint func(context.Context, fs.FS) (string, error) `json:"-"`
}

func (*SourceArtifact) artifact() {}

// ArtifactRead selects the source facts consumed by an execution.
type ArtifactRead uint8

const (
	// ArtifactContent selects content availability, generation and source binding.
	ArtifactContent ArtifactRead = 1 << iota
	// ArtifactRevision selects the human-readable source revision.
	ArtifactRevision
	// ArtifactOrigin selects origin revision and its presence.
	ArtifactOrigin
)

// ArtifactInput is the comparable projection of selected source facts.
type ArtifactInput struct {
	Present, Readable                                bool
	Kind, Key, Content, Digest, LocalRoot, LocalPath string
	InputAlias                                       bool
	Revision, Origin                                 string
	OriginPresent                                    bool
}

// Input returns the selected facts without retaining filesystem handles or metadata maps.
func (a *SourceArtifact) Input(read ArtifactRead) ArtifactInput {
	if a == nil {
		return ArtifactInput{}
	}
	var input ArtifactInput
	if read&ArtifactContent != 0 {
		input = ArtifactInput{Present: true, Readable: a.FS != nil, Kind: a.Kind, Key: a.Key, Content: a.Content, Digest: a.Digest, LocalRoot: a.LocalRoot, LocalPath: a.LocalPath, InputAlias: a.InputAlias}
	}
	if read&ArtifactRevision != 0 {
		input.Revision = a.Revision
	}
	if read&ArtifactOrigin != 0 {
		input.Origin, input.OriginPresent = a.Metadata["org.opencontainers.image.revision"]
	}
	return input
}

type artifactSnapshot struct {
	value       Artifact
	publication uint64
}

type artifactRecord struct {
	artifactSnapshot
	effective Artifact
	authority
	revisionHead, originHead authority
}

// SetArtifact stores the complete latest descriptor and notifies selected readers.
func (s *Store) SetArtifact(id manifest.NamedResource, artifact Artifact) {
	var input ArtifactInput
	if a, ok := artifact.(*SourceArtifact); ok {
		input = a.Input(ArtifactContent | ArtifactRevision | ArtifactOrigin)
	}
	sh := s.shardFor(id)
	for {
		var snapshot artifactSnapshot
		_, source := artifact.(*SourceArtifact)
		if !source {
			sh.mu.RLock()
			snapshot = sh.artifacts[id].artifactSnapshot
			sh.mu.RUnlock()
			if artifactValueEqual(snapshot.value, artifact) {
				return
			}
		}
		sh.mu.Lock()
		prev := sh.artifacts[id]
		if !source && prev.publication != snapshot.publication {
			sh.mu.Unlock()
			continue
		}
		if prev.value == artifact {
			sh.mu.Unlock()
			return
		}
		var retired *ReadSet
		if before, ok := prev.value.(*SourceArtifact); ok {
			retired = before.Reads
			if a, ok := artifact.(*SourceArtifact); ok && before.Reads == a.Reads {
				retired = nil
			}
		}
		prev.value = artifact
		if sh.derived {
			var view capture
			sh.captureLocked(id, sh.cells[id], &view)
			if sh.cells[id].hidden {
				artifact, input = nil, ArtifactInput{}
			} else if view.payload != nil {
				artifact, input = view.payload, *view.payloadInput
			}
		}
		changed := sh.publishArtifactLocked(id, &prev, artifact, input)
		sh.artifacts[id] = prev
		var dispatch eventDispatch
		if changed && !sh.cells[id].hidden {
			dispatch = fireUnderLock(s, EventArtifactUpdated, id)
			dispatch.payload = artifact
		}
		sh.mu.Unlock()
		retired.Retire()
		dispatch.run()
		return
	}
}

func (sh *shard) publishArtifactLocked(id manifest.NamedResource, record *artifactRecord, value Artifact, input ArtifactInput) bool {
	var before ArtifactInput
	if source, ok := record.effective.(*SourceArtifact); ok {
		before = source.Input(ArtifactContent | ArtifactRevision | ArtifactOrigin)
	}
	revision, origin, originPresent := before.Revision, before.Origin, before.OriginPresent
	after := input
	before.Revision, before.Origin, before.OriginPresent = "", "", false
	after.Revision, after.Origin, after.OriginPresent = "", "", false
	changed := ArtifactRead(0)
	if before != after || input.Readable && input.Content == "" && record.effective != value {
		changed |= ArtifactContent
	}
	if _, source := value.(*SourceArtifact); !source && record.effective != value {
		changed |= ArtifactContent
	}
	if revision != input.Revision {
		changed |= ArtifactRevision
	}
	if origin != input.Origin || originPresent != input.OriginPresent {
		changed |= ArtifactOrigin
	}
	heads := [3]*authority{&record.authority, &record.revisionHead, &record.originHead}
	var admissions uint64
	for i, head := range heads {
		if changed&(1<<i) != 0 {
			sh.requireCapacityLocked(*head)
			if head.generation == 0 {
				admissions++
			}
		}
	}
	if record.publication == ^uint64(0) || !sh.canAdmit(admissions) {
		sh.mu.Unlock()
		panic("store: authority generation exhausted")
	}
	record.publication++
	for i, head := range heads {
		if changed&(1<<i) != 0 {
			if head.generation == 0 {
				sh.admitAbsenceLocked(id, contentFacet+facet(i))
			}
			*head = sh.advanceLocked(*head)
		}
	}
	record.effective = value
	return changed != 0
}

// GetArtifact returns the artifact for id, or nil if none was set.
func (s *Store) GetArtifact(id manifest.NamedResource, _ ArtifactRead) Artifact {
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
func artifactValueEqual(prev, next Artifact) bool {
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
	if aok && bok && (a == nil || b == nil) {
		return a == b
	}
	if !aok || !bok {
		return reflect.DeepEqual(prev, next)
	}
	if (a.FS == nil) != (b.FS == nil) || a.FS != nil && (a.Content == "" || b.Content == "") {
		return false
	}
	ac, bc := *a, *b
	ac.FS, bc.FS = nil, nil
	ac.Reads, bc.Reads = nil, nil
	ac.Warnings, bc.Warnings = nil, nil
	ac.UsedLocalSSHAgent, bc.UsedLocalSSHAgent = false, false
	ac.ContentFingerprint, bc.ContentFingerprint = nil, nil
	return reflect.DeepEqual(ac, bc)
}

func emissionCurrent(e *Emission, view capture) bool {
	input := view.child
	binding := view.sourceBinding()
	equal, _ := reconcileInputEqual(e.input, e.inputDoc, input.Doc)
	if !equal || !e.inputHead.binding.equal(&binding) {
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

func (sh *shard) clearFacetsLocked(id manifest.NamedResource) {
	sh.requireCapacityLocked(sh.conditions[id].stamp(), sh.artifacts[id].authority, sh.artifacts[id].revisionHead, sh.artifacts[id].originHead)
	if record := sh.conditions[id]; len(record.conditions) > 0 || record.failure != nil {
		record.advanceLocked(sh)
		record.conditions, record.failure = record.conditions[:0], nil
		sh.conditions[id] = record
	}
	if record := sh.artifacts[id]; record.value != nil {
		record.value = nil
		sh.publishArtifactLocked(id, &record, nil, ArtifactInput{})
		sh.artifacts[id] = record
	}
}
