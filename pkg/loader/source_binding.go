package loader

import (
	"hash/maphash"
	"maps"
	"path/filepath"
	"sync/atomic"

	"github.com/home-operations/flate/pkg/change"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/selection"
	"github.com/home-operations/flate/pkg/store"
)

type admissionSource struct {
	source                   manifest.NamedResource
	changes, delta           *change.Set
	snapshots                [2]*store.SourceArtifact
	declarations             [2]manifest.BaseManifest
	declaration              manifest.BaseManifest
	input, initial           *store.SourceArtifact
	origin                   string
	toIndex                  string
	files                    []selection.SourceFile
	bound, paired            bool
	filesKnown, initialKnown bool
	frozen, sideKnown        bool
	side                     manifest.TreeSide
}

// SourceFacts borrows comparison facts while capturing current source authority.
func (l *Loader) SourceFacts(source manifest.NamedResource, reads *store.ReadSet) selection.SourceFacts {
	if source == (manifest.NamedResource{}) {
		source = manifest.BootstrapSourceID
	}
	changes, snapshots, declarations := l.Options.Changes.ForSource(source)
	side, sideKnown := l.comparisonSide()
	evidence := &admissionSource{
		source: source, changes: changes, snapshots: snapshots, declarations: declarations,
		frozen: l.frozen, sideKnown: sideKnown, side: side,
	}
	var objects interface {
		GetArtifact(manifest.NamedResource, store.ArtifactRead) store.Artifact
		GetObject(manifest.NamedResource) manifest.BaseManifest
	} = l.Store
	if reads != nil {
		objects = reads
	}
	delta := changes
	input, _ := objects.GetArtifact(source, store.ArtifactContent|store.ArtifactRevision).(*store.SourceArtifact)
	declaration := objects.GetObject(source)
	if cached := l.sourceBinding(source); cached != nil {
		if cached.matchesComparison(l, evidence) && cached.matchesInputs(input, declaration) {
			*evidence = *cached
			return l.sourceViewFacts(evidence, false)
		}
		evidence.initial, evidence.initialKnown = cached.initial, cached.initialKnown
	}

	derived := true
	defer func() { l.publishSource(source, evidence) }()
	evidence.declaration = declaration
	evidence.delta, evidence.input, evidence.bound, evidence.paired = delta, input, true, false
	evidence.origin = l.SourceRoot
	if input != nil {
		evidence.origin = input.OriginPath(input.LocalPath)
	}
	evidence.toIndex, _ = filepath.Rel(l.SourceRoot, evidence.origin)
	for side, snapshot := range snapshots {
		if snapshot == nil || input == nil || declaration != declarations[side] {
			continue
		}
		bound := snapshot == input
		if l.frozen {
			bound = evidence.sideKnown && manifest.TreeSide(side) == evidence.side && evidence.initial == input &&
				snapshot.Input(
					store.ArtifactContent|store.ArtifactRevision,
				) == input.Input(
					store.ArtifactContent|store.ArtifactRevision,
				)
		}
		if bound && input.Reads.Valid() {
			evidence.paired = true
			return l.sourceViewFacts(evidence, derived)
		}
	}
	if source == manifest.BootstrapSourceID {
		return l.sourceViewFacts(evidence, derived)
	}
	location := sourceView(source, l.SourceRoot, input, declaration)
	if !location.Pinned && !location.External {
		delta, _, _ = l.Options.Changes.ForSource(manifest.BootstrapSourceID)
		evidence.delta = delta
		return l.sourceViewFacts(evidence, derived)
	}
	evidence.delta = nil
	return l.sourceViewFacts(evidence, derived)
}

func (e *admissionSource) matchesComparison(l *Loader, current *admissionSource) bool {
	comparison := e.bound && e.changes == current.changes
	if !e.paired && e.source != manifest.BootstrapSourceID {
		location := sourceView(e.source, l.SourceRoot, e.input, e.declaration)
		if !location.Pinned && !location.External {
			delta, _, _ := l.Options.Changes.ForSource(manifest.BootstrapSourceID)
			comparison = comparison && e.delta == delta
		}
	}
	inputs := e.snapshots == current.snapshots && e.declarations == current.declarations
	interpretation := e.frozen == current.frozen && e.sideKnown == current.sideKnown && e.side == current.side
	return comparison && inputs && interpretation
}

func (e *admissionSource) matchesInputs(input *store.SourceArtifact, declaration manifest.BaseManifest) bool {
	return e.input == input && e.declaration == declaration && (input == nil || input.Reads.Valid())
}

func (l *Loader) publishSource(source manifest.NamedResource, evidence *admissionSource) {
	bucket := l.sourceBucket(source)
	for {
		previous := bucket.Load()
		if previous != nil {
			if slot := (*previous)[source]; slot != nil {
				slot.Store(new(*evidence))
				return
			}
		}
		// Runtime discoveries MUST NOT copy the catalog to cache one source.
		if l.frozen {
			return
		}
		next := make(map[manifest.NamedResource]*atomic.Pointer[admissionSource])
		if previous != nil {
			maps.Copy(next, *previous)
		}
		slot := new(atomic.Pointer[admissionSource])
		slot.Store(new(*evidence))
		next[source] = slot
		if bucket.CompareAndSwap(previous, &next) {
			return
		}
	}
}

func (l *Loader) sourceViewFacts(e *admissionSource, derived bool) selection.SourceFacts {
	location := sourceView(e.source, l.SourceRoot, e.input, e.declaration)
	facts := selection.SourceFacts{
		Context:         l.selectionContext(),
		Comparison:      l.Options.Changes,
		ConfiguredInput: l.Options.ConfiguredInput,
		Source:          e.source,
		Changes:         e.changes,
		Delta:           e.delta,
		Snapshots:       e.snapshots,
		Declarations:    e.declarations,
		Declaration:     e.declaration,
		Input:           e.input,
		Initial:         e.initial,
		Origin:          e.origin,
		Path:            location.Path,
		ToIndex:         e.toIndex,
		Files:           e.files,
		Paired:          e.paired,
		InitialKnown:    e.initialKnown,
		Pinned:          location.Pinned,
		External:        location.External,
		Derived:         derived,
	}
	if !e.filesKnown {
		paths := e.delta.Paths()
		e.files = make([]selection.SourceFile, len(paths))
		for i, file := range paths {
			indexed := file
			if e.paired {
				physical := filepath.Join(e.input.LocalPath, filepath.FromSlash(file))
				if filepath.IsAbs(physical) {
					_, indexed = facts.Coordinates(l.SourceRoot, physical)
				}
			}
			e.files[i] = selection.SourceFile{Path: file, Indexed: indexed}
		}
		e.filesKnown = true
	}
	facts.Files = e.files
	return facts
}

func (l *Loader) sourceBinding(source manifest.NamedResource) *admissionSource {
	if bindings := l.sourceBucket(source).Load(); bindings != nil {
		if slot := (*bindings)[source]; slot != nil {
			return slot.Load()
		}
	}
	return nil
}

//nolint:gochecknoglobals // Immutable process seed distributes source identities across publication buckets.
var sourceBindingSeed = maphash.MakeSeed()

func (l *Loader) sourceBucket(
	source manifest.NamedResource,
) *atomic.Pointer[map[manifest.NamedResource]*atomic.Pointer[admissionSource]] {
	return &l.bindings[maphash.Comparable(sourceBindingSeed, source)%uint64(len(l.bindings))]
}
