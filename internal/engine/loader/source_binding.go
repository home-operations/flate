package loader

import (
	"cmp"
	"hash/maphash"
	"sync/atomic"

	"github.com/home-operations/flate/internal/engine/change"
	"github.com/home-operations/flate/internal/engine/selection"
	"github.com/home-operations/flate/internal/engine/store"
	"github.com/home-operations/flate/pkg/manifest"
)

type admissionSource struct {
	source                   manifest.NamedResource
	changes, delta           *change.Set
	snapshots                [2]*store.SourceArtifact
	declarations             [2]manifest.BaseManifest
	declaration              manifest.BaseManifest
	input, initial           *store.SourceArtifact
	origin                   string
	files                    []selection.SourceFile
	bound, paired            bool
	filesKnown, initialKnown bool
	frozen, sideKnown        bool
	side                     manifest.TreeSide
}

type sourceBinding struct {
	source  manifest.NamedResource
	next    *sourceBinding
	current atomic.Pointer[admissionSource]
	// visited MUST remain exclusive to serial source initialization.
	visited bool
}

// SourceFacts MUST capture current reads before reusing source evidence.
func (l *Loader) SourceFacts(source manifest.NamedResource, reads *store.ReadSet) selection.SourceFacts {
	return l.admissionSourceFacts(source, reads, false)
}

func (l *Loader) admissionSourceFacts(
	source manifest.NamedResource,
	reads *store.ReadSet,
	initialize bool,
) selection.SourceFacts {
	source = cmp.Or(source, manifest.BootstrapSourceID)
	changes, snapshots, declarations := l.Options.Changes.ForSource(source)
	side, sideKnown := l.comparisonSide()
	evidence := admissionSource{
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
	evidence.input, evidence.declaration = input, declaration
	if cached := l.sourceBinding(source); cached != nil {
		if cached.reusable(l, &evidence, initialize) {
			evidence = *cached
			return l.sourceViewFacts(&evidence, false)
		}
		evidence.initial, evidence.initialKnown = cached.initial, cached.initialKnown
	}

	if initialize && !evidence.initialKnown {
		evidence.initial, evidence.initialKnown = input, true
	}
	defer l.publishSource(source, &evidence)
	evidence.delta, evidence.bound, evidence.paired = delta, true, false
	evidence.origin = l.SourceRoot
	if input != nil {
		evidence.origin = input.OriginPath(input.LocalPath)
	}
	for side, snapshot := range snapshots {
		if evidence.pairsSnapshot(manifest.TreeSide(side), snapshot) {
			evidence.paired = true
			return l.sourceViewFacts(&evidence, true)
		}
	}
	if source == manifest.BootstrapSourceID {
		return l.sourceViewFacts(&evidence, true)
	}
	location := sourceView(source, l.SourceRoot, input, declaration)
	if !location.Pinned && !location.External {
		delta, _, _ = l.Options.Changes.ForSource(manifest.BootstrapSourceID)
		evidence.delta = delta
		return l.sourceViewFacts(&evidence, true)
	}
	evidence.delta = nil
	return l.sourceViewFacts(&evidence, true)
}

func (e *admissionSource) pairsSnapshot(side manifest.TreeSide, snapshot *store.SourceArtifact) bool {
	input := e.input
	if snapshot == nil || input == nil || e.declaration != e.declarations[side] {
		return false
	}
	if !e.frozen {
		return snapshot == input && input.Reads.Valid()
	}
	const read = store.ArtifactContent | store.ArtifactRevision
	eligible := e.sideKnown && side == e.side && (e.initial == input || snapshot.Content != "")
	return eligible && snapshot.Input(read) == input.Input(read) && input.Reads.Valid()
}

func (e *admissionSource) reusable(l *Loader, current *admissionSource, initialize bool) bool {
	comparison := e.bound && e.changes == current.changes
	if !e.paired && e.source != manifest.BootstrapSourceID {
		location := sourceView(e.source, l.SourceRoot, e.input, e.declaration)
		if !location.Pinned && !location.External {
			delta, _, _ := l.Options.Changes.ForSource(manifest.BootstrapSourceID)
			comparison = comparison && e.delta == delta
		}
	}
	inputs := e.snapshots == current.snapshots && e.declarations == current.declarations &&
		e.matchesInputs(current.input, current.declaration)
	interpretation := e.frozen == current.frozen && e.sideKnown == current.sideKnown && e.side == current.side
	initialized := !initialize || e.initialKnown
	return comparison && inputs && interpretation && initialized
}

func (e *admissionSource) matchesInputs(input *store.SourceArtifact, declaration manifest.BaseManifest) bool {
	return e.input == input && e.declaration == declaration && (input == nil || input.Reads.Valid())
}

func (l *Loader) publishSource(source manifest.NamedResource, evidence *admissionSource) {
	bucket := l.sourceBucket(source)
	for {
		previous := bucket.Load()
		if binding := findSourceBinding(previous, source); binding != nil {
			binding.current.Store(new(*evidence))
			return
		}
		// Frozen membership MUST NOT expand for runtime discoveries.
		if l.frozen {
			return
		}
		next := &sourceBinding{source: source, next: previous}
		next.current.Store(new(*evidence))
		if bucket.CompareAndSwap(previous, next) {
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
				indexed, _ = e.input.IndexPath(l.SourceRoot, file)
			}
			e.files[i] = selection.SourceFile{Path: file, Indexed: indexed}
		}
		e.filesKnown = true
	}
	facts.Files = e.files
	return facts
}

func (l *Loader) sourceBinding(source manifest.NamedResource) *admissionSource {
	if binding := findSourceBinding(l.sourceBucket(source).Load(), source); binding != nil {
		return binding.current.Load()
	}
	return nil
}

func findSourceBinding(binding *sourceBinding, source manifest.NamedResource) *sourceBinding {
	for ; binding != nil; binding = binding.next {
		if binding.source == source {
			return binding
		}
	}
	return nil
}

var sourceBindingSeed = maphash.MakeSeed()

func (l *Loader) sourceBucket(
	source manifest.NamedResource,
) *atomic.Pointer[sourceBinding] {
	return &l.bindings[maphash.Comparable(sourceBindingSeed, source)%uint64(len(l.bindings))]
}
