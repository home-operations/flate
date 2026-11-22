package selection

import (
	"slices"

	"github.com/home-operations/flate/internal/engine/store"
	"github.com/home-operations/flate/pkg/manifest"
)

// Result contains the objects and diagnostics projected onto requested roots.
// Borrowed manifests and document batches MUST remain immutable in use.
type Result struct {
	LookupMatched bool
	LookupFailed  bool
	// Subjects maps raw identities to captured display context.
	Subjects        map[manifest.NamedResource]manifest.DiagnosticIdentity
	Manifests       []manifest.DocumentBatch
	Failed          map[manifest.NamedResource]store.StatusInfo
	Blocked         map[manifest.NamedResource][]manifest.NamedResource
	Warnings        []manifest.Warning
	ClusterWarnings []manifest.Warning
	// Objects contains selected roots after metadata filtering, grouped by requested kind.
	// Unchanged catalog-only roots expose reporting facts, never complete specs or execution authority.
	Objects []manifest.BaseManifest
	// Counts records matched roots per requested kind before label filtering.
	Counts map[string]int
	// Statuses contains roster statuses and eligible diagnostic failures. Blocked
	// messages are prepared for test reports without changing dependency identity.
	Statuses map[manifest.NamedResource]store.StatusInfo
	// ChartVersions contains versions resolved from selected HR chartRef sources.
	ChartVersions map[manifest.NamedResource]string
	// Causes maps prepared failure roots to sorted distinct blocked victims.
	// Missing roots have no Failed entry; closed cycle roots carry prepared Failed messages.
	Causes map[manifest.NamedResource][]manifest.NamedResource
	// Unavailable contains canonical failure, soft-skip or suspension evidence for
	// paired producer output, independently of fatal namespace scope.
	Unavailable map[manifest.NamedResource]store.StatusInfo
	Bindings    store.ProducerBindings
	// Unrelated contains prepared advisories for failures outside named fatal scope.
	Unrelated []manifest.Warning
	// External contains paired external-source skip disclosures, honoring explicit namespace scope.
	External []manifest.Warning
	// Producers maps batch and document positions to emitters that differ from the reporting parent.
	Producers map[int]map[int]manifest.DiagnosticIdentity
}

// BlockedCount excludes dependency-cycle roots promoted to primary failures.
func (r *Result) BlockedCount() int {
	count := len(r.Blocked)
	for root := range r.Causes {
		if _, isFailed := r.Failed[root]; isFailed {
			if _, promoted := r.Blocked[root]; promoted {
				count--
			}
		}
	}
	return count
}

// Diagnostic returns prepared display context, including ordinary absent objects.
func (r *Result) Diagnostic(id manifest.NamedResource) manifest.DiagnosticIdentity {
	if subject, ok := r.Subjects[id]; ok {
		return subject
	}
	return manifest.DiagnosticFor(id, nil)
}

func (s *selectionSide) prepareExternal(root manifest.NamedResource, obj manifest.BaseManifest) {
	src, _, _ := obj.SourcePath()
	id := obj.Named()
	if !(Request{Roots: []manifest.NamedResource{root}}).Matches(id) {
		return
	}
	if s.run.Facts == nil || !s.run.Facts.SourceExternal(src, &s.reads) {
		return
	}
	info := s.status(id)
	if info.Outcome() != store.OutcomeUnchanged && info.Outcome() != store.OutcomeSkipped {
		return
	}
	entry := s.run.Entry.Producer
	ownsReader := entry != (manifest.NamedResource{}) && s.run.Store.EmissionOwner(id) == entry
	if ownsReader && s.reads.GeneratedDocument(entry) {
		owner := s.run.Store.EmissionOwner(src)
		ownsGenerator := owner != (manifest.NamedResource{}) && s.run.Store.EmissionOwner(owner) == entry
		input, _ := s.reads.GetArtifact(src, store.ArtifactContent).(*store.SourceArtifact)
		readableUnmounted := input != nil && input.FS != nil && input.LocalPath == ""
		if ownsGenerator && readableUnmounted {
			return
		}
	}
	if slices.ContainsFunc(s.run.Warnings, func(w manifest.Warning) bool {
		return w.Resource == src && w.Category == manifest.WarnSourceCredentialsUnavailable
	}) {
		return
	}
	s.result.External = append(
		s.result.External,
		manifest.Warning{
			Resource: id,
			Subject:  s.result.Diagnostic(id),
			Related:  []manifest.NamedResource{src},
			Category: manifest.WarnExternalSource,
			Message:  "external source " + s.diagnostic(src).String() + " is outside the local tree; not rendered",
		},
	)
}
