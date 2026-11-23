package selection

import (
	"context"
	"iter"
	"slices"
	"strings"
	"sync/atomic"

	"github.com/home-operations/flate/internal/engine/change"
	"github.com/home-operations/flate/internal/engine/store"
	"github.com/home-operations/flate/pkg/manifest"
)

// SourceContext borrows construction facts shared by one preparation.
type SourceContext struct {
	Root                       string
	Roots                      []NativeRootFacts
	InputReaders, InputObjects map[manifest.NamedResource][]manifest.NamedResource
	TemplateProjections        map[manifest.NamedResource]manifest.TemplateProjection
	EntryPath                  string
	NativeEntry                bool
	PlainEntry                 bool
	Frozen                     bool
	SideKnown                  bool
	Side                       manifest.TreeSide
}

// SourceReadPaths distinguishes exact files from subtrees.
type SourceReadPaths struct {
	Files []string
	Roots []string
}

func (p *SourceReadPaths) all() iter.Seq2[string, bool] {
	return func(yield func(string, bool) bool) {
		for _, root := range p.Roots {
			if !yield(root, false) {
				return
			}
		}
		for _, file := range p.Files {
			if !yield(file, true) {
				return
			}
		}
	}
}

func (p *SourceReadPaths) matches(file string) bool {
	if slices.Contains(p.Files, file) {
		return true
	}
	for _, root := range p.Roots {
		name := manifest.NormalizeClaimBase(root)
		if name == "" || name == "." || file == name || strings.HasPrefix(file, name+"/") {
			return true
		}
	}
	return false
}

// SourceFile preserves both source-relative and census-relative coordinates.
type SourceFile struct {
	Path    string
	Indexed string
}

// SourceFacts borrows the source owner's immutable qualified comparison.
type SourceFacts struct {
	Comparison           *change.Set
	Context              SourceContext
	EntryScopes          *atomic.Pointer[EntryScopes]
	Source               manifest.NamedResource
	Changes, Delta       *change.Set
	Snapshots            [2]*store.SourceArtifact
	Declarations         [2]manifest.BaseManifest
	Declaration          manifest.BaseManifest
	Input, Initial       *store.SourceArtifact
	ConfiguredInput      *store.SourceArtifact
	Origin               string
	Path                 string
	Files                []SourceFile
	Paired, InitialKnown bool
	Pinned, External     bool
	PendingGeneration    bool
	Derived              bool
}

// ProducerFacts preserves an immutable producer projection and its derivation witness.
type ProducerFacts struct {
	Declaration manifest.BaseManifest
	Derived     bool
	Occurrence  OccurrenceFacts
}

type census interface {
	ChartReadPaths(context.Context, *store.SourceArtifact, string) SourceReadPaths
	DeclarationContents() func(map[string]any) store.DeclarationContent
	ResolveRoot(context.Context, *store.ReadSet, manifest.NamedResource, string) (string, bool, error)
	OccurrenceObject(context.Context, *OccurrenceFacts, *store.SourceArtifact) (store.Child, error)
	SelectionIndex() *manifest.DiscoveryIndex
	Occurrence(manifest.NamedResource) (OccurrenceFacts, bool)
	AdmissionCandidate(manifest.NamedResource) store.Child
	OccurrenceRoots(manifest.NamedResource) (manifest.NamedResource, []string)
	PreparedObject(manifest.NamedResource, *store.ReadSet) (store.Child, bool)
	RootEvidence(manifest.NamedResource, *store.ReadSet) RootEvidence
	ProducerFacts(manifest.BaseManifest) ProducerFacts
	SourceFacts(manifest.NamedResource, *store.ReadSet) SourceFacts
}
