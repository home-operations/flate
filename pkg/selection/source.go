package selection

import (
	"context"

	"github.com/home-operations/flate/pkg/change"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
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

// SourceFile preserves both source-relative and census-relative coordinates.
type SourceFile struct {
	Path    string
	Indexed string
}

// SourceFacts borrows the source owner's immutable qualified comparison.
type SourceFacts struct {
	Comparison           *change.Set
	Context              SourceContext
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
	Derived              bool
}

// ProducerFacts preserves an immutable producer projection and its derivation witness.
type ProducerFacts struct {
	Declaration manifest.BaseManifest
	Derived     bool
	Occurrence  OccurrenceFacts
}

type census interface {
	DeclarationContents() func(map[string]any) store.DeclarationContent
	ResolveRoot(context.Context, *store.ReadSet, manifest.NamedResource, string) (string, bool, error)
	OccurrenceObject(context.Context, *OccurrenceFacts, *store.SourceArtifact) (store.Child, error)
	SelectionIndex() *manifest.DiscoveryIndex
	Occurrence(manifest.NamedResource) (OccurrenceFacts, bool)
	AdmissionCandidate(manifest.NamedResource) store.Child
	OccurrenceRoots(manifest.NamedResource) (manifest.NamedResource, []string)
	PreparedObject(manifest.NamedResource, *store.ReadSet) (store.Child, bool)
	RootEvidence(manifest.NamedResource, *store.ReadSet, bool) RootEvidence
	ProducerFacts(manifest.BaseManifest) ProducerFacts
	SourceFacts(manifest.NamedResource, *store.ReadSet) SourceFacts
}
