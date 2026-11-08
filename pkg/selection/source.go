package selection

import (
	"path/filepath"

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
	ToIndex              string
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
	SelectionIndex() *manifest.DiscoveryIndex
	Occurrence(manifest.NamedResource) (OccurrenceFacts, bool)
	AdmissionCandidate(manifest.NamedResource) store.Child
	OccurrenceRoots(manifest.NamedResource) (manifest.NamedResource, []string)
	PreparedObject(manifest.NamedResource, *store.ReadSet) (store.Child, bool)
	RootEvidence(manifest.NamedResource, *store.ReadSet) RootEvidence
	ProducerFacts(manifest.BaseManifest) ProducerFacts
	SourceFacts(manifest.NamedResource, *store.ReadSet) SourceFacts
}

// Coordinates projects an absolute path through the captured source origin.
//
//nolint:gocritic // Both coordinate spaces MUST remain explicit in the public result.
func (f *SourceFacts) Coordinates(root, path string) (string, string) {
	art := f.Input
	sourceRoot := root
	if art != nil {
		sourceRoot = art.LocalPath
	}
	rel, err := filepath.Rel(sourceRoot, path)
	if err != nil {
		return "", ""
	}
	relative := filepath.ToSlash(rel)
	indexed := relative
	if f.ToIndex != "." {
		physical := path
		if art != nil && f.Origin != art.LocalPath {
			physical = filepath.Join(f.Origin, rel)
		}
		rel, err = filepath.Rel(root, physical)
		if err != nil {
			return "", ""
		}
		indexed = filepath.ToSlash(rel)
	}
	return relative, indexed
}
