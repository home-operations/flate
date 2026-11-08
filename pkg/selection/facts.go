package selection

import (
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

// OccurrenceID preserves document and List-item coordinates within a source.
type OccurrenceID struct {
	Source   string
	File     string
	Document int
	Item     int
}

// OccurrenceFacts borrows immutable parsed data; its collections must not be mutated.
type OccurrenceFacts struct {
	Declaration     OccurrenceID
	Identity        manifest.NamedResource
	Authored        manifest.NamedResource
	Object          manifest.BaseManifest
	Catalog         manifest.BaseManifest
	Source          manifest.NamedResource
	Path            string
	HasPath         bool
	Namespaced      bool
	Uncertain       bool
	RequiresBuild   bool
	Retained        bool
	Structural      *manifest.Kustomization
	NeedsCandidates bool
	Labels          map[string]string
	Labeled         bool
	Traversed       bool
	Mounted         *store.SourceArtifact
	Witness         any
	Located         bool
	CatalogKnown    bool
	Refs            []manifest.Reference
	Reverse         []manifest.NamedResource
	TemplateInputs  []manifest.TemplateInput
	TemplateObjects []manifest.BaseManifest
	Roots           []string
	Targets         []manifest.ProducerTarget
	AuthoredTargets []manifest.ProducerTarget
	Policy          *manifest.EmissionPolicy
}

// SourcePath resolves references in the occurrence's effective namespace.
//
//nolint:gocritic // The value receiver preserves the occurrence identity snapshot across namespace projection.
func (o OccurrenceFacts) SourcePath() (manifest.NamedResource, string, bool) {
	if o.Witness != nil {
		return manifest.Resolve(o.Source, "", o.Identity.Namespace), o.Path, o.HasPath
	}
	object := o.Object
	if object == nil {
		object = o.Catalog
	}
	if object == nil {
		return manifest.NamedResource{}, "", false
	}
	if o.Identity.Namespace != o.Authored.Namespace {
		object = object.WithNamespace(o.Identity.Namespace)
	}
	return object.SourcePath()
}

// NativeRootFacts borrows one physical package's graph membership and construction state.
type NativeRootFacts struct {
	Path       string
	Identities []manifest.NamedResource
	Owners     []manifest.NamedResource
	Claims     []manifest.KSClaim
	Entry      bool
	Built      bool
	Selected   bool
	Excluded   bool
	Component  bool
}

// RootScopeFacts borrows physical scope relationships without membership verdicts.
type RootScopeFacts struct {
	Path              string
	Eligibility       RootEligibility
	Owners            []RootOwnerFacts
	Peers             [][]manifest.NamedResource
	Records           map[manifest.NamedResource]manifest.DiscoveryRecord
	Claims            map[string][]manifest.NamedResource
	Claim             string
	ApplicationSource manifest.NamedResource
	ApplicationPath   string
	Loaded            bool
	Bootstrap         bool
	Initial           bool
	Terminal          bool
	NamespaceEligible bool
	NativeOperand     bool
	NativeEntry       bool
	EntryKustomize    bool
	Application       bool
	LocalApplication  bool
	SameEntryOrigin   bool
	Prepared          bool
	Candidate         bool
	Compact           bool
	PackageKnown      bool
	PackagePaths      []string
	PackageOwners     [][]int
	PackageIndex      int
	PackageRoot       bool
	PackageEligible   bool
	PackageRunnable   bool
	NativePackage     bool
}

// RootOwnerFacts borrows a scope owner's indexed path.
type RootOwnerFacts struct {
	Identity manifest.NamedResource
	Path     string
	Indexed  bool
	Initial  bool
}

// RootSourceSupport captures authority from one source generation.
type RootSourceSupport struct {
	Source    manifest.NamedResource
	Allowed   bool
	Available bool
	Path      string
	Origin    string
}

// RootCoveringOwner borrows producer ancestry and captured output authority.
type RootCoveringOwner struct {
	Identity     manifest.NamedResource
	Object       manifest.BaseManifest
	Productions  []manifest.Production
	Source       manifest.NamedResource
	Path         string
	Root         string
	Applications map[manifest.NamedResource]manifest.DiscoveryRecord
	Replaced     bool
	Output       *store.Emission
	Input        *store.SourceArtifact
}

// RootRecord retains only the coordinates used for root classification and support.
type RootRecord struct {
	Builder      string
	Authored     manifest.NamedResource
	Path         string
	Parent       manifest.NamedResource
	BuilderOwner manifest.NamedResource
}

// RootRecordFrom excludes declaration payloads from retained root evidence.
func RootRecordFrom(record *manifest.DiscoveryRecord) RootRecord {
	return RootRecord{
		Authored: record.Authored, Path: record.Path, Parent: record.Parent,
		Builder: record.Builder, BuilderOwner: record.BuilderOwner,
	}
}

// RootEvidence combines immutable discovery facts with captured current support.
type RootEvidence struct {
	Native            *NativeRootFacts
	Identity          manifest.NamedResource
	Record            RootRecord
	StaticRecord      *RootRecord
	Occurrence        OccurrenceFacts
	Scope             RootScopeFacts
	InitialScope      *RootScopeFacts
	InitialOccurrence *OccurrenceFacts
	Productions       []manifest.Production
	LiveProductions   []manifest.Production
	Declaration       manifest.BaseManifest
	Sources           [2]RootSourceSupport
	CoveringOwners    []RootCoveringOwner
	Supplied          bool
	Incoming          bool
	Needed            bool
	StaticOwner       bool
	DeclarationSource string
	Captured          bool
	Stale             bool
	PreparedRoot      string
	BuiltChild        store.Child
	BuiltCurrent      bool
}
