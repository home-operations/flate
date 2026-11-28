package selection

import (
	"iter"

	"github.com/home-operations/flate/internal/engine/store"
	"github.com/home-operations/flate/pkg/manifest"
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
	Parsed          manifest.BaseManifest
	Source          manifest.NamedResource
	Path            string
	HasPath         bool
	Application     bool
	Namespaced      bool
	Uncertain       bool
	RequiresBuild   bool
	OrderingKnown   bool
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
	ImplicitGate    *manifest.Reference
	Refs            []manifest.Reference
	Reverse         []manifest.NamedResource
	TemplateInputs  []manifest.TemplateInput
	TemplateObjects []manifest.BaseManifest
	Roots           []string
	Targets         []manifest.ProducerTarget
	AuthoredTargets []manifest.ProducerTarget
	Policy          *manifest.EmissionPolicy
}

// SourcePath reads the source and path resolved by the loader.
func (o *OccurrenceFacts) SourcePath() (manifest.NamedResource, string, bool) {
	return o.Source, o.Path, o.HasPath
}

// EntryApplication requires application eligibility in addition to a source and path.
func (o *OccurrenceFacts) EntryApplication() (manifest.EntryApplication, bool) {
	return manifest.EntryApplication{Source: o.Source, Path: o.Path}, o.Application
}

// EffectiveObject preserves the borrowed object when its namespace already matches.
func (o *OccurrenceFacts) EffectiveObject() manifest.BaseManifest {
	object := o.Object
	if object == nil {
		object = o.Catalog
	}
	if object != nil && object.Named().Namespace != o.Identity.Namespace {
		object = object.WithNamespace(o.Identity.Namespace)
	}
	return object
}

// PendingPackageProductions traverses discovered package ownership for a declaration that needs a build.
func PendingPackageProductions(
	id manifest.NamedResource, record *manifest.DiscoveryRecord,
) iter.Seq2[manifest.NamedResource, *manifest.Production] {
	return func(yield func(manifest.NamedResource, *manifest.Production) bool) {
		for i := range record.Productions {
			production := &record.Productions[i]
			owner := production.Materializer(id)
			if owner != id && owner.KindInfo().Structural && !yield(owner, production) {
				return
			}
		}
	}
}

// NativeRootFacts borrows one physical package's graph membership and construction state.
type NativeRootFacts struct {
	Path       string
	Identities []manifest.NamedResource
	Owners     []manifest.NamedResource
	Index      *manifest.DiscoveryIndex
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
	OutsideInventory  bool
	Application       bool
	LocalApplication  bool
	InputApplication  bool
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

// OutsideInventorySource excludes checkout sources already handled by bootstrap.
func (s *RootScopeFacts) OutsideInventorySource(id manifest.NamedResource) bool {
	physical := s.OutsideInventory && s.Loaded && id.KindInfo().IsSource()
	return physical && !s.Bootstrap && s.Eligibility != RootInactive
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
	claims       *manifest.DiscoveryIndex
	claimRoot    string
	covering     bool
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
	EntryScope        *EntryScope
	BuiltChild        store.Child
	BuiltCurrent      bool
}

// VisitReferences keeps reconciliation gates outside the relation view.
func (o *OccurrenceFacts) VisitReferences(view manifest.ReferenceView, yield func(manifest.Reference) bool) bool {
	indexed := o.Witness != nil || o.CatalogKnown || o.Refs != nil || o.ImplicitGate != nil
	if !indexed {
		for ref := range manifest.References(o.EffectiveObject(), view) {
			if !yield(ref) {
				return false
			}
		}
		return true
	}
	visit := func(ref manifest.Reference) bool {
		if ref.NamespaceDefault {
			ref.Namespace = o.Identity.Namespace
		}
		return yield(ref)
	}
	if view == manifest.ReferenceAll && o.ImplicitGate != nil && !visit(*o.ImplicitGate) {
		return false
	}
	for i := range o.Refs {
		if !visit(o.Refs[i]) {
			return false
		}
	}
	return true
}
