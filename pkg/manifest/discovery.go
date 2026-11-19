package manifest

import (
	"iter"
	"path"
	"path/filepath"
	"slices"
	"strings"
)

// Production identifies one source-qualified contribution to a queried target.
type Production struct {
	Owner       NamedResource `json:"Owner"`
	Declaration NamedResource `json:"Declaration"`
	Authored    NamedResource `json:"Authored"`
	Source      NamedResource `json:"Source"`
	Path        string        `json:"Path"`
	File        string        `json:"File"`
}

// ReferenceData retains one production's lookup evidence without object authority.
type ReferenceData struct {
	Production  Production `json:"Production"`
	Keys        KeySet     `json:"Keys"`
	OfflineData Data       `json:"OfflineData"`
	Warnings    []Warning  `json:"-"`
}

// Materializer returns the owner that can supply the target through this contribution.
func (p Production) Materializer(target NamedResource) NamedResource {
	if p.Declaration != (NamedResource{}) && p.File != "" && !filepath.IsLocal(p.File) {
		return NamedResource{}
	}
	if target.KindInfo().IsSource() && p.Declaration == (NamedResource{}) && (p.Path != "" || p.File != "") {
		return NamedResource{}
	}
	return p.Owner
}

// DiscoveryRecord contains static facts about one authored or projected identity.
// Path is empty for generated or projected objects that have no raw resource file.
// Slices are borrowed and MUST remain immutable after discovery publishes the index.
type DiscoveryRecord struct {
	Application   string                     `json:"Application"`   // Accepted applier document; valid only for its owning render receipt.
	Declaration   BaseManifest               `json:"-"`             // Immutable parsed input; absence does not imply publication.
	Authored      NamedResource              `json:"Authored"`      // Proven raw identity; zero for generated records.
	Path          string                     `json:"Path"`          // Absolute raw resource path; empty for generated or projected records.
	File          string                     `json:"File"`          // Diff coordinate; generator records use the declaration file.
	BuilderOwner  NamedResource              `json:"BuilderOwner"`  // Actual owner of the immutable prepared declaration.
	Builder       string                     `json:"Builder"`       // Authoritative package coordinate; empty for file-seeded objects.
	Inputs        []string                   `json:"Inputs"`        // Additional declaration inputs; these do not establish render ownership.
	Refs          []NamedResource            `json:"Refs"`          // Authored chart/source references under the discovery identity.
	Parent        NamedResource              `json:"Parent"`        // Strict structural parent, excluding peers with the same claimed path.
	Productions   []Production               `json:"Productions"`   // Immutable source-qualified contributor evidence.
	ReferenceData []ReferenceData            `json:"ReferenceData"` // Borrowed lookup candidates qualified by their production.
	Origin        DeclarationOrigin          `json:"-"`
	StaticOutput  bool                       `json:"StaticOutput"` // Discovery derives this producer's output without accepted history.
	Pinned        bool                       `json:"Pinned"`       // The Kustomization reads a committed local source artifact.
	ContentRoots  map[NamedResource][]string `json:"-"`            // Source-qualified render locations; no authored-file authority.
}

// Equal compares the record facts retained by accepted renders.
func (r DiscoveryRecord) Equal(other DiscoveryRecord) bool {
	if r.Application != other.Application || r.Authored != other.Authored || r.Path != other.Path ||
		r.File != other.File {
		return false
	}
	if r.Parent != other.Parent || r.Builder != other.Builder || r.Pinned != other.Pinned ||
		r.StaticOutput != other.StaticOutput {
		return false
	}
	if (r.Inputs == nil) != (other.Inputs == nil) || !slices.Equal(r.Inputs, other.Inputs) {
		return false
	}
	if (r.Refs == nil) != (other.Refs == nil) || !slices.Equal(r.Refs, other.Refs) {
		return false
	}
	return (r.Productions == nil) == (other.Productions == nil) && slices.Equal(r.Productions, other.Productions)
}

// FileReader identifies a consumer and the source supplying its file input.
type FileReader struct {
	Reader, Source, Declaration NamedResource
	Exact                       bool // Source-file reads exclude descendants and establish no render ownership.
	Materialize                 bool // The producer supplies another reader, not the changed content itself.
}

// InputVisibility records the visible physical inputs in one source load domain.
// Inputs and their sorted contribution paths MUST remain immutable after publication.
type InputVisibility struct {
	Source   NamedResource
	Root     string
	Ignore   string
	Complete bool
	Inputs   map[NamedResource][]string
}

// FileDeclarations is immutable evidence and MUST NOT grant discovery authority.
type FileDeclarations struct {
	Present  bool
	Complete bool
	Objects  map[NamedResource]string
}

// DiscoveryIndex owns immutable discovery records, canonical claims and file readers.
// Construction writes these maps directly before publication; queries never mutate them.
type DiscoveryIndex struct {
	ProducerKeys    map[NamedResource]NamedResource // Accepted actual producers mapped to comparison slots.
	Declarations    map[NamedResource]map[string]FileDeclarations
	Records         map[NamedResource]DiscoveryRecord // Authored and projected identities.
	Claims          map[string][]NamedResource        // Local-tree path claimants by slash-terminated prefix.
	Components      map[string][]string               // Native component references by physical package path.
	Readers         map[string][]FileReader           // Graph and declared source-input readers by file or missing directory.
	CompleteSources map[NamedResource]bool            // Sources whose declared-object census completed without missing input.
	Visibility      []InputVisibility                 // Completed and incomplete observations of physical input availability.
}

// Lookup returns the borrowed record for id and reports whether it exists.
func (i *DiscoveryIndex) Lookup(id NamedResource) (DiscoveryRecord, bool) {
	if i == nil {
		return DiscoveryRecord{}, false
	}
	r, ok := i.Records[id]
	return r, ok
}

// IsFileIndexed reports whether id has a raw resource file, including an inherited namespace.
func (i *DiscoveryIndex) IsFileIndexed(id NamedResource) bool {
	if r, ok := i.Lookup(id); ok && r.Path != "" {
		return true
	}
	if id.Namespace == "" {
		return false
	}
	// Discovery retains bare identities until parsing applies the parent namespace.
	id.Namespace = ""
	r, ok := i.Lookup(id)
	return ok && r.Path != ""
}

// Files yields records with a change-coordinate file, including generator declarations.
// Iteration is unordered; callers MUST sort any order-sensitive result.
func (i *DiscoveryIndex) Files() iter.Seq2[NamedResource, DiscoveryRecord] {
	return func(yield func(NamedResource, DiscoveryRecord) bool) {
		if i == nil {
			return
		}
		//nolint:gocritic // Map iteration supplies value snapshots without an additional lookup.
		for id, r := range i.Records {
			if r.File != "" && !yield(id, r) {
				return
			}
		}
	}
}

// FileOwners separates deepest file claimants from their strict ancestors.
type FileOwners struct {
	Owners    []NamedResource
	Ancestors []NamedResource
}

// ClaimOwners requires an artifact index coordinate, never an unqualified source-relative path.
// Eligibility MUST be applied before a caller chooses the deepest group.
func (i *DiscoveryIndex) ClaimOwners(
	file string,
	source, subject NamedResource,
	eligible func(NamedResource) bool,
) iter.Seq[[]NamedResource] {
	return func(yield func([]NamedResource) bool) {
		if i == nil {
			return
		}
		query := Production{Owner: subject, Source: source, Authored: i.Records[subject].Authored, File: file}
		if source == (NamedResource{}) && subject == (NamedResource{}) &&
			!i.visitClaimGroup(file+"/", &query, eligible, yield) {
			return
		}
		for end := len(file); ; {
			if end == 0 && source != BootstrapSourceID && file != "/" && !filepath.IsLocal(file) {
				return
			}
			if !i.visitClaimGroup(file[:end], &query, eligible, yield) || end == 0 {
				return
			}
			end = strings.LastIndexByte(file[:end-1], '/') + 1
		}
	}
}

func (i *DiscoveryIndex) visitClaimGroup(
	prefix string,
	subject *Production,
	eligible func(NamedResource) bool,
	yield func([]NamedResource) bool,
) bool {
	group := i.Claims[prefix]
	if len(group) == 0 {
		return true
	}
	owners := i.eligibleClaims(group, subject, eligible)
	return len(owners) == 0 || yield(owners)
}

func ownsClaim(id NamedResource, r *DiscoveryRecord, subject *Production) bool {
	own := id == subject.Owner
	if subject.Authored != (NamedResource{}) {
		own = own || id == subject.Authored || r.Authored == subject.Authored
	}
	if r.Authored != (NamedResource{}) {
		own = own || r.Authored == subject.Owner
	}
	return own
}

func (i *DiscoveryIndex) eligibleClaims(
	group []NamedResource,
	subject *Production,
	eligible func(NamedResource) bool,
) []NamedResource {
	var owners []NamedResource
	filtered := false
	for n, id := range group {
		record, _ := i.Lookup(id)
		allowed := claimAllowed(&record, subject) && (eligible == nil || eligible(id))
		own := ownsClaim(id, &record, subject)
		if allowed && own && subject.Owner.KindInfo().Structural {
			return nil
		}
		if !allowed || own {
			if !filtered {
				owners = slices.Clone(group[:n])
				filtered = true
			}
			continue
		}
		if filtered {
			owners = appendUnique(owners, id)
		}
	}
	if !filtered {
		owners = group
	}
	if !slices.IsSortedFunc(owners, NamedResource.Compare) {
		if !filtered {
			owners = slices.Clone(owners)
		}
		slices.SortFunc(owners, NamedResource.Compare)
	}
	return owners
}

// OwnersOfFile returns deepest claimants plus render graph readers, and strict ancestors.
// Missing resource directories retain ownership of deleted descendant files.
func (i *DiscoveryIndex) OwnersOfFile(file string) FileOwners {
	var owners, ancestors []NamedResource
	if i == nil || file == "" {
		return FileOwners{}
	}
	for ids := range i.ClaimOwners(file, NamedResource{}, NamedResource{}, nil) {
		if owners == nil {
			owners = slices.Clone(ids)
		} else {
			ancestors = append(ancestors, ids...)
		}
	}
	for reader := range i.ReadersOfFile(file) {
		if reader.Declaration == (NamedResource{}) && !reader.Exact && !reader.Materialize {
			owners = appendUnique(owners, reader.Reader)
		}
	}
	slices.SortFunc(owners, NamedResource.Compare)
	slices.SortFunc(ancestors, NamedResource.Compare)
	return FileOwners{Owners: owners, Ancestors: ancestors}
}

// ReadersOfFile yields source-qualified readers of a file or any missing ancestor directory.
// Callers deduplicate pairs when combining them with other ownership relations.
func (i *DiscoveryIndex) ReadersOfFile(file string) iter.Seq[FileReader] {
	return func(yield func(FileReader) bool) {
		if i == nil {
			return
		}
		for current := file; current != "" && current != "/"; current = path.Dir(current) {
			{
				idValues := i.Readers[current]
				for idIndex := range idValues {
					id := idValues[idIndex]
					if id.Exact && current != file {
						continue
					}
					if !yield(id) {
						return
					}
				}
			}
			if current == "." {
				break
			}
		}
	}
}

func appendUnique(dst []NamedResource, id NamedResource) []NamedResource {
	if !slices.Contains(dst, id) {
		return append(dst, id)
	}
	return dst
}

func claimAllowed(record *DiscoveryRecord, subject *Production) bool {
	if record.Pinned && subject.Source == BootstrapSourceID && filepath.IsLocal(subject.File) {
		return false
	}
	return subject.Owner == (NamedResource{}) || record.Declaration == nil ||
		record.Declaration.EmissionPolicy().MatchesNamespace(subject.Owner)
}
