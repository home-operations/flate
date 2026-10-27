package manifest

import (
	"iter"
	"path"
	"path/filepath"
	"slices"
	"strings"

	"github.com/home-operations/flate/internal/claimpath"
)

// Production identifies one source-qualified contribution to a queried target.
type Production struct {
	Owner       NamedResource
	Declaration NamedResource
	Authored    NamedResource
	Source      NamedResource
	Path        string
	File        string
}

// ReferenceData retains one production's lookup evidence without object authority.
type ReferenceData struct {
	Production   Production
	DeclaredKeys []string
	OfflineData  Data
}

// Materializer returns the owner that can supply the target through this contribution.
func (p Production) Materializer(target NamedResource) NamedResource {
	if p.Declaration != (NamedResource{}) && p.File != "" && !filepath.IsLocal(p.File) {
		return NamedResource{}
	}
	if target.Group == SourceDomain && p.Declaration == (NamedResource{}) && (p.Path != "" || p.File != "") {
		return NamedResource{}
	}
	return p.Owner
}

// DiscoveryRecord contains static facts about one authored or projected identity.
// Path is empty for generated or projected objects that have no raw resource file.
// Slices are borrowed and MUST remain immutable after discovery publishes the index.
type DiscoveryRecord struct {
	Declaration   BaseManifest               `json:"-"` // Immutable parsed input; absence does not imply publication.
	Authored      NamedResource              // Proven raw identity; zero for generated records.
	Path          string                     // Absolute raw resource path; empty for generated or projected records.
	File          string                     // Diff coordinate; generator records use the declaration file.
	BuilderOwner  NamedResource              // Actual owner of the immutable prepared declaration.
	Builder       string                     // Authoritative package coordinate; empty for file-seeded objects.
	Inputs        []string                   // Additional declaration inputs; these do not establish render ownership.
	Refs          []NamedResource            // Authored chart/source references under the discovery identity.
	Parent        NamedResource              // Strict structural parent, excluding peers with the same claimed path.
	Productions   []Production               // Immutable source-qualified contributor evidence.
	ReferenceData []ReferenceData            // Borrowed lookup candidates qualified by their production.
	Pinned        bool                       // The Kustomization reads a committed local source artifact.
	ContentRoots  map[NamedResource][]string `json:"-"` // Source-qualified render locations; no authored-file authority.
}

// FileReader identifies a consumer and the source supplying its file input.
type FileReader struct {
	Reader, Source NamedResource
	Exact          bool // Source-file reads exclude descendants and establish no render ownership.
}

// DiscoveryIndex owns immutable discovery records, canonical claims and file readers.
// Construction writes these maps directly before publication; queries never mutate them.
type DiscoveryIndex struct {
	Records map[NamedResource]DiscoveryRecord // Authored and projected identities.
	Claims  map[string][]NamedResource        // Local-tree path claimants by slash-terminated prefix.
	Readers map[string][]FileReader           // Graph and declared source-input readers by file or missing directory.
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
		for id, r := range i.Records {
			if r.File != "" && !yield(id, r) {
				return
			}
		}
	}
}

// OwnersOfFile returns deepest claimants plus render graph readers, and strict ancestors.
// Missing resource directories retain ownership of deleted descendant files.
func (i *DiscoveryIndex) OwnersOfFile(file string) (owners, ancestors []NamedResource) {
	if i == nil || file == "" {
		return nil, nil
	}
	for end, ids := range claimpath.Groups(i.Claims, file+"/") {
		if end == 0 && (file == ".." || strings.HasPrefix(file, "../")) {
			break
		}
		if owners == nil {
			owners = slices.Clone(ids)
		} else {
			ancestors = append(ancestors, ids...)
		}
	}
	for reader := range i.ReadersOfFile(file) {
		if !reader.Exact {
			owners = appendUnique(owners, reader.Reader)
		}
	}
	slices.SortFunc(owners, NamedResource.Compare)
	slices.SortFunc(ancestors, NamedResource.Compare)
	return owners, ancestors
}

// ReadersOfFile yields source-qualified readers of a file or any missing ancestor directory.
// Callers deduplicate pairs when combining them with other ownership relations.
func (i *DiscoveryIndex) ReadersOfFile(file string) iter.Seq[FileReader] {
	return func(yield func(FileReader) bool) {
		if i == nil {
			return
		}
		for current := file; current != "" && current != "." && current != "/"; current = path.Dir(current) {
			for _, id := range i.Readers[current] {
				if id.Exact && current != file {
					continue
				}
				if !yield(id) {
					return
				}
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
