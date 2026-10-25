package manifest

import (
	"iter"
	"path"
	"slices"
	"strings"
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

// DiscoveryRecord contains static facts about one authored or projected identity.
// Path is empty for generated or projected objects that have no raw resource file.
// Slices are borrowed and MUST remain immutable after discovery publishes the index.
type DiscoveryRecord struct {
	Authored    NamedResource   // Proven raw identity; zero for generated records.
	Path        string          // Absolute raw resource path; empty for generated or projected records.
	File        string          // Diff coordinate; generator records use the declaration file.
	Builder     string          // Authoritative package coordinate; empty for file-seeded objects.
	Inputs      []string        // Additional declaration inputs; these do not establish render ownership.
	Refs        []NamedResource // Authored chart/source references under the discovery identity.
	Parent      NamedResource   // Strict structural parent, excluding peers with the same claimed path.
	Productions []Production    // Immutable source-qualified contributor evidence.
	Pinned      bool            // The Kustomization reads a committed local source artifact.
}

// DiscoveryIndex owns immutable discovery records, canonical claims and file readers.
// Construction writes these maps directly before publication; queries never mutate them.
type DiscoveryIndex struct {
	Records map[NamedResource]DiscoveryRecord // Authored and projected identities.
	Claims  map[string][]NamedResource        // Local-tree path claimants by slash-terminated prefix.
	Readers map[string][]NamedResource        // Graph and declared source-input readers by file or missing directory.
	Errors  map[string]error                  // File decode failures deferred until final source ownership is known.
}

// Lookup returns the borrowed record for id and reports whether it exists.
func (i *DiscoveryIndex) Lookup(id NamedResource) (DiscoveryRecord, bool) {
	if i == nil {
		return DiscoveryRecord{}, false
	}
	r, ok := i.Records[id]
	return r, ok
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

// OwnersOfFile returns deepest claimants plus file readers, and strict ancestors.
// Missing resource directories retain ownership of deleted descendant files.
func (i *DiscoveryIndex) OwnersOfFile(file string) (owners, ancestors []NamedResource) {
	if i == nil || file == "" {
		return nil, nil
	}
	for ids := range i.matchingPrefixes(file) {
		if owners == nil {
			owners = slices.Clone(ids)
		} else {
			ancestors = append(ancestors, ids...)
		}
	}
	for id := range i.ReadersOfFile(file) {
		owners = appendUnique(owners, id)
	}
	slices.SortFunc(owners, NamedResource.Compare)
	slices.SortFunc(ancestors, NamedResource.Compare)
	return owners, ancestors
}

func (i *DiscoveryIndex) matchingPrefixes(file string) iter.Seq[[]NamedResource] {
	return func(yield func([]NamedResource) bool) {
		if file == ".." || strings.HasPrefix(file, "../") {
			return
		}
		prefixed := file + "/"
		for end := len(prefixed); ; {
			if ids := i.Claims[prefixed[:end]]; len(ids) != 0 && !yield(ids) {
				return
			}
			if end == 0 {
				return
			}
			end = strings.LastIndexByte(prefixed[:end-1], '/') + 1
		}
	}
}

// ReadersOfFile yields readers of a file or any missing ancestor directory.
// Callers deduplicate identities when combining them with other ownership relations.
func (i *DiscoveryIndex) ReadersOfFile(file string) iter.Seq[NamedResource] {
	return func(yield func(NamedResource) bool) {
		if i == nil {
			return
		}
		for current := file; current != "" && current != "." && current != "/"; current = path.Dir(current) {
			for _, id := range i.Readers[current] {
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
