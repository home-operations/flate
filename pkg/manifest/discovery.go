package manifest

import (
	"iter"
	"path"
	"slices"
	"strings"
)

// DiscoveryRecord contains static facts about one authored or projected identity.
// Path is empty for generated or projected objects that have no raw resource file.
// Slices are borrowed and MUST remain immutable after discovery publishes the index.
type DiscoveryRecord struct {
	Path              string          // Absolute raw resource path; empty for generated or projected records.
	File              string          // Diff coordinate; generator records use the declaration file.
	Refs              []NamedResource // Authored chart/source references retained under their raw identity.
	Deferred          bool            // File data awaits runtime admission or pre-render orphan promotion.
	Parent            NamedResource
	Renderers         []NamedResource
	DeclarationOwners []NamedResource
	Owners            []NamedResource
	Excluded          bool
	Pinned            bool
}

// DiscoveryIndex owns immutable discovery records, canonical claims and graph readers.
// Construction writes these maps directly before publication; queries never mutate them.
type DiscoveryIndex struct {
	Records map[NamedResource]DiscoveryRecord
	Claims  map[string][]NamedResource
	Readers map[string][]NamedResource
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

// OwnersOfFile returns deepest claimants plus graph readers, and strict ancestors.
// Missing resource directories retain ownership of deleted descendant files.
func (i *DiscoveryIndex) OwnersOfFile(file string) (owners, ancestors []NamedResource) {
	if i == nil || file == "" {
		return nil, nil
	}
	if file != ".." && !strings.HasPrefix(file, "../") {
		prefixed := file + "/"
		first := true
		for end := len(prefixed); ; {
			if ids := i.Claims[prefixed[:end]]; len(ids) != 0 {
				if first {
					owners = slices.Clone(ids)
					first = false
				} else {
					ancestors = append(ancestors, ids...)
				}
			}
			if end == 0 {
				break
			}
			end = strings.LastIndexByte(prefixed[:end-1], '/') + 1
		}
	}
	for _, id := range i.Readers[file] {
		owners = appendUnique(owners, id)
	}
	for dir := path.Dir(file); dir != "." && dir != "/"; dir = path.Dir(dir) {
		for _, id := range i.Readers[dir] {
			owners = appendUnique(owners, id)
		}
	}
	slices.SortFunc(owners, NamedResource.Compare)
	slices.SortFunc(ancestors, NamedResource.Compare)
	return owners, ancestors
}

// DataOwners returns data renderers and raw file owners, retaining the asymmetric
// empty-namespace bridge. Two unequal nonempty namespaces never match.
func (i *DiscoveryIndex) DataOwners(id NamedResource) []NamedResource {
	r, ok := i.Lookup(id)
	if (!ok || len(r.Owners) == 0) && id.Namespace != "" {
		id.Namespace = ""
		r, _ = i.Lookup(id)
	}
	return r.Owners
}

func appendUnique(dst []NamedResource, id NamedResource) []NamedResource {
	if !slices.Contains(dst, id) {
		return append(dst, id)
	}
	return dst
}
