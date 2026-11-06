package store

import (
	"slices"

	"github.com/home-operations/flate/pkg/manifest"
)

// ListProductionCandidates returns visible objects with indexed, prospective, or published ownership.
// A prospective key without a namespace includes every namespace.
func (s *Store) ListProductionCandidates(
	group, kind string,
	index *manifest.DiscoveryIndex,
	prospective map[manifest.NamedResource]bool,
) []manifest.BaseManifest {
	if index == nil {
		index = s.index.Load()
	}
	sh := s.shardForKind(kind)
	sh.mu.RLock()
	out := sh.productionCandidatesLocked(group, kind, index, prospective)
	sh.mu.RUnlock()
	slices.SortFunc(out, func(a, b manifest.BaseManifest) int { return a.Named().Compare(b.Named()) })
	return out
}

func (sh *shard) productionCandidatesLocked(
	group, kind string,
	index *manifest.DiscoveryIndex,
	prospective map[manifest.NamedResource]bool,
) []manifest.BaseManifest {
	if index == nil && len(sh.members) == 0 && len(prospective) == 0 {
		return nil
	}
	var out []manifest.BaseManifest
	for name := range sh.byName[kindID{group, kind}].names {
		id := manifest.NamedResource{Group: group, Kind: kind, Namespace: name.namespace, Name: name.name}
		bare := id
		bare.Namespace = ""
		indexed := prospective[id] || prospective[bare]
		if !indexed && index != nil {
			indexed = slices.ContainsFunc(index.Records[id].Productions, func(p manifest.Production) bool {
				return p.Owner != (manifest.NamedResource{}) && p.Owner != id
			})
			if !indexed && id.Namespace != "" {
				indexed = slices.ContainsFunc(index.Records[bare].Productions, func(p manifest.Production) bool {
					return p.Owner != (manifest.NamedResource{}) && p.Owner != id
				})
			}
		}
		if !indexed && !slices.ContainsFunc(sh.members[id], func(m membership) bool {
			return !m.declaration && m.slot.id != id && m.slot.visible.Load()
		}) {
			continue
		}
		if obj := sh.objectLocked(id); obj != nil {
			out = append(out, obj)
		}
	}
	return out
}

// ListProductionCandidates captures visible objects with indexed, prospective, or published ownership.
// A prospective key without a namespace includes every namespace.
func (r *ReadSet) ListProductionCandidates(
	group, kind string,
	index *manifest.DiscoveryIndex,
	prospective map[manifest.NamedResource]bool,
) []manifest.BaseManifest {
	if index == nil {
		index = r.store.index.Load()
	}
	key := manifest.NamedResource{Group: group, Kind: kind}
	input := r.input(key)
	sh := r.store.shardFor(key)
	sh.mu.RLock()
	objects := sh.productionCandidatesLocked(group, kind, index, prospective)
	generation := sh.byName[kindID{group, kind}].generation
	for _, object := range objects {
		id := object.Named()
		for _, m := range sh.members[id] {
			if !m.declaration && m.slot.id != id && m.slot.id != r.id && m.slot.visible.Load() {
				r.sample(id, m.slot, m.slot.current.Load(), true)
			}
		}
	}
	sh.mu.RUnlock()
	r.observe(input, bucketFacet, generation)
	r.activateProducers()
	slices.SortFunc(objects, func(a, b manifest.BaseManifest) int { return a.Named().Compare(b.Named()) })
	missing := len(objects)
	for captured := r.inputs; captured != nil; captured = captured.next {
		if _, found := slices.BinarySearchFunc(
			objects,
			captured.Ref.NamedResource,
			func(obj manifest.BaseManifest, id manifest.NamedResource) int {
				return obj.Named().Compare(id)
			},
		); found {
			missing--
		}
	}
	r.reserve(max(0, missing))
	out := objects[:0]
	for _, object := range objects {
		if obj := r.GetObject(object.Named()); obj != nil {
			out = append(out, obj)
		}
	}
	return out
}
