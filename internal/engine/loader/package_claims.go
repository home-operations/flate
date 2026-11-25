package loader

import "github.com/home-operations/flate/pkg/manifest"

// NewPackageClaimIndex uses caller coordinates for identity and already formatted prefixes for lookup.
func NewPackageClaimIndex(
	size int, coordinate func(int) string, prefixes func(add func(int, string)),
) (*manifest.DiscoveryIndex, []manifest.NamedResource, map[manifest.NamedResource]int) {
	index := &manifest.DiscoveryIndex{Claims: make(map[string][]manifest.NamedResource)}
	ids := make([]manifest.NamedResource, size)
	owners := make(map[manifest.NamedResource]int, size)
	for i := range ids {
		ids[i] = manifest.NewEntryKustomization(manifest.BootstrapSourceID, coordinate(i), manifest.NamedResource{}).
			Named()
	}
	prefixes(func(owner int, prefix string) {
		id := ids[owner]
		owners[id] = owner
		index.Claims[prefix] = append(index.Claims[prefix], id)
	})
	return index, ids, owners
}
