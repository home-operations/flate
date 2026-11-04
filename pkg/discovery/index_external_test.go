package discovery_test

import "github.com/home-operations/flate/pkg/manifest"

func filesForTest(index *manifest.DiscoveryIndex) map[manifest.NamedResource]string {
	files := map[manifest.NamedResource]string{}
	for id, r := range index.Files() { //nolint:gocritic // The iterator supplies independent value snapshots.
		files[id] = r.File
	}
	return files
}

func parentsForTest(index *manifest.DiscoveryIndex) map[manifest.NamedResource]manifest.NamedResource {
	parents := map[manifest.NamedResource]manifest.NamedResource{}
	for id, r := range index.Records { //nolint:gocritic // Map iteration preserves a value snapshot without an extra lookup.
		if r.Parent != (manifest.NamedResource{}) {
			parents[id] = r.Parent
		}
	}
	return parents
}

func ownersForTest(index *manifest.DiscoveryIndex, file string) []manifest.NamedResource {
	ownership := index.OwnersOfFile(file)
	owners := ownership.Owners
	return owners
}
