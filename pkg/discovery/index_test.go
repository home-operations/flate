package discovery

import (
	"github.com/home-operations/flate/pkg/loader"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

func filesForTest(index *manifest.DiscoveryIndex) map[manifest.NamedResource]string {
	files := map[manifest.NamedResource]string{}
	for id, r := range index.Files() {
		files[id] = r.File
	}
	return files
}
func parentsForTest(index *manifest.DiscoveryIndex) map[manifest.NamedResource]manifest.NamedResource {
	parents := map[manifest.NamedResource]manifest.NamedResource{}
	for id, r := range index.Records {
		if r.Parent != (manifest.NamedResource{}) {
			parents[id] = r.Parent
		}
	}
	return parents
}
func renderersForTest(index *manifest.DiscoveryIndex, id manifest.NamedResource) []manifest.NamedResource {
	r, _ := index.Lookup(id)
	return r.Renderers
}

func discoveryLoaderForTest(s *store.Store) *loader.Loader {
	l := loader.New(s)
	l.Index = &manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{}}
	return l
}
