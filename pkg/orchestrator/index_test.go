package orchestrator

import (
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
)

func indexForTest(files map[manifest.NamedResource]string) *manifest.DiscoveryIndex {
	index := &manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{}}
	for id, file := range files {
		index.Records[id] = manifest.DiscoveryRecord{File: file}
	}
	return index
}
func filesForTest(index *manifest.DiscoveryIndex) map[manifest.NamedResource]string {
	files := map[manifest.NamedResource]string{}
	for id, r := range index.Files() {
		files[id] = r.File
	}
	return files
}
func parentsForTest(index *manifest.DiscoveryIndex) map[manifest.NamedResource]manifest.NamedResource {
	parents := map[manifest.NamedResource]manifest.NamedResource{}
	if index != nil {
		for id, r := range index.Records {
			if r.Parent != (manifest.NamedResource{}) {
				parents[id] = r.Parent
			}
		}
	}
	return parents
}
func setParentsForTest(o *Orchestrator, parents map[manifest.NamedResource]manifest.NamedResource) {
	if o.index == nil {
		o.index = indexForTest(nil)
	}
	index := o.index
	for id, parent := range parents {
		r := index.Records[id]
		r.Parent = parent
		index.Records[id] = r
	}
}
func renderersForTest(index *manifest.DiscoveryIndex, id manifest.NamedResource) []manifest.NamedResource {
	r, _ := index.Lookup(id)
	return testutil.ProductionOwners(r)
}
