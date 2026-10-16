package discovery

import (
	"os"

	"github.com/home-operations/flate/internal/testutil"
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
	return testutil.ProductionOwners(r)
}

func admitDiscoveryForTest(s *store.Store, root string) {
	s.SetArtifact(manifest.BootstrapSourceID, &store.SourceArtifact{Kind: manifest.KindGitRepository, LocalPath: root, LocalRoot: root, FS: os.DirFS(root)})
}
