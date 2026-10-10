package loader

import (
	"path/filepath"
	"slices"

	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

func loaderFixture(s *store.Store, files map[manifest.NamedResource]string) *Loader {
	l := New(s)
	l.Index = &manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{}}
	l.documents = documentReader{}
	for id, file := range files {
		l.Index.Records[id] = manifest.DiscoveryRecord{File: file}
	}
	return l
}

func inheritForTest(s *store.Store, files map[manifest.NamedResource]string, refs map[manifest.NamedResource][]manifest.NamedResource, root string) {
	l := loaderFixture(s, files)
	for id, r := range l.Index.Records {
		r.Refs = refs[id]
		l.Index.Records[id] = r
	}
	l.applyNamespaceInheritance(root)
	clear(files)
	for id, r := range l.Index.Files() {
		files[id] = r.File
	}
}

func stampForTest(s *store.Store, files map[manifest.NamedResource]string, root string) {
	loaderFixture(s, files).stampTransformerTargetNamespaces(root)
}

func namespaceForTest(root, dir string) string {
	k, _ := documentReader{}.read(filepath.Join(root, dir))
	if k == nil {
		return ""
	}
	return k.Namespace
}

func rendererFixture(s *store.Store, root string, producers *manifest.ProducerIndex, wipe bool) *manifest.DiscoveryIndex {
	l := New(s)
	l.Index = &manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{}}
	l.documents = documentReader{}
	l.Options.WipeSecrets = wipe
	l.FinalizeIndex(root, producers)
	return l.Index
}

func renderers(index *manifest.DiscoveryIndex, id manifest.NamedResource) []manifest.NamedResource {
	r, _ := index.Lookup(id)
	return r.Renderers
}

func filesForTest(index *manifest.DiscoveryIndex) map[manifest.NamedResource]string {
	files := map[manifest.NamedResource]string{}
	for id, r := range index.Files() {
		files[id] = r.File
	}
	return files
}

func refsForTest(index *manifest.DiscoveryIndex) map[manifest.NamedResource][]manifest.NamedResource {
	refs := map[manifest.NamedResource][]manifest.NamedResource{}
	for id, r := range index.Records {
		if len(r.Refs) != 0 {
			refs[id] = r.Refs
		}
	}
	return refs
}

func ownersForTest(index *manifest.DiscoveryIndex, file string) []manifest.NamedResource {
	owners, _ := index.OwnersOfFile(file)
	return owners
}

func declarationOwnersForTest(index *manifest.DiscoveryIndex, id manifest.NamedResource) []manifest.NamedResource {
	r, _ := index.Lookup(id)
	return slices.Clone(r.DeclarationOwners)
}

func emissionParentForTest(index *manifest.DiscoveryIndex, file string) (manifest.NamedResource, bool) {
	for _, r := range index.Files() {
		if r.File == file && r.Parent != (manifest.NamedResource{}) {
			return r.Parent, true
		}
	}
	return manifest.NamedResource{}, false
}

func defaultNamespacesForTest(s *store.Store, files map[manifest.NamedResource]string) {
	l := loaderFixture(s, files)
	l.applyDefaultNamespaces()
	clear(files)
	for id, r := range l.Index.Files() {
		files[id] = r.File
	}
}
