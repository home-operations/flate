package change

import (
	"github.com/home-operations/flate/pkg/loader"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

type filterFixture struct {
	files    map[manifest.NamedResource]string
	root     string
	objects  ObjectLister
	refs     map[manifest.NamedResource][]manifest.NamedResource
	excluded map[manifest.NamedResource]struct{}
	pinned   map[manifest.NamedResource]struct{}
	readers  func(string) []manifest.NamedResource
	changed  *Set
}

func (f filterFixture) index() *manifest.DiscoveryIndex {
	st := store.New()
	if f.objects != nil {
		for _, obj := range f.objects.ListObjects(manifest.KindKustomization) {
			if _, excluded := f.excluded[obj.Named()]; !excluded {
				st.AddObject(obj)
			}
		}
	}
	index := &manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{}}
	index.Claims = map[string][]manifest.NamedResource{}
	index.Readers = map[string][]manifest.NamedResource{}
	for _, c := range loader.Claims(st, f.root) {
		index.Claims[c.Prefix] = append(index.Claims[c.Prefix], c.ID)
	}
	for id, file := range f.files {
		_, pinned := f.pinned[id]
		index.Records[id] = manifest.DiscoveryRecord{File: file, Refs: f.refs[id], Pinned: pinned}
	}
	for id, refs := range f.refs {
		r := index.Records[id]
		r.Refs = refs
		index.Records[id] = r
	}
	for id := range f.pinned {
		r := index.Records[id]
		r.Pinned = true
		index.Records[id] = r
	}
	if f.readers != nil {
		for _, file := range f.files {
			index.Readers[file] = f.readers(file)
		}
		for _, file := range f.changed.Paths() {
			index.Readers[file] = f.readers(file)
		}
	}
	for id, r := range index.Records {
		if id.Kind == manifest.KindConfigMap || id.Kind == manifest.KindSecret {
			r.Owners, _ = index.OwnersOfFile(r.File)
			index.Records[id] = r
		}
	}
	return index
}
