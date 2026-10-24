package change

import (
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/loader"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
	"slices"
)

type filterFixture struct {
	files    map[manifest.NamedResource]string
	root     string
	objects  objects
	refs     map[manifest.NamedResource][]manifest.NamedResource
	excluded map[manifest.NamedResource]struct{}
	pinned   map[manifest.NamedResource]struct{}
	readers  func(string) []manifest.FileReader
	changed  *Set
}

func (f filterFixture) index() *manifest.DiscoveryIndex {
	st := store.New()
	if f.objects != nil {
		for _, obj := range f.objects.Objects() {
			if _, ok := obj.(*manifest.Kustomization); !ok {
				continue
			}
			if _, excluded := f.excluded[obj.Named()]; !excluded {
				st.AddObject(obj)
			}
		}
	}
	index := &manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{}}
	index.Claims = map[string][]manifest.NamedResource{}
	index.Readers = map[string][]manifest.FileReader{}
	claims := loader.Claims(st, f.root)
	for _, c := range claims {
		index.Claims[c.Prefix] = append(index.Claims[c.Prefix], c.ID)
	}
	for id, file := range f.files {
		_, pinned := f.pinned[id]
		record := manifest.DiscoveryRecord{File: file, Refs: f.refs[id], Pinned: pinned}
		if id.KindInfo().Structural {
			own := map[string]struct{}{}
			for _, c := range claims {
				if c.ID == id {
					own[c.Prefix] = struct{}{}
				}
			}
			record.Parent, _ = manifest.StrictParent(claims, file, id, own)
		}
		index.Records[id] = record
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
		{
			owners, _ := index.OwnersOfFile(r.File)
			r.Productions = testutil.ProductionEvidence(owners)
			for j := range r.Productions {
				r.Productions[j].Authored = id
				r.Productions[j].Path = r.File
				r.Productions[j].File = r.File
			}
			index.Records[id] = r
		}
	}
	for id, r := range index.Records {
		for _, p := range r.Productions {
			if f.objects == nil {
				continue
			}
			ks, ok := f.objects.GetObject(p.Owner).(*manifest.Kustomization)
			if !ok {
				continue
			}
			effective := manifest.Resolve(id, ks.TargetNamespace, ks.Namespace)
			if effective != id {
				projected := index.Records[effective]
				projected.File = r.File
				projected.Productions = append(slices.Clone(projected.Productions), p)
				index.Records[effective] = projected
			}
		}
	}
	return index
}
