package bucket_test

import (
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source"
	"github.com/home-operations/flate/pkg/store"
)

type testInputs struct {
	secrets source.SecretGetter
}

func (f testInputs) readSet(obj manifest.BaseManifest) *store.ReadSet {
	s := store.New()
	seen := make(map[manifest.NamedResource]bool)
	s.AddObject(obj)
	for ref := range manifest.References(obj, manifest.ReferenceRelations) {
		if ref.Kind != manifest.KindSecret || f.secrets == nil || seen[ref.NamedResource] {
			continue
		}
		seen[ref.NamedResource] = true
		if sec := f.secrets(ref.Namespace, ref.Name); sec != nil {
			copied := *sec
			copied.Namespace, copied.Name = ref.Namespace, ref.Name
			s.AddObject(&copied)
		}
	}

	reads := new(store.ReadSet)
	reads.Init(s, obj.Named(), nil)
	return reads
}
