package source_test

import (
	"github.com/home-operations/flate/internal/engine/source"
	"github.com/home-operations/flate/internal/engine/store"
	"github.com/home-operations/flate/pkg/manifest"
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
		if sec := f.secrets(ref.Namespace, ref.Name); sec.Data != nil {
			copied := *sec.Data.(*manifest.Secret)
			copied.Namespace, copied.Name = ref.Namespace, ref.Name
			s.AddDeclaration(store.Declaration{Obj: &copied, Warnings: sec.Warnings})
		}
	}

	reads := new(store.ReadSet)
	reads.Init(s, obj.Named(), nil)
	return reads
}
