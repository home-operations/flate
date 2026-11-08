package helmchart

import (
	"github.com/home-operations/flate/internal/source"
	"github.com/home-operations/flate/internal/store"
	"github.com/home-operations/flate/pkg/manifest"
)

type testInputs struct {
	secrets source.SecretGetter
	repos   func(string, string) *manifest.HelmRepository
}

func (f testInputs) readSet(obj manifest.BaseManifest) *store.ReadSet {
	s := store.New()
	seen := make(map[manifest.NamedResource]bool)
	f.admit(s, seen, obj)
	reads := new(store.ReadSet)
	reads.Init(s, obj.Named(), nil)
	return reads
}

func (f testInputs) admit(s *store.Store, seen map[manifest.NamedResource]bool, obj manifest.BaseManifest) {
	s.AddObject(obj)
	for ref := range manifest.References(obj, manifest.ReferenceRelations) {
		if seen[ref.NamedResource] {
			continue
		}
		seen[ref.NamedResource] = true
		if ref.Kind == manifest.KindSecret && f.secrets != nil {
			if sec := f.secrets(ref.Namespace, ref.Name); sec != nil {
				clone := *sec
				clone.Namespace, clone.Name = ref.Namespace, ref.Name
				s.AddObject(&clone)
			}
		}
		if ref.Kind == manifest.KindHelmRepository && f.repos != nil {
			if repo := f.repos(ref.Namespace, ref.Name); repo != nil {
				f.admit(s, seen, repo)
			}
		}
	}
}

func (f testInputs) lookup(obj manifest.BaseManifest) source.SecretGetter {
	if f.secrets == nil {
		return nil
	}
	return source.SecretLookup(f.readSet(obj))
}
