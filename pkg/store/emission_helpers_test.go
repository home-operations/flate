package store

import (
	"testing"

	"github.com/home-operations/flate/pkg/manifest"
)

func recordForTest(tb testing.TB, s *Store, id manifest.NamedResource, children []manifest.NamedResource) {
	tb.Helper()
	if s.GetObject(id) == nil {
		s.AddObject(&manifest.Kustomization{Name: id.Name, Namespace: id.Namespace})
	}
	next := &Emission{}
	for _, child := range children {
		version := "v1"
		if child.Group != "" {
			version = child.Group + "/v1"
		}
		next.Children = append(next.Children, Child{Obj: &manifest.RawObject{APIVersion: version, Kind: child.Kind, Namespace: child.Namespace, Name: child.Name}})
	}
	s.RecordEmitted(id, next, nil)
}
