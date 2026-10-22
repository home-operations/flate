package orchestrator

import (
	"testing"

	"github.com/home-operations/flate/pkg/controllers/base"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

func publishDocuments(c *base.Controller, id manifest.NamedResource, docs []map[string]any, opts manifest.ParseDocOptions) ([]store.Child, []map[string]any) {
	prior, hadStatus := c.Store.GetStatus(id)
	children, retained := parseChildrenForTest(id, docs, opts)
	_, next := c.Store.GetEmission(id)
	defer next.Finish()
	next.Children, next.Manifests, next.Complete = children, retained, true
	c.Emit(id, &next)
	if hadStatus && prior.Status == store.StatusPending {
		c.Store.UpdateStatus(id, prior)
	}
	return children, retained
}

func publishFixture(tb testing.TB, s *store.Store, id manifest.NamedResource, children []manifest.NamedResource) {
	tb.Helper()
	if s.GetObject(id) == nil {
		s.AddObject(&manifest.RawObject{APIVersion: id.Group + "/v1", Kind: id.Kind, Name: id.Name, Namespace: id.Namespace})
	}
	next := &store.Emission{}
	for _, target := range children {
		obj := s.GetObject(target)
		if obj == nil {
			version := "v1"
			if target.Group != "" {
				version = target.Group + "/v1"
			}
			obj = &manifest.RawObject{APIVersion: version, Kind: target.Kind, Name: target.Name, Namespace: target.Namespace}
		}
		next.Children = append(next.Children, store.Child{Obj: obj})
	}
	s.RecordEmitted(id, next)
}
