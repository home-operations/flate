package orchestrator

import (
	"context"
	"testing"

	"github.com/home-operations/flate/internal/controllers/base"
	"github.com/home-operations/flate/internal/store"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/controllertest"
	"github.com/home-operations/flate/pkg/manifest"
)

func publishDocuments(
	c *base.Controller,
	id manifest.NamedResource,
	docs []map[string]any,
	opts manifest.ParseDocOptions,
) childMaps {
	prior, hadStatus := c.Store.GetStatus(id)
	parseChildrenForTestResult1 := parseChildrenForTest(id, docs, opts)
	children, retained := parseChildrenForTestResult1.Children, parseChildrenForTestResult1.Documents
	_, _, next := c.Store.GetEmission(id)
	defer next.Finish()
	next.Children, next.Manifests, next.Complete = children, testutil.DocumentOccurrences(retained), true
	controllertest.Emit(context.TODO(), c, id, &next)
	if hadStatus && prior.Status == store.StatusPending {
		c.Store.UpdateStatus(id, prior)
	}
	return childMaps{Children: children, Documents: retained}
}

func publishFixture(tb testing.TB, s *store.Store, id manifest.NamedResource, children []manifest.NamedResource) {
	tb.Helper()
	if s.GetObject(id) == nil {
		s.AddObject(
			&manifest.RawObject{APIVersion: id.Group + "/v1", Kind: id.Kind, Name: id.Name, Namespace: id.Namespace},
		)
	}
	next := &store.Emission{}
	for _, target := range children {
		obj := s.GetObject(target)
		if obj == nil {
			version := "v1"
			if target.Group != "" {
				version = target.Group + "/v1"
			}
			obj = &manifest.RawObject{
				APIVersion: version,
				Kind:       target.Kind,
				Name:       target.Name,
				Namespace:  target.Namespace,
			}
		}
		next.Children = append(next.Children, store.Child{Obj: obj})
	}
	s.RecordEmitted(id, next, nil)
}
