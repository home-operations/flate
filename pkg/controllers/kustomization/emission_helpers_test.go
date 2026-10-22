package kustomization

import (
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

func publishDocuments(c *Controller, id manifest.NamedResource, docs []map[string]any, opts manifest.ParseDocOptions) ([]store.Child, []map[string]any) {
	prior, hadStatus := c.Store.GetStatus(id)
	if c.Store.GetObject(id) == nil {
		c.Store.AddObject(&manifest.RawObject{APIVersion: id.Group + "/v1", Kind: id.Kind, Name: id.Name, Namespace: id.Namespace})
	}
	children, retained := parseChildrenForTest(id, docs, opts)
	_, _, next := c.Store.GetEmission(id)
	defer next.Finish()
	next.Children, next.Manifests, next.Complete = children, retained, true
	c.Emit(id, &next, nil)
	if hadStatus && prior.Status == store.StatusPending {
		c.Store.UpdateStatus(id, prior)
	}
	return children, retained
}
