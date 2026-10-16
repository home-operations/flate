package base_test

import (
	"testing"

	"github.com/home-operations/flate/pkg/controllers/base"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

func publishDocuments(tb testing.TB, c *base.Controller, id manifest.NamedResource, docs []map[string]any, opts manifest.ParseDocOptions) ([]store.Child, []map[string]any) {
	tb.Helper()
	if c.Store.GetObject(id) == nil {
		c.Store.AddObject(&manifest.Kustomization{Name: id.Name, Namespace: id.Namespace})
	}
	children, retained := base.ParseChildren(id, docs, opts)
	_, next := c.Store.GetEmission(id)
	defer next.Finish()
	next.Children, next.Manifests, next.Complete = children, retained, true
	c.Emit(id, &next)
	return children, retained
}
