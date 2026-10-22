package helmrelease

import (
	"testing"

	"github.com/home-operations/flate/pkg/controllers/base"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

func publishDocuments(c *Controller, id manifest.NamedResource, docs []map[string]any, opts manifest.ParseDocOptions) ([]store.Child, []map[string]any) {
	if c.Store.GetObject(id) == nil {
		c.Store.AddObject(&manifest.RawObject{APIVersion: id.Group + "/v1", Kind: id.Kind, Name: id.Name, Namespace: id.Namespace})
	}
	children, retained := base.ParseChildren(id, docs, opts)
	_, next := c.Store.GetEmission(id)
	defer next.Finish()
	next.Children, next.Manifests, next.Complete = children, retained, true
	c.Emit(id, &next)
	return children, retained
}

func parentIndexForTest(parents map[manifest.NamedResource]manifest.NamedResource) *manifest.DiscoveryIndex {
	index := &manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{}}
	for id, parent := range parents {
		index.Records[id] = manifest.DiscoveryRecord{Parent: parent, Productions: []manifest.Production{{Owner: parent}}}
	}
	return index
}

func publishRawFixture(t *testing.T, c *Controller, obj manifest.BaseManifest) {
	t.Helper()
	root := &manifest.Kustomization{Name: "producer-root", Namespace: obj.Named().Namespace}
	if c.Store.GetObject(root.Named()) == nil {
		c.Store.AddObject(root)
	}
	c.Emit(root.Named(), &store.Emission{Children: []store.Child{{Obj: obj}}, Complete: true})
}
