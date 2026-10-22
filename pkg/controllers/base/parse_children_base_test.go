package base

import (
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

func parseChildrenForTest(id manifest.NamedResource, docs []map[string]any, opts manifest.ParseDocOptions) ([]store.Child, []map[string]any) {
	parent := &manifest.RawObject{APIVersion: id.Group + "/v1", Kind: id.Kind, Name: id.Name, Namespace: id.Namespace}
	return (&Controller{}).ParseChildren(parent, docs, opts)
}
