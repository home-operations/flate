package kustomization

import (
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/controllers/base"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

type childMaps struct {
	Children  []store.Child
	Documents []map[string]any
}

func parseChildrenForTest(
	id manifest.NamedResource,
	docs []map[string]any,
	opts manifest.ParseDocOptions,
) childMaps {
	parent := &manifest.RawObject{APIVersion: id.Group + "/v1", Kind: id.Kind, Name: id.Name, Namespace: id.Namespace}
	result := (&base.Controller{}).ParseChildren(parent, testutil.DocumentOccurrences(docs), opts)
	if err := manifest.DocumentFailure(result.Failures); err != nil {
		panic(err)
	}
	return childMaps{Children: result.Children, Documents: testutil.DocumentMaps(result.Visible)}
}
