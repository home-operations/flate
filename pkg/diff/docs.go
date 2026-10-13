package diff

import (
	"strings"

	"github.com/home-operations/flate/pkg/manifest"
)

func selectedDocs(objects []manifest.BaseManifest, manifests map[manifest.NamedResource][]map[string]any) []Doc {
	out := make([]Doc, 0, len(objects))
	for _, obj := range objects {
		parent := objectParent(obj)
		for _, doc := range manifests[obj.Named()] {
			out = append(out, Doc{Manifest: doc, Parent: parent})
		}
	}
	return out
}

func objectParent(obj manifest.BaseManifest) Parent {
	id := obj.Named()
	parent := Parent{Kind: id.Kind, Namespace: id.Namespace, Name: id.Name}
	if ks, ok := obj.(*manifest.Kustomization); ok {
		parent.Path = strings.TrimPrefix(ks.Path, "./")
	}
	return parent
}
