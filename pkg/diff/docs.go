package diff

import (
	"strings"

	"github.com/home-operations/flate/pkg/manifest"
)

func selectedDocs(manifests []manifest.DocumentBatch) []Doc {
	out := make([]Doc, 0, len(manifests))
	for _, batch := range manifests {
		parent := objectParent(batch.Parent)
		for _, doc := range batch.Documents {
			out = append(out, Doc{Manifest: doc, Parent: parent})
		}
	}
	return out
}

func objectParent(obj manifest.BaseManifest) Parent {
	if obj == nil {
		return Parent{}
	}
	parent := Parent{NamedResource: obj.Named()}
	if _, path, ok := obj.SourcePath(); ok {
		parent.Path = strings.TrimPrefix(path, "./")
	}
	return parent
}
