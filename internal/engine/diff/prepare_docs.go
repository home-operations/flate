package diff

import (
	"strings"

	"github.com/home-operations/flate/pkg/manifest"
)

func documents(snapshot Snapshot) []Doc {
	out := make([]Doc, 0, len(snapshot.Manifests))
	for i, batch := range snapshot.Manifests {
		parent := objectParent(batch.Parent)
		defaultProducer := manifest.DiagnosticFor(parent.NamedResource, batch.Parent)
		producers := snapshot.Producers[i]
		for j, doc := range batch.Documents {
			producer, ok := producers[j]
			if !ok {
				producer = defaultProducer
			}
			out = append(out, Doc{
				Manifest: doc, Parent: parent, Producer: producer,
			})
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
