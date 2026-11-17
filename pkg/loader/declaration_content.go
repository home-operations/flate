package loader

import (
	"maps"

	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

type declarationEvidence struct {
	document map[string]any
	content  store.DeclarationContent
}

// DeclarationContents MUST be confined to one serial admission preparation.
// Independent preparations never share mutable memoization state.
func (*Loader) DeclarationContents() func(map[string]any) store.DeclarationContent {
	var evidence map[manifest.NamedResource][]declarationEvidence
	return func(document map[string]any) store.DeclarationContent {
		if document == nil {
			return store.DeclarationContent{}
		}
		id := manifest.DocIdentity(document)
		for _, entry := range evidence[id] {
			if manifest.ValueEqual(document, entry.document) {
				return entry.content
			}
		}
		content := preparedContent(document)
		if evidence == nil {
			evidence = make(map[manifest.NamedResource][]declarationEvidence)
		}
		evidence[id] = append(evidence[id], declarationEvidence{document: document, content: content})
		return content
	}
}

func preparedContent(document map[string]any) store.DeclarationContent {
	document = manifest.WithoutOwnershipLabels(document)
	exact := manifest.Fingerprint(document)
	content := store.DeclarationContent{
		Exact:            exact,
		WithoutNamespace: exact,
		Namespace:        manifest.DocMetadata(document).Namespace,
	}
	metadata, _ := document["metadata"].(map[string]any)
	if _, namespaced := metadata["namespace"]; namespaced {
		document = maps.Clone(document)
		metadata = maps.Clone(metadata)
		delete(metadata, "namespace")
		document["metadata"] = metadata
		content.WithoutNamespace = manifest.Fingerprint(document)
	}
	return content
}
