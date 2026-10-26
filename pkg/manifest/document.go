package manifest

import "maps"

type documentRenderer interface {
	RenderDocument() (map[string]any, error)
}

// ObjectDocument projects captured identity and desired fields with owned top-level
// and metadata maps; nested payloads MUST remain immutable.
func ObjectDocument(id NamedResource, obj BaseManifest) (map[string]any, error) {
	var doc map[string]any
	if r, ok := obj.(documentRenderer); ok {
		rendered, err := r.RenderDocument()
		if err != nil {
			return nil, err
		}
		doc = maps.Clone(rendered)
		doc["metadata"] = maps.Clone(rendered["metadata"].(map[string]any))
	} else {
		doc = map[string]any{"metadata": map[string]any{}}
	}
	meta := doc["metadata"].(map[string]any)
	meta["name"], meta["namespace"] = id.Name, id.Namespace
	doc["kind"], doc["apiVersion"] = id.Kind, id.KindInfo().APIVersion()
	return doc, nil
}
