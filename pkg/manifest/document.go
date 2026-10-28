package manifest

import "maps"

// RenderDocument projects an ExternalArtifact declaration and its evaluated metadata.
func (e *ExternalArtifact) RenderDocument() (map[string]any, error) {
	metadata := map[string]any{}
	if e.Name != "" {
		metadata["name"] = e.Name
	}
	if e.Namespace != "" {
		metadata["namespace"] = e.Namespace
	}
	for key, values := range map[string]map[string]string{"labels": e.Labels, "annotations": e.Annotations} {
		if len(values) != 0 {
			fields := make(map[string]any, len(values))
			for key, value := range values {
				fields[key] = value
			}
			metadata[key] = fields
		}
	}
	spec := map[string]any{}
	if ref := e.SourceRef; ref != nil {
		source := map[string]any{"kind": ref.Kind, "name": ref.Name}
		if ref.APIVersion != "" {
			source["apiVersion"] = ref.APIVersion
		}
		if ref.Namespace != "" {
			source["namespace"] = ref.Namespace
		}
		spec["sourceRef"] = source
	}
	doc := map[string]any{"apiVersion": sourceAPIVersion, "kind": KindExternalArtifact, "metadata": metadata, "spec": spec}
	return doc, nil
}

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
