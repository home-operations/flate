package manifest

import "maps"

// RenderDocument projects an ExternalArtifact declaration and its evaluated metadata.
func (e *ExternalArtifact) RenderDocument() (map[string]any, error) {
	metadata := map[string]any{}
	if e.Name != "" {
		metadata[nameKey] = e.Name
	}
	if e.Namespace != "" {
		metadata[namespaceKey] = e.Namespace
	}
	for key, values := range map[string]map[string]string{labelsKey: e.Labels, annotationsKey: e.Annotations} {
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
		source := map[string]any{kindKey: ref.Kind, nameKey: ref.Name}
		if ref.APIVersion != "" {
			source[apiVersionKey] = ref.APIVersion
		}
		if ref.Namespace != "" {
			source[namespaceKey] = ref.Namespace
		}
		spec["sourceRef"] = source
	}
	doc := map[string]any{
		apiVersionKey: sourceAPIVersion,
		kindKey:       KindExternalArtifact,
		metadataKey:   metadata,
		specKey:       spec,
	}
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
		//nolint:errcheck // The owning path guarantees the concrete type and panics on a broken invariant.
		doc[metadataKey] = maps.Clone(rendered[metadataKey].(map[string]any))
	} else {
		doc = map[string]any{metadataKey: map[string]any{}}
	}
	//nolint:errcheck // The owning path guarantees the concrete type and panics on a broken invariant.
	meta := doc[metadataKey].(map[string]any)
	meta[nameKey], meta[namespaceKey] = id.Name, id.Namespace
	doc[kindKey], doc[apiVersionKey] = id.Kind, id.KindInfo().APIVersion()
	return doc, nil
}
