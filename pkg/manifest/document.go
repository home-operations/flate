package manifest

import (
	"fmt"
	"maps"

	kjson "k8s.io/apimachinery/pkg/util/json"
)

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
	if len(e.ExportedInputs) != 0 {
		inputs, err := e.renderExportedInputs()
		if err != nil {
			return nil, err
		}
		doc["status"] = map[string]any{"exportedInputs": inputs}
	}
	return doc, nil
}

func (e *ExternalArtifact) renderExportedInputs() (map[string]any, error) {
	inputs := make(map[string]any, len(e.ExportedInputs))
	for key, input := range e.ExportedInputs {
		var value any
		if input != nil {
			if err := kjson.Unmarshal(input.Raw, &value); err != nil {
				return nil, fmt.Errorf("decode exported inputs: %w", err)
			}
		}
		inputs[key] = value
	}
	return inputs, nil
}

type documentRenderer interface {
	RenderDocument() (map[string]any, error)
}

// ObjectDocument projects captured identity and desired fields with owned top-level
// and metadata maps; nested payloads MUST remain immutable.
func ObjectDocument(id NamedResource, obj BaseManifest) (map[string]any, error) {
	var doc, meta map[string]any
	if r, ok := obj.(documentRenderer); ok {
		rendered, err := r.RenderDocument()
		if err != nil {
			return nil, err
		}
		doc = maps.Clone(rendered)
		//nolint:errcheck // Sealed manifest RenderDocument implementations produce metadata as map[string]any.
		meta = maps.Clone(rendered[metadataKey].(map[string]any))
	} else {
		doc, meta = map[string]any{}, map[string]any{}
	}
	doc[metadataKey] = meta
	meta[nameKey], meta[namespaceKey] = id.Name, id.Namespace
	doc[kindKey], doc[apiVersionKey] = id.Kind, id.KindInfo().APIVersion()
	return doc, nil
}
