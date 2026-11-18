package manifest

import "maps"

func ensureMetadata(doc map[string]any) map[string]any {
	metadata, _ := doc[metadataKey].(map[string]any)
	if metadata == nil {
		metadata = make(map[string]any)
		doc[metadataKey] = metadata
	}
	return metadata
}

func mergeStringMap(md map[string]any, key string, in map[string]string) {
	if len(in) == 0 {
		return
	}
	out, _ := md[key].(map[string]any)
	if out == nil {
		out = make(map[string]any, len(in))
	}
	for k, v := range in {
		out[k] = v
	}
	md[key] = out
}

func defaultNamespace(
	obj BaseManifest,
	doc map[string]any,
	namespace string,
	changed bool,
) (BaseManifest, map[string]any, bool) {
	if obj == nil || namespace == "" || obj.Named().Namespace != "" ||
		!(KindInfo{Kind: DocKind(doc), apiVersion: DocAPIVersion(doc)}).Namespaced() {
		return obj, doc, changed
	}
	obj = obj.WithNamespace(namespace)
	if !changed {
		doc = maps.Clone(doc)
	}
	//nolint:errcheck // The parse path supplies metadata as map[string]any before namespace projection.
	metadata := maps.Clone(doc[metadataKey].(map[string]any))
	metadata[namespaceKey] = namespace
	doc[metadataKey] = metadata
	return obj, doc, true
}
