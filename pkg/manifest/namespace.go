package manifest

import "maps"

// EnsureMetadata returns doc["metadata"] as a map[string]any, lazily
// creating one when absent (or when present as a non-map). Used by
// writers that mutate metadata.labels / metadata.annotations; readers
// should prefer a direct type-assert + nil-tolerant access so we
// don't allocate when nothing will be written.
func EnsureMetadata(doc map[string]any) map[string]any {
	metadata, _ := doc["metadata"].(map[string]any)
	if metadata == nil {
		metadata = make(map[string]any)
		doc["metadata"] = metadata
	}
	return metadata
}

// MergeStringMap merges in into md[key] as a map[string]any. Creates
// the inner map when absent; existing entries with the same key are
// overwritten. No-op when in is empty.
//
// Used by the ResourceSet output + Helm post-render passes to merge
// CommonMetadata (labels / annotations) onto rendered docs without
// touching unrelated entries.
func MergeStringMap(md map[string]any, key string, in map[string]string) {
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

func defaultNamespace(obj BaseManifest, doc map[string]any, namespace string, changed bool) (BaseManifest, map[string]any, bool) {
	if obj == nil || namespace == "" || obj.Named().Namespace != "" || !(KindInfo{Kind: DocKind(doc), apiVersion: DocAPIVersion(doc)}).Namespaced() {
		return obj, doc, changed
	}
	obj = obj.WithNamespace(namespace)
	if !changed {
		doc = maps.Clone(doc)
	}
	metadata := maps.Clone(doc["metadata"].(map[string]any))
	metadata["namespace"] = namespace
	doc["metadata"] = metadata
	return obj, doc, true
}
