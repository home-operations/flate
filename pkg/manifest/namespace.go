package manifest

// IsClusterScoped reports group-aware scope for a recognized document.
func IsClusterScoped(doc map[string]any) bool {
	if DocAPIVersion(doc) == "" {
		return IsClusterScopedKind(DocKind(doc))
	}
	info, known := kindInfo(apiGroup(DocAPIVersion(doc)), DocKind(doc))
	return known && !info.Namespaced
}

// IsClusterScopedKind reports scope for known kind names without group context.
// Document consumers MUST use IsClusterScoped to distinguish foreign groups.
func IsClusterScopedKind(kind string) bool {
	if i, ok := kindIndex[kind]; ok {
		return !kindTable[i].Namespaced
	}
	return false
}

// StampNamespace defaults omitted metadata.namespace to ns under the emission
// policy. No-op when ns is empty, the doc already names a namespace, or the kind
// is excluded. Explicit namespaces are preserved, including on cluster-scoped kinds.
func StampNamespace(doc map[string]any, ns string) {
	if ns == "" {
		return
	}
	// Read metadata.namespace without creating the map — a nil-map lookup
	// returns "" safely, so EnsureMetadata only runs when actually stamping.
	md, _ := doc["metadata"].(map[string]any)
	if cur, _ := md["namespace"].(string); cur != "" {
		return
	}
	// Emission defaults use a kind-only policy independent of group-aware scope facts.
	if i, ok := kindIndex[DocKind(doc)]; ok && kindTable[i].skipNamespaceStamp {
		return
	}
	EnsureMetadata(doc)["namespace"] = ns
}

// StampNamespaces applies StampNamespace to every doc in docs.
func StampNamespaces(docs []map[string]any, ns string) {
	if ns == "" {
		return
	}
	for _, doc := range docs {
		StampNamespace(doc, ns)
	}
}

// EnsureMetadata returns doc["metadata"] as a map[string]any, lazily
// creating one when absent (or when present as a non-map). Used by
// writers that mutate metadata.labels / metadata.annotations; readers
// should prefer a direct type-assert + nil-tolerant access so we
// don't allocate when nothing will be written.
func EnsureMetadata(doc map[string]any) map[string]any {
	return childMap(doc, "metadata")
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
