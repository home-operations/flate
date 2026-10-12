package manifest

// clusterScopedKinds is the set of well-known cluster-scoped Kubernetes and
// common-ecosystem kinds. Namespace defaulting (StampNamespace) skips these so
// a Namespace / ClusterRole / CRD never gets a spurious metadata.namespace —
// matching kustomize-controller and helm-controller apply behavior. Kind-only
// keying mirrors DropKinds; an offline renderer has no RESTMapper, so this is a
// curated heuristic and unknown kinds default to namespaced.
var clusterScopedKinds = map[string]struct{}{
	"Namespace":                      {},
	"Node":                           {},
	"PersistentVolume":               {},
	"StorageClass":                   {},
	"PriorityClass":                  {},
	"IngressClass":                   {},
	"ClusterRole":                    {},
	"ClusterRoleBinding":             {},
	"CustomResourceDefinition":       {},
	"MutatingWebhookConfiguration":   {},
	"ValidatingWebhookConfiguration": {},
	"APIService":                     {},
	"ClusterIssuer":                  {}, // cert-manager.io
	"ClusterSecretStore":             {}, // external-secrets.io
	"ClusterPolicy":                  {}, // kyverno.io
}

// IsClusterScoped reports whether doc's kind is a well-known cluster-scoped
// kind that must not carry a metadata.namespace.
func IsClusterScoped(doc map[string]any) bool {
	return IsClusterScopedKind(DocKind(doc))
}

// IsClusterScopedKind reports whether kind is a known cluster-scoped kind.
// Unknown kinds are treated as namespaced because offline rendering has no RESTMapper.
func IsClusterScopedKind(kind string) bool {
	_, ok := clusterScopedKinds[kind]
	return ok
}

// StampNamespace defaults doc's metadata.namespace to ns when it's a namespaced
// kind that omits one — mirroring how kustomize-controller / helm-controller
// namespace resources on apply, so an offline render matches the in-cluster
// state. No-op when ns is empty, the doc already names a namespace, or the kind
// is cluster-scoped. An explicit namespace (even a differing one) is preserved,
// so this can never manufacture a diff.
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
	if IsClusterScoped(doc) {
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
