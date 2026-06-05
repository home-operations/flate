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
	_, ok := clusterScopedKinds[DocKind(doc)]
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
