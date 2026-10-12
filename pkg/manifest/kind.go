package manifest

import (
	"iter"
	"strings"

	"sigs.k8s.io/kustomize/kyaml/openapi"
	"sigs.k8s.io/kustomize/kyaml/yaml"
)

// KindInfo contains group-aware scope, admission and reconciliation facts.
type KindInfo struct {
	Group      string
	Kind       string
	Namespaced bool
	Tracked    bool
	Runnable   bool
	Status     bool
}

type kindEntry struct {
	KindInfo
	decode             func(map[string]any, ParseDocOptions) (BaseManifest, error)
	skipNamespaceStamp bool
}

var kindTable = [...]kindEntry{
	{KindInfo: KindInfo{FluxKustomizeDomain, KindKustomization, true, true, true, true}, decode: func(d map[string]any, _ ParseDocOptions) (BaseManifest, error) { return parseKustomization(d) }},
	{KindInfo: KindInfo{HelmReleaseDomain, KindHelmRelease, true, true, true, true}, decode: func(d map[string]any, _ ParseDocOptions) (BaseManifest, error) { return parseHelmRelease(d) }},
	{KindInfo: KindInfo{SourceDomain, KindHelmRepository, true, true, true, true}, decode: func(d map[string]any, _ ParseDocOptions) (BaseManifest, error) { return parseHelmRepository(d) }},
	{KindInfo: KindInfo{SourceDomain, KindHelmChart, true, true, true, true}, decode: func(d map[string]any, _ ParseDocOptions) (BaseManifest, error) { return parseHelmChartSource(d) }},
	{KindInfo: KindInfo{SourceDomain, KindGitRepository, true, true, true, true}, decode: func(d map[string]any, _ ParseDocOptions) (BaseManifest, error) { return parseGitRepository(d) }},
	{KindInfo: KindInfo{SourceDomain, KindOCIRepository, true, true, true, true}, decode: func(d map[string]any, _ ParseDocOptions) (BaseManifest, error) { return ParseOCIRepository(d) }},
	{KindInfo: KindInfo{SourceDomain, KindExternalArtifact, true, true, true, true}, decode: func(d map[string]any, _ ParseDocOptions) (BaseManifest, error) { return parseExternalArtifact(d) }},
	{KindInfo: KindInfo{SourceDomain, KindBucket, true, true, true, true}, decode: func(d map[string]any, _ ParseDocOptions) (BaseManifest, error) { return parseBucket(d) }},
	{KindInfo: KindInfo{FluxOperatorDomain, KindResourceSet, true, true, true, false}, decode: func(d map[string]any, _ ParseDocOptions) (BaseManifest, error) { return parseResourceSet(d) }},
	{KindInfo: KindInfo{FluxOperatorDomain, KindResourceSetInputProvider, true, true, false, false}, decode: func(d map[string]any, _ ParseDocOptions) (BaseManifest, error) {
		return parseResourceSetInputProvider(d)
	}},
	{KindInfo: KindInfo{"", KindConfigMap, true, true, false, false}, decode: func(d map[string]any, o ParseDocOptions) (BaseManifest, error) {
		return parseConfigMap(d)
	}},
	{KindInfo: KindInfo{"", KindSecret, true, true, false, false}, decode: func(d map[string]any, o ParseDocOptions) (BaseManifest, error) { return parseSecret(d) }},
	scopeEntry("v1", "Namespace", true),
	scopeEntry("v1", "Node", true),
	scopeEntry("v1", "PersistentVolume", true),
	scopeEntry("v1", "ComponentStatus", false),
	scopeEntry("storage.k8s.io/v1", "StorageClass", true),
	scopeEntry("storage.k8s.io/v1", "CSIDriver", false),
	scopeEntry("storage.k8s.io/v1", "CSINode", false),
	scopeEntry("storage.k8s.io/v1", "VolumeAttachment", false),
	scopeEntry("scheduling.k8s.io/v1", "PriorityClass", true),
	scopeEntry("networking.k8s.io/v1", "IngressClass", true),
	scopeEntry("node.k8s.io/v1", "RuntimeClass", false),
	scopeEntry("rbac.authorization.k8s.io/v1", "ClusterRole", true),
	scopeEntry("rbac.authorization.k8s.io/v1", "ClusterRoleBinding", true),
	scopeEntry("apiextensions.k8s.io/v1", KindCustomResourceDefinition, true),
	scopeEntry("admissionregistration.k8s.io/v1", "MutatingWebhookConfiguration", true),
	scopeEntry("admissionregistration.k8s.io/v1", "ValidatingWebhookConfiguration", true),
	scopeEntry("admissionregistration.k8s.io/v1", "ValidatingAdmissionPolicy", false),
	scopeEntry("admissionregistration.k8s.io/v1", "ValidatingAdmissionPolicyBinding", false),
	scopeEntry("apiregistration.k8s.io/v1", "APIService", true),
	scopeEntry("certificates.k8s.io/v1", "CertificateSigningRequest", false),
	scopeEntry("flowcontrol.apiserver.k8s.io/v1beta1", "FlowSchema", false),
	scopeEntry("flowcontrol.apiserver.k8s.io/v1beta1", "PriorityLevelConfiguration", false),
	{Group: "cert-manager.io", Kind: "ClusterIssuer", skipNamespaceStamp: true},
	{Group: "external-secrets.io", Kind: "ClusterSecretStore", skipNamespaceStamp: true},
	{Group: "kyverno.io", Kind: "ClusterPolicy", skipNamespaceStamp: true},
}

var kindIndex, groupKindIndex = func() (map[string]int, map[[2]string]int) {
	kinds := make(map[string]int, len(kindTable))
	groups := make(map[[2]string]int, len(kindTable))
	for i := range kindTable {
		entry := &kindTable[i]
		kinds[entry.Kind] = i
		groups[[2]string{entry.Group, entry.Kind}] = i
	}
	return kinds, groups
}()

// Scope is resolved once; render-path lookups never initialize global OpenAPI state.
func scopeEntry(apiVersion, kind string, skipNamespaceStamp bool) kindEntry {
	return kindEntry{
		Group: apiGroup(apiVersion), Kind: kind,
		Namespaced:         !openapi.IsCertainlyClusterScoped(yaml.TypeMeta{APIVersion: apiVersion, Kind: kind}),
		skipNamespaceStamp: skipNamespaceStamp}
}

// Kinds yields immutable registry values in a stable order.
func Kinds() iter.Seq[KindInfo] {
	return func(yield func(KindInfo) bool) {
		for _, entry := range kindTable {
			if !yield(entry.KindInfo) {
				return
			}
		}
	}
}

// KindOf returns the actual object's facts. Unknown kinds are untracked and
// use namespaced fallback internally; their authored namespace is preserved.
func KindOf(obj BaseManifest) KindInfo {
	if obj == nil {
		return KindInfo{}
	}
	if raw, ok := obj.(*RawObject); ok {
		info, _ := kindInfo(apiGroup(raw.APIVersion), raw.Kind)
		info.Tracked, info.Runnable, info.Status = false, false, false
		return info
	}
	id := obj.Named()
	if i, ok := kindIndex[id.Kind]; ok {
		return kindTable[i].KindInfo
	}
	return KindInfo{Kind: id.Kind, Namespaced: true}
}

func apiGroup(apiVersion string) string {
	return apiVersion[:max(strings.IndexByte(apiVersion, '/'), 0)]
}

func kindInfo(group, kind string) (KindInfo, bool) {
	if i, ok := groupKindIndex[[2]string{group, kind}]; ok {
		return kindTable[i].KindInfo, true
	}
	return KindInfo{Group: group, Kind: kind, Namespaced: true}, false
}
