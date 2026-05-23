package manifest

import (
	"cmp"
	"encoding/json"
	"strings"
)

// decodeTyped re-marshals a parsed YAML document into a typed Flux CR
// via a JSON round-trip. The Flux API types use the same JSON tags
// k8s uses to serialize their CRDs, so this matches `kubectl apply`'s
// decoding fidelity without keeping the raw YAML bytes around.
//
// Per-document cost is ~one marshal + one unmarshal; documents are
// small (a single CR) and this only runs during the load phase, not on
// the render hot path.
func decodeTyped[T any](doc map[string]any, out *T) error {
	raw, err := json.Marshal(doc)
	if err != nil {
		return err
	}
	return json.Unmarshal(raw, out)
}

// DocKind returns the "kind" field of a manifest document, or "" if
// absent. Centralizes the doc["kind"].(string) cast.
func DocKind(doc map[string]any) string {
	k, _ := doc["kind"].(string)
	return k
}

// DocMetadata returns (name, namespace) from a manifest document's
// metadata block. Both are "" when metadata is absent or unset.
func DocMetadata(doc map[string]any) (name, namespace string) {
	md, _ := doc["metadata"].(map[string]any)
	if md == nil {
		return "", ""
	}
	name, _ = md["name"].(string)
	namespace, _ = md["namespace"].(string)
	return
}

// ParseDocOptions tunes ParseDoc behavior.
type ParseDocOptions struct {
	// WipeSecrets controls Secret cleartext replacement. Default true.
	WipeSecrets bool
}

// DefaultParseDocOptions returns the standard options — secrets wiped.
func DefaultParseDocOptions() ParseDocOptions {
	return ParseDocOptions{WipeSecrets: true}
}

// ParseDoc dispatches on kind + apiVersion to the appropriate concrete
// parser. Unknown kinds become a RawObject; kustomize.config.k8s.io
// build directives and bare data files are silently dropped.
func ParseDoc(doc map[string]any, opts ParseDocOptions) (BaseManifest, error) {
	kind, _ := doc["kind"].(string)
	apiVersion, _ := doc["apiVersion"].(string)
	// kustomize.config.k8s.io build directives (Kustomization,
	// Component) aren't k8s resources — they're build inputs we
	// already follow via spec.path discovery. Drop them silently.
	if strings.HasPrefix(apiVersion, KustomizeDomain) {
		return &RawObject{Kind: kind, APIVersion: apiVersion}, nil
	}
	// Documents without a kind are bare data files (helm values,
	// arbitrary YAML configs). Treat them as RawObjects so the
	// loader drops them without surfacing as parse errors.
	if kind == "" {
		return &RawObject{APIVersion: apiVersion}, nil
	}
	if apiVersion == "" {
		return nil, inputf("missing apiVersion for %s", kind)
	}

	switch {
	case kind == KindKustomization && strings.HasPrefix(apiVersion, FluxKustomizeDomain):
		return ParseKustomization(doc)
	case kind == KindHelmRelease:
		return ParseHelmRelease(doc)
	case kind == KindHelmRepository:
		return ParseHelmRepository(doc)
	case kind == KindHelmChart && strings.HasPrefix(apiVersion, SourceDomain):
		return ParseHelmChartSource(doc)
	case kind == KindGitRepository:
		return ParseGitRepository(doc)
	case kind == KindOCIRepository:
		return ParseOCIRepository(doc)
	case kind == KindExternalArtifact && strings.HasPrefix(apiVersion, SourceDomain):
		return ParseExternalArtifact(doc)
	case kind == KindBucket && strings.HasPrefix(apiVersion, SourceDomain):
		return ParseBucket(doc)
	case kind == KindResourceSet && strings.HasPrefix(apiVersion, FluxOperatorDomain):
		return ParseResourceSet(doc)
	case kind == KindResourceSetInputProvider && strings.HasPrefix(apiVersion, FluxOperatorDomain):
		return ParseResourceSetInputProvider(doc)
	case kind == KindConfigMap:
		return ParseConfigMap(doc)
	case kind == KindSecret:
		return ParseSecret(doc, opts.WipeSecrets)
	}
	return ParseRawObject(doc)
}

// checkAPIVersion enforces an api group prefix on a raw document.
func checkAPIVersion(doc map[string]any, want string) error {
	v, _ := doc["apiVersion"].(string)
	if v == "" {
		return inputf("missing apiVersion")
	}
	if !strings.HasPrefix(v, want) {
		return inputf("expected apiVersion %q, got %q", want, v)
	}
	return nil
}

// requireMetadata pulls name + namespace from a non-nil metadata block.
func requireMetadata(kind string, doc map[string]any) (name, ns string, err error) {
	md, ok := doc["metadata"].(map[string]any)
	if !ok || md == nil {
		return "", "", inputf("%s missing metadata", kind)
	}
	name, _ = md["name"].(string)
	if name == "" {
		return "", "", inputf("%s missing metadata.name", kind)
	}
	ns, _ = md["namespace"].(string)
	return name, ns, nil
}

// stringOr returns m[k] as a string, or fallback when absent/empty.
func stringOr(m map[string]any, k, fallback string) string {
	v, _ := m[k].(string)
	return cmp.Or(v, fallback)
}
