package manifest

import (
	"encoding/json"
	"strings"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// decodeInto re-marshals a parsed YAML document into a typed Flux CR
// via a JSON round-trip. The Flux API types use the same JSON tags
// k8s uses to serialize their CRDs, so this matches `kubectl apply`'s
// decoding fidelity without keeping the raw YAML bytes around.
//
// Per-document cost is ~one marshal + one unmarshal; documents are
// small (a single CR) and this only runs during the load phase, not on
// the render hot path.
func decodeInto(doc map[string]any, out any) error {
	raw, err := json.Marshal(doc)
	if err != nil {
		return err
	}
	return json.Unmarshal(raw, out)
}

// decodeCR runs the shared typed-CR decode preamble: API-version
// check, JSON round-trip into cr, and a non-empty metadata.name
// check. cr must be a pointer to a Flux CR type (all embed
// metav1.ObjectMeta, so GetName() is available). Centralizes the
// identical opening previously hand-copied across the typed parsers.
func decodeCR(doc map[string]any, cr metav1.Object, kind, domain string) error {
	if err := checkAPIVersion(doc, domain); err != nil {
		return err
	}
	if err := decodeInto(doc, cr); err != nil {
		return inputf("%s decode: %w", kind, err)
	}
	if cr.GetName() == "" {
		return inputf("%s missing metadata.name", kind)
	}
	return nil
}

// DocKind returns the "kind" field of a manifest document, or "" if absent.
func DocKind(doc map[string]any) string {
	k, _ := doc["kind"].(string)
	return k
}

// DocAPIVersion returns the "apiVersion" field of a manifest document,
// or "" if absent.
func DocAPIVersion(doc map[string]any) string {
	v, _ := doc["apiVersion"].(string)
	return v
}

// DropKinds returns docs with every entry whose `kind` appears in drop
// removed. drop=nil is a no-op (returns docs unchanged). Used by the
// orchestrator's Render and the CLI's build/diff paths to honor
// --skip-secrets / --skip-crds / --skip-kinds against both
// HelmRelease and Kustomization sources uniformly. helm.TemplateDocs
// already filters HR output upstream; this is the canonical helper
// for downstream code that needs the same operation.
func DropKinds(docs []map[string]any, drop []string) []map[string]any {
	if len(drop) == 0 || len(docs) == 0 {
		return docs
	}
	dropSet := make(map[string]struct{}, len(drop))
	for _, kind := range drop {
		dropSet[kind] = struct{}{}
	}
	var out []map[string]any
	for i, doc := range docs {
		if _, skip := dropSet[DocKind(doc)]; skip {
			if out == nil {
				out = make([]map[string]any, 0, len(docs)-1)
				out = append(out, docs[:i]...)
			}
			continue
		}
		if out != nil {
			out = append(out, doc)
		}
	}
	if out == nil {
		return docs
	}
	return out
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

// defaultParseDocOptions returns the standard options — secrets wiped.
func defaultParseDocOptions() ParseDocOptions {
	return ParseDocOptions{WipeSecrets: true}
}

// ParseDoc dispatches on kind + apiVersion to the appropriate concrete
// parser. Unknown kinds become a RawObject; kustomize.config.k8s.io
// build directives and bare data files are silently dropped.
func ParseDoc(doc map[string]any, opts ParseDocOptions) (BaseManifest, error) {
	kind := DocKind(doc)
	apiVersion := DocAPIVersion(doc)
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
		return parseKustomization(doc)
	case kind == KindHelmRelease:
		return parseHelmRelease(doc)
	case kind == KindHelmRepository:
		return parseHelmRepository(doc)
	case kind == KindHelmChart && strings.HasPrefix(apiVersion, SourceDomain):
		return parseHelmChartSource(doc)
	case kind == KindGitRepository:
		return parseGitRepository(doc)
	case kind == KindOCIRepository:
		return ParseOCIRepository(doc)
	case kind == KindExternalArtifact && strings.HasPrefix(apiVersion, SourceDomain):
		return parseExternalArtifact(doc)
	case kind == KindBucket && strings.HasPrefix(apiVersion, SourceDomain):
		return parseBucket(doc)
	case kind == KindResourceSet && strings.HasPrefix(apiVersion, FluxOperatorDomain):
		return parseResourceSet(doc)
	case kind == KindResourceSetInputProvider && strings.HasPrefix(apiVersion, FluxOperatorDomain):
		return parseResourceSetInputProvider(doc)
	case kind == KindConfigMap:
		return parseConfigMap(doc, opts.WipeSecrets)
	case kind == KindSecret:
		return parseSecret(doc, opts.WipeSecrets)
	}
	return parseRawObject(doc)
}

// IsKustomizeBuildDirective reports whether obj is a kustomize.config.k8s.io
// build directive (a Kustomization or Component). ParseDoc preserves these as
// metadata-less RawObjects — they're build inputs, not cluster resources, and
// the loader drops them during discovery. Render paths must drop them too: a
// kustomization.yaml self-referenced in its own resources: makes `kustomize
// build` emit one, which would otherwise land in the Store as a phantom,
// nameless object (id "<kind>//") that no controller ever reconciles.
func IsKustomizeBuildDirective(obj BaseManifest) bool {
	raw, ok := obj.(*RawObject)
	return ok && strings.HasPrefix(raw.APIVersion, KustomizeDomain)
}

// checkAPIVersion enforces an api group prefix on a raw document.
func checkAPIVersion(doc map[string]any, want string) error {
	v := DocAPIVersion(doc)
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
