package manifest

import (
	"encoding/json"

	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/validation/field"
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

func decodeCR(doc map[string]any, cr any, kind, domain string) error {
	if err := checkAPIVersion(doc, domain); err != nil {
		return err
	}
	// Typed JSON errors MUST precede permissive required-metadata checks.
	if err := decodeInto(doc, cr); err != nil {
		return inputf("%s decode: %w", kind, err)
	}
	if _, _, missing := requireMetadata(doc); missing != "" {
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

// DropKinds returns docs with every entry whose kind appears in drop removed.
// drop=nil returns docs unchanged. Orchestrator Render and the stream emitter
// use it to apply skipped kinds to HelmRelease and Kustomization output.
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

// DocIdentity extracts a document's resource identity without validation or namespace defaulting.
func DocIdentity(doc map[string]any) NamedResource {
	name, namespace := DocMetadata(doc)
	return NamedResource{Group: apiGroup(DocAPIVersion(doc)), Kind: DocKind(doc), Namespace: namespace, Name: name}
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
	return name, namespace
}

// ParseDocOptions tunes ParseDoc behavior.
type ParseDocOptions struct {
	// WipeSecrets replaces SOPS ciphertext with offline placeholders.
	WipeSecrets bool
	// DefaultNamespace fills omitted namespaced identity and authoritative document metadata.
	DefaultNamespace string
}

// ParseDoc decodes recognized group/kind pairs and returns the authoritative
// document and whether sanitation or namespace defaulting changed it, including
// on errors. Foreign kinds become RawObjects; bare data and Kustomize build
// directives return nil.
func ParseDoc(doc map[string]any, opts ParseDocOptions) (BaseManifest, map[string]any, bool, error) {
	kind, apiVersion := DocKind(doc), DocAPIVersion(doc)
	if kind == "" || apiGroup(apiVersion) == KustomizeDomain {
		return nil, doc, false, nil
	}
	if apiVersion == "" {
		return nil, doc, false, inputf("missing apiVersion for %s", kind)
	}

	changed := false
	if opts.WipeSecrets && apiVersion == CoreAPIVersion && (kind == KindConfigMap || kind == KindSecret) {
		doc, changed = sanitizeData(doc, kind)
	}
	var obj BaseManifest
	var err error
	group := apiGroup(apiVersion)
	if i, ok := kindIndex[kind]; ok && kindTable[i].Group == group && (group != "" || apiVersion == CoreAPIVersion) {
		obj, err = kindTable[i].decode(doc)
	} else {
		obj, err = parseRawObject(doc)
	}
	if err == nil {
		obj, doc, changed = defaultNamespace(obj, doc, opts.DefaultNamespace, changed)
	}
	return obj, doc, changed, err
}

// checkAPIVersion requires an exact API group match on a raw document.
func checkAPIVersion(doc map[string]any, want string) error {
	v := DocAPIVersion(doc)
	if v == "" {
		return inputf("missing apiVersion")
	}
	gv, err := schema.ParseGroupVersion(v)
	if err != nil || gv.Version == "" {
		return inputf("invalid %w", field.Invalid(field.NewPath("apiVersion"), v, "must have a valid version"))
	}
	if gv.Group != want {
		return inputf("expected apiVersion %q, got %q", want, v)
	}
	return nil
}

func requireMetadata(doc map[string]any) (name, namespace, missing string) {
	name, namespace = DocMetadata(doc)
	if name != "" {
		return name, namespace, ""
	}
	if md, _ := doc["metadata"].(map[string]any); md == nil {
		return "", "", "metadata"
	}
	return "", "", "metadata.name"
}
