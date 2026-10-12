package manifest

import (
	"encoding/base64"
	"encoding/json"
	"maps"
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
	// WipeSecrets replaces SOPS ciphertext with offline placeholders.
	WipeSecrets bool
}

// ParseDoc decodes recognized group/kind pairs and returns the authoritative
// document and whether sanitation changed it, including on errors. Foreign kinds
// become RawObjects; bare data and Kustomize build directives return nil.
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
		encrypted := IsEncryptedSecret(doc)
		var sanitized map[string]any
		for _, field := range []string{"data", "stringData", "binaryData"} {
			var encoding *base64.Encoding
			if field == "binaryData" || (kind == KindSecret && field == "data") {
				encoding = base64.StdEncoding
			}
			data, _ := doc[field].(map[string]any)
			var replaced map[string]any
			for key, raw := range data {
				// Base64 decoding cannot lengthen a scalar to contain a longer marker.
				if value, ok := raw.(string); !encrypted && ok && len(value) < len(sopsCiphertextPrefix) {
					continue
				}
				value, err := scalarString(raw)
				if err != nil && !encrypted {
					continue
				}
				wiped := WipeValue(key, value, encoding, encrypted)
				if wiped == value {
					continue
				}
				if replaced == nil {
					replaced = maps.Clone(data)
				}
				replaced[key] = wiped
			}
			if replaced != nil {
				if sanitized == nil {
					sanitized = maps.Clone(doc)
				}
				sanitized[field] = replaced
			}
		}
		if sanitized != nil {
			doc = sanitized
			changed = true
		}
	}
	var obj BaseManifest
	var err error
	group := apiGroup(apiVersion)
	if i, ok := groupKindIndex[[2]string{group, kind}]; ok && kindTable[i].decode != nil && (group != "" || apiVersion == CoreAPIVersion) {
		obj, err = kindTable[i].decode(doc, opts)
	} else {
		obj, err = parseRawObject(doc)
	}
	return obj, doc, changed, err
}

// inGroup reports whether apiVersion belongs to the API group group.
// The group is compared exactly while the version is ignored, so any
// version of a group flate models is accepted (see constants.go) and a
// group that merely starts with one of ours is not.
func inGroup(apiVersion, group string) bool {
	g, _, _ := strings.Cut(apiVersion, "/")
	return g == group
}

// checkAPIVersion requires an exact API group match on a raw document.
func checkAPIVersion(doc map[string]any, want string) error {
	v := DocAPIVersion(doc)
	if v == "" {
		return inputf("missing apiVersion")
	}
	if !inGroup(v, want) {
		return inputf("expected apiVersion %q, got %q", want, v)
	}
	return nil
}

// requireMetadata pulls name + namespace from a non-nil metadata block.
func requireMetadata(kind string, doc map[string]any) (name, ns string, err error) {
	md, _ := doc["metadata"].(map[string]any)
	if md == nil {
		return "", "", inputf("%s missing metadata", kind)
	}
	name, _ = md["name"].(string)
	if name == "" {
		return "", "", inputf("%s missing metadata.name", kind)
	}
	ns, _ = md["namespace"].(string)
	return name, ns, nil
}
