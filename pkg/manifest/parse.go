package manifest

import (
	"encoding/json"
	"fmt"

	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/validation/field"
)

// Flux API JSON tags govern typed admission of loaded and rendered documents.
func decodeInto(doc map[string]any, out any) error {
	raw, err := json.Marshal(doc)
	if err != nil {
		//nolint:wrapcheck // decodeCR adds the resource operation and preserves the admitted JSON error text.
		return err
	}
	//nolint:wrapcheck // decodeCR adds the resource operation and preserves the admitted JSON error text.
	return json.Unmarshal(raw, out)
}

func decodeCR(doc map[string]any, cr any, kind, domain string) error {
	if err := checkAPIVersion(doc, domain); err != nil {
		return err
	}
	// Typed JSON errors MUST precede permissive required-metadata checks.
	if err := decodeInto(doc, cr); err != nil {
		return NewError(ErrInput, fmt.Errorf("%s decode: %w", kind, err), "")
	}
	if _, _, missing := requireMetadata(doc); missing != "" {
		return inputf("%s missing metadata.name", kind)
	}
	return nil
}

// DocKind returns the "kind" field of a manifest document, or "" if absent.
func DocKind(doc map[string]any) string {
	k, _ := doc[kindKey].(string)
	return k
}

// DocAPIVersion returns the "apiVersion" field of a manifest document,
// or "" if absent.
func DocAPIVersion(doc map[string]any) string {
	v, _ := doc[apiVersionKey].(string)
	return v
}

// DocIdentity extracts a document's resource identity without validation or namespace defaulting.
func DocIdentity(doc map[string]any) NamedResource {
	name, namespace := DocMetadata(doc)
	return NamedResource{Group: apiGroup(DocAPIVersion(doc)), Kind: DocKind(doc), Namespace: namespace, Name: name}
}

// DocMetadata returns (name, namespace) from a manifest document's
// metadata block. Both are "" when metadata is absent or unset.
func DocMetadata(doc map[string]any) (string, string) {
	md, _ := doc[metadataKey].(map[string]any)
	if md == nil {
		return "", ""
	}
	name, _ := md[nameKey].(string)
	namespace, _ := md[namespaceKey].(string)
	return name, namespace
}

// ParseDocOptions tunes ParseDoc behavior.
type ParseDocOptions struct {
	// RequiredIdentity requires complete Kubernetes identity without namespace defaulting.
	RequiredIdentity bool
	// WipeSecrets replaces SOPS ciphertext with offline placeholders.
	WipeSecrets bool
	// DefaultNamespace fills omitted namespaced identity and authoritative document metadata.
	DefaultNamespace string
}

// ParseDoc decodes recognized group/kind pairs and returns the authoritative
// document and whether sanitation or namespace defaulting changed it, including
// on errors. Foreign kinds become RawObjects; bare data and Kustomize build
// directives return nil.
//
//nolint:ireturn // The manifest contract requires polymorphic results.
func ParseDoc(doc map[string]any, opts ParseDocOptions) (BaseManifest, map[string]any, bool, error) {
	kind, apiVersion := DocKind(doc), DocAPIVersion(doc)
	if opts.RequiredIdentity {
		if err := ValidateIdentity(doc, nil); err != nil {
			return nil, doc, false, err
		}
		opts.DefaultNamespace, opts.WipeSecrets = "", false
	}
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
		if opts.RequiredIdentity {
			if data, ok := obj.(Data); ok {
				err = validateDataDocument(doc, data)
			}
		}
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
		return NewError(
			ErrInput,
			fmt.Errorf("invalid %w", field.Invalid(field.NewPath(apiVersionKey), v, "must have a valid version")),
			"",
		)
	}
	if gv.Group != want {
		return inputf("expected apiVersion %q, got %q", want, v)
	}
	return nil
}

func requireMetadata(doc map[string]any) (string, string, string) {
	name, namespace := DocMetadata(doc)
	if name != "" {
		return name, namespace, ""
	}
	if md, _ := doc[metadataKey].(map[string]any); md == nil {
		return "", "", metadataKey
	}
	return "", "", "metadata.name"
}
