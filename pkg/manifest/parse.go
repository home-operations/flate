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
	if metadata := requireMetadata(doc); metadata.missing != "" {
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
	metadata := DocMetadata(doc)
	return NamedResource{
		Group:     apiGroup(DocAPIVersion(doc)),
		Kind:      DocKind(doc),
		Namespace: metadata.Namespace,
		Name:      metadata.Name,
	}
}

// Metadata preserves authored names without namespace defaulting.
type Metadata struct {
	Name      string
	Namespace string
}

// DocMetadata preserves empty metadata fields when authors omit them.
func DocMetadata(doc map[string]any) Metadata {
	md, _ := doc[metadataKey].(map[string]any)
	if md == nil {
		return Metadata{}
	}
	name, _ := md[nameKey].(string)
	namespace, _ := md[namespaceKey].(string)
	return Metadata{Name: name, Namespace: namespace}
}

// ParseDocOptions tunes ParseDoc behavior.
type ParseDocOptions struct {
	// RequiredIdentity requires complete Kubernetes identity without namespace defaulting.
	RequiredIdentity bool
	// DefaultNamespace fills omitted namespaced identity and authoritative document metadata.
	DefaultNamespace string
}

// ParsedDoc retains authoritative documents even when admission fails.
type ParsedDoc struct {
	Object   BaseManifest
	Document map[string]any
	Changed  bool
	Warnings []Warning
}

// ParseDoc decodes recognized group/kind pairs and returns the authoritative
// document and whether sanitation or namespace defaulting changed it, including
// on errors. Foreign kinds become RawObjects; bare data and Kustomize build
// directives return no object.
func ParseDoc(doc map[string]any, opts ParseDocOptions) (ParsedDoc, error) {
	kind, apiVersion := DocKind(doc), DocAPIVersion(doc)
	if opts.RequiredIdentity {
		if err := ValidateIdentity(doc, nil); err != nil {
			return ParsedDoc{Document: doc}, err
		}
		opts.DefaultNamespace = ""
	}
	if kind == "" || apiGroup(apiVersion) == KustomizeDomain {
		return ParsedDoc{Document: doc}, nil
	}
	if apiVersion == "" {
		return ParsedDoc{Document: doc}, inputf("missing apiVersion for %s", kind)
	}

	doc, warnings, changed := sanitizeDocument(doc, nil, kind, apiVersion)
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
	for i := range warnings {
		warnings[i].Resource = DocIdentity(doc)
	}
	return ParsedDoc{Object: obj, Document: doc, Changed: changed, Warnings: warnings}, err
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

type requiredMetadata struct {
	Metadata

	missing string
}

func requireMetadata(doc map[string]any) requiredMetadata {
	metadata := DocMetadata(doc)
	if metadata.Name != "" {
		return requiredMetadata{Metadata: metadata}
	}
	if md, _ := doc[metadataKey].(map[string]any); md == nil {
		return requiredMetadata{missing: metadataKey}
	}
	return requiredMetadata{missing: "metadata.name"}
}
