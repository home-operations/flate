package manifest

import (
	"bytes"
	"encoding/base64"
	"maps"
	"regexp"
	"slices"
	"strings"

	kustomizev1 "github.com/fluxcd/kustomize-controller/api/v1"
)

const sopsCiphertextPrefix = "ENC[AES256_GCM,"

// SOPS v3.13.3 aes/cipher.go encre defines framing without an end anchor.
// https://raw.githubusercontent.com/getsops/sops/v3.13.3/aes/cipher.go
var sopsCiphertext = regexp.MustCompile(`^ENC\[AES256_GCM,data:(.+),iv:(.+),tag:(.+),type:(.+)\]`)

// SanitizeDocument retains canonical ciphertext and removes only SOPS metadata.
//
//nolint:gocritic // DocOccurrence preserves an immutable value snapshot across this boundary.
func SanitizeDocument(o DocOccurrence) (DocOccurrence, bool) {
	doc, warnings, changed := sanitizeDocument(o.Document, o.Warnings, DocKind(o.Document), DocAPIVersion(o.Document))
	o.Document, o.Warnings = doc, warnings
	return o, changed
}

func sanitizeDocument(
	doc map[string]any,
	warnings []Warning,
	kind, apiVersion string,
) (map[string]any, []Warning, bool) {
	if apiVersion != CoreAPIVersion || kind != KindSecret && kind != KindConfigMap {
		return doc, warnings, false
	}
	for i := range warnings {
		if warnings[i].Resource != DocIdentity(doc) {
			warnings = slices.Clone(warnings)
			for j := range warnings {
				warnings[j].Resource = DocIdentity(doc)
			}
			break
		}
	}
	if _, present := doc["sops"]; !present {
		return doc, warnings, false
	}
	doc = maps.Clone(doc)
	delete(doc, "sops")
	return doc, warnings, true
}

// SubstitutionDocument keeps recognized ciphertext opaque to expression evaluation.
//
//nolint:gocritic // The SDK preserves immutable value snapshots and the value method set.
func (o DocOccurrence) SubstitutionDocument() map[string]any {
	doc := o.Document
	kind := DocKind(doc)
	if DocAPIVersion(doc) != CoreAPIVersion || kind != KindSecret && kind != KindConfigMap {
		return doc
	}
	var projected map[string]any
	for _, field := range []string{dataKey, stringDataKey, binaryDataKey} {
		value, changed := substitutionField(doc[field], kind == KindSecret && field == dataKey)
		if !changed {
			continue
		}
		if projected == nil {
			projected = maps.Clone(doc)
		}
		projected[field] = value
	}
	if projected != nil {
		return projected
	}
	return doc
}

func substitutionField(raw any, secretData bool) (any, bool) {
	if value, ok := raw.(string); ok {
		if opaqueField(value) && strings.Contains(value, "$") {
			return strings.ReplaceAll(value, "$", "$$"), true
		}
		return raw, false
	}
	data, _ := raw.(map[string]any)
	var encoding *base64.Encoding
	if secretData {
		encoding = base64.StdEncoding
	}
	var replaced map[string]any
	for key, raw := range data {
		value, ok := raw.(string)
		if !ok || !strings.Contains(value, "$") || !encryptedValue(value, encoding, secretData) {
			continue
		}
		if replaced == nil {
			replaced = maps.Clone(data)
		}
		replaced[key] = strings.ReplaceAll(value, "$", "$$")
	}
	if replaced != nil {
		return replaced, true
	}
	return raw, false
}

func encryptedValue(value string, encoding *base64.Encoding, secretData bool) bool {
	encrypted := strings.HasPrefix(value, sopsCiphertextPrefix) && sopsCiphertext.MatchString(value)
	if !encrypted && secretData && encoding != nil {
		encrypted = encryptedSecretValue(value, encoding)
	}
	return encrypted
}

// SubstituteAnnotationKey is Flux kustomize-controller's per-resource
// opt-out for postBuild substitution. A resource carrying this label
// or annotation with value "disabled" is excluded from envsubst,
// commonly used for ConfigMaps that embed shell scripts whose
// $${VAR[@]} bash array expansions would otherwise crash the parser.
const SubstituteAnnotationKey = "kustomize.toolkit.fluxcd.io/substitute"

// HasSubstituteDisabled reports whether a manifest doc carries the
// substitute-disabled label or annotation. flate skips envsubst on
// such resources, mirroring Flux's per-resource opt-out.
func HasSubstituteDisabled(doc map[string]any) bool {
	md, _ := doc[metadataKey].(map[string]any)
	if md == nil {
		return false
	}
	for _, field := range []string{labelsKey, annotationsKey} {
		m, _ := md[field].(map[string]any)
		if v, _ := m[SubstituteAnnotationKey].(string); v == kustomizev1.DisabledValue {
			return true
		}
	}
	return false
}

func encryptedSecretValue(value string, encoding *base64.Encoding) bool {
	encrypted := false
	var scratch [128]byte
	decoded, err := encoding.AppendDecode(scratch[:0], []byte(value))
	if err == nil {
		if bytes.HasPrefix(decoded, []byte(sopsCiphertextPrefix)) {
			// The regexp matcher makes its input escape; keep ordinary decoding on the stack.
			encrypted = sopsCiphertext.Match(bytes.Clone(decoded))
		}
		// Flux 07fe06f32fd5 decryptor.go detects Secret documents by these markers.
		for _, marker := range []string{"mac: ENC[", `"mac": "ENC[`, "sops_mac=ENC[", "[sops]"} {
			if bytes.Contains(decoded, []byte(marker)) {
				encrypted = true
				break
			}
		}
	}
	return encrypted
}
