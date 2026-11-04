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

// SanitizeDocument removes unreadable core data while retaining occurrence diagnostics.
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
	sanitized, changed := sanitizeData(doc, kind)
	if !changed {
		return doc, warnings, false
	}
	var keys []string
	for _, field := range []string{"data", "stringData", "binaryData"} {
		before, _ := doc[field].(map[string]any)
		after, _ := sanitized[field].(map[string]any)
		if len(before) == len(after) {
			continue
		}
		for key := range before {
			if _, present := after[key]; !present {
				keys = append(keys, key)
			}
		}
	}
	doc = sanitized
	if len(keys) > 0 {
		slices.Sort(keys)
		warnings = append(slices.Clone(warnings), Warning{
			Resource: DocIdentity(doc),
			Category: WarnSOPSUnreadable,
			Detail:   slices.Compact(keys),
			Message:  "SOPS-encrypted data is unreadable; supply dummy values for references; re-encryption cannot be compared",
		})
	}
	return doc, warnings, changed
}

func sanitizeData(doc map[string]any, kind string) (map[string]any, bool) {
	var sanitized map[string]any
	for _, field := range []string{dataKey, stringDataKey, binaryDataKey} {
		var encoding *base64.Encoding
		secretData := kind == KindSecret && field == dataKey
		if field == binaryDataKey || secretData {
			encoding = base64.StdEncoding
		}
		data, _ := doc[field].(map[string]any)
		replaced := sanitizeDataField(data, encoding, secretData)
		if replaced != nil {
			if sanitized == nil {
				sanitized = maps.Clone(doc)
			}
			sanitized[field] = replaced
		}
	}
	if _, ok := doc["sops"]; ok {
		if sanitized == nil {
			sanitized = maps.Clone(doc)
		}
		delete(sanitized, "sops")
	}
	if sanitized != nil {
		return sanitized, true
	}
	return doc, false
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

func sanitizeDataField(data map[string]any, encoding *base64.Encoding, secretData bool) map[string]any {
	var replaced map[string]any
	for key, raw := range data {
		if value, ok := raw.(string); ok && !secretData && len(value) < len(sopsCiphertextPrefix) {
			continue
		}
		value, err := scalarString(raw)
		if err != nil {
			continue
		}
		if !encryptedValue(value, encoding, secretData) {
			continue
		}
		if replaced == nil {
			replaced = maps.Clone(data)
		}
		delete(replaced, key)
	}
	return replaced
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
