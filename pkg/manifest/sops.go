package manifest

import (
	"bytes"
	"encoding/base64"
	"maps"
	"strings"

	kustomizev1 "github.com/fluxcd/kustomize-controller/api/v1"
)

// sopsCiphertextPrefix opens every SOPS-encrypted scalar. SOPS rewrites
// an encrypted value as `ENC[AES256_GCM,data:…,iv:…,tag:…,type:…]`; the
// algorithm-qualified prefix plus trailing `]` is specific enough that a
// real cleartext value never matches it by accident.
const sopsCiphertextPrefix = "ENC[AES256_GCM,"

// ValuePlaceholder returns the offline placeholder token for key.
func ValuePlaceholder(key string) string {
	return ValuePlaceholderPrefix + key + ".."
}

func sanitizeData(doc map[string]any, kind string) (map[string]any, bool) {
	encrypted := isEncryptedSecret(doc)
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
		return sanitized, true
	}
	return doc, false
}

// WipeValue replaces raw or once-decoded SOPS ciphertext with a per-key token.
// Nil encoding denotes a plaintext field; malformed encoding alone is preserved.
func WipeValue(key, value string, encoding *base64.Encoding, encrypted bool) string {
	wiped := encrypted || strings.Contains(value, sopsCiphertextPrefix)
	if !wiped && encoding != nil {
		var scratch [128]byte
		decoded, err := encoding.AppendDecode(scratch[:0], []byte(value))
		wiped = err == nil && bytes.Contains(decoded, []byte(sopsCiphertextPrefix))
	}
	if !wiped {
		return value
	}
	token := ValuePlaceholder(key)
	if encoding != nil {
		return encoding.EncodeToString([]byte(token))
	}
	return token
}

func isEncryptedSecret(doc map[string]any) bool {
	sops, ok := doc["sops"].(map[string]any)
	if !ok {
		return false
	}
	_, hasMac := sops["mac"]
	_, hasVersion := sops["version"]
	return hasMac || hasVersion
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
	md, _ := doc["metadata"].(map[string]any)
	if md == nil {
		return false
	}
	for _, field := range []string{"labels", "annotations"} {
		m, _ := md[field].(map[string]any)
		if v, _ := m[SubstituteAnnotationKey].(string); v == kustomizev1.DisabledValue {
			return true
		}
	}
	return false
}
