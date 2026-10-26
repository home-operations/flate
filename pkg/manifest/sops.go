package manifest

import (
	"bytes"
	"encoding/base64"
	"maps"
	"regexp"
	"strings"

	kustomizev1 "github.com/fluxcd/kustomize-controller/api/v1"
)

const sopsCiphertextPrefix = "ENC[AES256_GCM,"

// SOPS v3.13.3 aes/cipher.go encre defines framing without an end anchor.
// https://raw.githubusercontent.com/getsops/sops/v3.13.3/aes/cipher.go
var sopsCiphertext = regexp.MustCompile(`^ENC\[AES256_GCM,data:(.+),iv:(.+),tag:(.+),type:(.+)\]`)

// ValuePlaceholder returns the offline placeholder token for key.
func ValuePlaceholder(key string) string {
	return ValuePlaceholderPrefix + key + ".."
}

func sanitizeData(doc map[string]any, kind string) (map[string]any, bool) {
	var sanitized map[string]any
	for _, field := range []string{"data", "stringData", "binaryData"} {
		var encoding *base64.Encoding
		secretData := kind == KindSecret && field == "data"
		if field == "binaryData" || secretData {
			encoding = base64.StdEncoding
		}
		data, _ := doc[field].(map[string]any)
		var replaced map[string]any
		for key, raw := range data {
			if value, ok := raw.(string); ok && !secretData && len(value) < len(sopsCiphertextPrefix) {
				continue
			}
			value, err := scalarString(raw)
			if err != nil {
				continue
			}
			wiped := WipeValue(key, value, encoding, secretData)
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

// WipeValue replaces raw SOPS tokens and once-decoded Secret.data tokens or documents.
// Nil encoding leaves the per-key replacement as plaintext.
func WipeValue(key, value string, encoding *base64.Encoding, secretData bool) string {
	wiped := strings.HasPrefix(value, sopsCiphertextPrefix) && sopsCiphertext.MatchString(value)
	if !wiped && secretData && encoding != nil {
		var scratch [128]byte
		decoded, err := encoding.AppendDecode(scratch[:0], []byte(value))
		if err == nil {
			if bytes.HasPrefix(decoded, []byte(sopsCiphertextPrefix)) {
				// The regexp matcher makes its input escape; keep ordinary decoding on the stack.
				wiped = sopsCiphertext.Match(bytes.Clone(decoded))
			}
			// Flux 07fe06f32fd5 decryptor.go detects Secret documents by these markers.
			for _, marker := range []string{"mac: ENC[", `"mac": "ENC[`, "sops_mac=ENC[", "[sops]"} {
				if bytes.Contains(decoded, []byte(marker)) {
					wiped = true
					break
				}
			}
		}
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
