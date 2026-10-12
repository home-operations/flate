package manifest

import (
	"bytes"
	"encoding/base64"
	"fmt"
	"strings"

	kustomizev1 "github.com/fluxcd/kustomize-controller/api/v1"
)

// sopsCiphertextPrefix opens every SOPS-encrypted scalar. SOPS rewrites
// an encrypted value as `ENC[AES256_GCM,data:…,iv:…,tag:…,type:…]`; the
// algorithm-qualified prefix plus trailing `]` is specific enough that a
// real cleartext value never matches it by accident.
const sopsCiphertextPrefix = "ENC[AES256_GCM,"

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
	token := fmt.Sprintf(ValuePlaceholderTemplate, key)
	if encoding != nil {
		return encoding.EncodeToString([]byte(token))
	}
	return token
}

// IsEncryptedSecret reports whether doc looks like a SOPS-encrypted
// Kubernetes resource. SOPS appends a top-level `sops` map containing
// its metadata (mac, kms/age/pgp blocks, version) after encrypting the
// document's body; presence of that map with a `mac` or `version`
// field is the unambiguous signal.
//
// flate runs offline and cannot decrypt; the kustomization and
// helmrelease controllers call this to log the encrypted resource
// then wipe its data fields to ValuePlaceholderTemplate via
// ParseDoc, when --wipe-secrets is enabled.
// flate ignores spec.decryption entirely — there is no path that
// reads the decryption Secret.
func IsEncryptedSecret(doc map[string]any) bool {
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
