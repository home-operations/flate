package manifest

import (
	"bytes"
	"encoding/base64"
	"fmt"
	"maps"
	"strings"
)

// ConfigMap is the core/v1 ConfigMap.
type ConfigMap struct {
	Name       string         `json:"name"                yaml:"name"`
	Namespace  string         `json:"namespace,omitempty" yaml:"namespace,omitempty"`
	Data       map[string]any `json:"-"                   yaml:"-"`
	BinaryData map[string]any `json:"-"                   yaml:"-"`
}

// Named identifies the ConfigMap.
func (c *ConfigMap) Named() NamedResource {
	return NamedResource{Kind: KindConfigMap, Namespace: c.Namespace, Name: c.Name}
}

// parseConfigMap decodes a core/v1 ConfigMap. When wipeSecrets is set
// (the default, matching parseSecret), SOPS-encrypted data values are
// replaced with placeholders — flate can't decrypt them and the raw
// ciphertext otherwise poisons downstream rendering.
func parseConfigMap(doc map[string]any, wipeSecrets bool) (*ConfigMap, error) {
	if err := checkAPIVersion(doc, "v1"); err != nil {
		return nil, err
	}
	name, ns, err := requireMetadata("ConfigMap", doc)
	if err != nil {
		return nil, err
	}
	cm := &ConfigMap{Name: name, Namespace: ns}
	if v, ok := doc["data"].(map[string]any); ok {
		if wipeSecrets {
			// ConfigMap .data is plaintext; a SOPS-encrypted value is an
			// ENC[...] scalar (exact match), matching historical behavior.
			v = wipeSopsCiphertext(v, IsEncryptedSecret(doc), IsSopsCiphertext, plaintextPlaceholder)
		}
		cm.Data = v
	}
	if v, ok := doc["binaryData"].(map[string]any); ok {
		cm.BinaryData = v
	}
	return cm, nil
}

// wipeSopsCiphertext replaces SOPS-encrypted scalars with a per-key
// placeholder (rendered by encode). flate runs offline and cannot decrypt, so
// SOPS ciphertext (commonly a postBuild.substituteFrom / valuesFrom source)
// would otherwise feed raw `ENC[AES256_GCM,…]` into envsubst — and the `:`
// inside trips chart validation (Ingress hosts, cert-manager dnsNames).
//
// A value is wiped when valueIsSops reports it carries SOPS ciphertext OR
// wholeEncrypted is set (the enclosing doc carries a top-level sops: block, so
// every value is ciphertext). Non-encrypted values in a non-encrypted doc —
// plaintext, public keys, certs, binary — pass through untouched. Returns data
// unchanged when nothing matches to avoid a needless copy on the common path.
//
// valueIsSops is field-specific: ConfigMap .data uses an exact ENC[…] scalar
// match; Secret fields use the broader sopsBearing* detectors (which also
// look inside base64). encode renders the placeholder in the field's encoding:
// plaintext for ConfigMap .data and Secret .stringData; base64 for Secret
// .data (so downstream base64-decode still yields the placeholder string).
//
// Scope: ConfigMap data and Secret data/stringData (via parseConfigMap /
// parseSecret). SOPS-encrypted inline HelmRelease spec.values — a
// partially-encrypted HR file via encrypted_regex — bypass this path and are
// left as-is.
func wipeSopsCiphertext(data map[string]any, wholeEncrypted bool, valueIsSops func(string) bool, encode func(key string) any) map[string]any {
	var out map[string]any
	for k, v := range data {
		s, isStr := v.(string)
		wipe := wholeEncrypted || (isStr && valueIsSops(s))
		if !wipe {
			continue
		}
		if out == nil {
			out = make(map[string]any, len(data))
			maps.Copy(out, data)
		}
		out[k] = encode(k)
	}
	if out == nil {
		return data
	}
	return out
}

// Secret is the core/v1 Secret. By default cleartext data is wiped to a
// placeholder during parsing.
type Secret struct {
	Name       string         `json:"name"                yaml:"name"`
	Namespace  string         `json:"namespace,omitempty" yaml:"namespace,omitempty"`
	Data       map[string]any `json:"-"                   yaml:"-"`
	StringData map[string]any `json:"-"                   yaml:"-"`
}

// Named identifies the Secret.
func (s *Secret) Named() NamedResource {
	return NamedResource{Kind: KindSecret, Namespace: s.Namespace, Name: s.Name}
}

// parseSecret decodes a Secret. flate is an offline renderer of the user's
// own repo, not a live cluster, so it does NOT blanket-wipe Secret values —
// plaintext, public keys, certs, and binary data pass through verbatim for
// faithful rendering. The one thing it must neutralize (when wipeSecrets is
// set) is SOPS ciphertext: flate can't decrypt it, and raw ENC[AES256_GCM,…]
// poisons downstream rendering (the `:`/commas break envsubst, Ingress hosts,
// cert-manager dnsNames). SOPS-encrypted scalars are replaced with the same
// ..PLACEHOLDER_<key>.. token parseConfigMap uses.
func parseSecret(doc map[string]any, wipeSecrets bool) (*Secret, error) {
	if err := checkAPIVersion(doc, "v1"); err != nil {
		return nil, err
	}
	name, ns, err := requireMetadata("Secret", doc)
	if err != nil {
		return nil, err
	}
	s := &Secret{Name: name, Namespace: ns}
	// A top-level sops: block means the whole Secret is SOPS-encrypted, so
	// every data value is ciphertext (even when a value doesn't carry the
	// ENC[...] marker verbatim, e.g. base64-wrapped). Combined with per-value
	// IsSopsCiphertext, this catches both whole-secret and inline encryption.
	encrypted := wipeSecrets && IsEncryptedSecret(doc)
	if data, ok := doc["data"].(map[string]any); ok {
		// .data values are base64-encoded per the Kubernetes Secret schema, so
		// a wiped value is the base64 of the placeholder — downstream readers
		// (StringFromSecret, valuesFrom decodeBag) base64-decode it back to the
		// ..PLACEHOLDER_<key>.. string. sopsBearingData also looks INSIDE the
		// base64 (a whole SOPS-encrypted values file mounted via valuesFrom).
		if wipeSecrets {
			data = wipeSopsCiphertext(data, encrypted, sopsBearingData, base64Placeholder)
		}
		s.Data = data
	}
	if sd, ok := doc["stringData"].(map[string]any); ok {
		// .stringData stays plaintext per the Kubernetes Secret schema.
		if wipeSecrets {
			sd = wipeSopsCiphertext(sd, encrypted, sopsBearingString, plaintextPlaceholder)
		}
		s.StringData = sd
	}
	return s, nil
}

// sopsBearingString reports whether a plaintext value carries SOPS ciphertext
// anywhere — a raw ENC[AES256_GCM,…] scalar, or an embedded multi-line blob
// containing the marker. flate can't decrypt it, so the whole value is wiped.
func sopsBearingString(s string) bool {
	return strings.Contains(s, sopsCiphertextPrefix)
}

// sopsBearingData reports whether a base64 Secret .data value carries SOPS
// ciphertext: either the raw value contains the marker, or its base64-decoded
// content does. The latter is the common "SOPS-encrypted helm values file
// mounted via valuesFrom Secret" pattern — the whole values.yaml (with its
// ENC[...] scalars) is one base64 blob, which would otherwise leak ciphertext
// into chart values and corrupt the render.
func sopsBearingData(s string) bool {
	if sopsBearingString(s) {
		return true
	}
	dec, err := base64.StdEncoding.DecodeString(s)
	return err == nil && bytes.Contains(dec, []byte(sopsCiphertextPrefix))
}

// plaintextPlaceholder renders the per-key wipe token verbatim — for plaintext
// fields (ConfigMap .data, Secret .stringData).
func plaintextPlaceholder(key string) any {
	return fmt.Sprintf(ValuePlaceholderTemplate, key)
}

// base64Placeholder renders the per-key wipe token base64-encoded — for the
// Secret .data field, whose values are base64 per the Kubernetes schema.
func base64Placeholder(key string) any {
	return base64.StdEncoding.EncodeToString(fmt.Appendf(nil, ValuePlaceholderTemplate, key))
}
