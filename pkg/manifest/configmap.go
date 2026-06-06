package manifest

import (
	"encoding/base64"
	"fmt"
	"maps"
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
			v = wipeSopsCiphertext(v)
		}
		cm.Data = v
	}
	if v, ok := doc["binaryData"].(map[string]any); ok {
		cm.BinaryData = v
	}
	return cm, nil
}

// wipeSopsCiphertext replaces SOPS-encrypted ConfigMap values with the
// same placeholder parseSecret uses for wiped Secret keys. flate runs
// offline and cannot decrypt, so a SOPS-encrypted ConfigMap (commonly a
// postBuild.substituteFrom source) would otherwise feed raw ciphertext
// into envsubst — and the `:` inside `ENC[AES256_GCM,…]` then trips
// chart validation (Ingress hosts, cert-manager dnsNames). Gated by the
// caller's wipeSecrets flag so it tracks Secret wiping: callers that opt
// to keep cleartext Secrets (WipeSecrets=false) keep the ciphertext too.
// Only encrypted scalars are touched, so a partially-encrypted ConfigMap
// keeps its cleartext entries. Returns data unchanged when nothing
// matches to avoid a needless copy on the common cleartext path.
//
// Scope: this covers ConfigMap data (and Secret data, via parseSecret).
// SOPS-encrypted inline HelmRelease spec.values — a partially-encrypted
// HR file via encrypted_regex — bypass this path and are left as-is.
func wipeSopsCiphertext(data map[string]any) map[string]any {
	var out map[string]any
	for k, v := range data {
		s, ok := v.(string)
		if !ok || !IsSopsCiphertext(s) {
			continue
		}
		if out == nil {
			out = make(map[string]any, len(data))
			maps.Copy(out, data)
		}
		out[k] = fmt.Sprintf(ValuePlaceholderTemplate, k)
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

// parseSecret decodes a Secret, wiping cleartext when wipeSecrets is true.
func parseSecret(doc map[string]any, wipeSecrets bool) (*Secret, error) {
	if err := checkAPIVersion(doc, "v1"); err != nil {
		return nil, err
	}
	name, ns, err := requireMetadata("Secret", doc)
	if err != nil {
		return nil, err
	}
	s := &Secret{Name: name, Namespace: ns}
	if data, ok := doc["data"].(map[string]any); ok {
		// .data values are base64-encoded per the Kubernetes Secret schema.
		s.Data = wiperField(data, wipeSecrets, func(k string) any {
			return base64.StdEncoding.EncodeToString(fmt.Appendf(nil, ValuePlaceholderTemplate, k))
		})
	}
	if sd, ok := doc["stringData"].(map[string]any); ok {
		// .stringData stays plaintext per the Kubernetes Secret schema.
		s.StringData = wiperField(sd, wipeSecrets, func(k string) any {
			return fmt.Sprintf(ValuePlaceholderTemplate, k)
		})
	}
	return s, nil
}

// wiperField copies src, replacing each value with a per-key placeholder
// (produced by encode) when wipeSecrets is set, else passing the original
// value through. parseSecret calls it for both .data and .stringData —
// the only difference between those fields is the encoding, via encode.
func wiperField(src map[string]any, wipeSecrets bool, encode func(key string) any) map[string]any {
	out := make(map[string]any, len(src))
	for k, v := range src {
		if wipeSecrets {
			out[k] = encode(k)
		} else {
			out[k] = v
		}
	}
	return out
}
