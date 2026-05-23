package source

import (
	"encoding/base64"
	"strings"

	"github.com/home-operations/flate/pkg/manifest"
)

// StringFromSecret reads a key from a Secret, preferring StringData
// over Data. Data values are base64-decoded (per k8s Secret semantics)
// before being returned, so the same string surface holds regardless
// of whether the source manifest used `data:` or `stringData:`.
// PLACEHOLDER_-wiped values (the result of flate's secret wiping
// pre-processing) are reported as empty so callers surface a clear
// "missing keys" error rather than authenticating with the literal
// placeholder.
//
// Used by per-kind Fetchers (git, oci, bucket) and cosign verification
// to resolve auth + trust material from the Secret a SecretRef points
// at.
func StringFromSecret(sec *manifest.Secret, key string) string {
	if v, ok := sec.StringData[key].(string); ok {
		if strings.HasPrefix(v, "..PLACEHOLDER_") {
			return ""
		}
		return v
	}
	if v, ok := sec.Data[key].(string); ok {
		// `data:` is base64-encoded YAML-side; decode so the returned
		// value is the actual material (PEM block, dockerconfigjson,
		// password, …) rather than its base64 envelope. Real k8s
		// Secrets that ship a `cosign.pub` or `tls.crt` use this form.
		decoded, err := base64.StdEncoding.DecodeString(v)
		if err != nil {
			return ""
		}
		s := string(decoded)
		if strings.HasPrefix(s, "..PLACEHOLDER_") {
			return ""
		}
		return s
	}
	return ""
}
