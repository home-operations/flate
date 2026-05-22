package source

import (
	"strings"

	"github.com/home-operations/flate/pkg/manifest"
)

// StringFromSecret reads a key from a Secret, preferring StringData
// over Data. PLACEHOLDER_-wiped values (the result of flate's default
// --wipe-secrets pre-processing) are reported as empty so callers
// surface a clear "missing keys" error rather than authenticating
// with the literal placeholder.
//
// Used by per-kind Fetchers (git, oci, bucket) to resolve auth from
// the Secret a SecretRef points at.
func StringFromSecret(sec *manifest.Secret, key string) string {
	if v, ok := sec.StringData[key].(string); ok {
		if strings.HasPrefix(v, "..PLACEHOLDER_") {
			return ""
		}
		return v
	}
	if v, ok := sec.Data[key].(string); ok {
		if strings.HasPrefix(v, "..PLACEHOLDER_") {
			return ""
		}
		return v
	}
	return ""
}
