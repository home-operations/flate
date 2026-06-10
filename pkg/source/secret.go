package source

import (
	"encoding/base64"
	"fmt"

	"github.com/home-operations/flate/pkg/manifest"
)

// MissingSecretError reports that a source's auth SecretRef couldn't be
// resolved offline. It carries the missing Secret's identity so the source
// controller can consult the producer index (an in-repo ExternalSecret /
// SealedSecret declaring that Secret) and auto-skip without
// --allow-missing-secrets. Unwrap returns manifest.ErrMissingSecret so the
// existing errors.Is(err, ErrMissingSecret) gates (and the flag path) hold.
type MissingSecretError struct {
	// Owner is the source CR whose SecretRef couldn't resolve.
	Owner manifest.NamedResource
	// Secret is the missing Secret the SecretRef points at (Kind=Secret,
	// the source's namespace, the ref name).
	Secret manifest.NamedResource
	// Detail is the human reason ("not found", "missing username/password").
	Detail string
}

// Error renders the same string the previous fmt.Errorf form produced, so the
// skip reason surfaced via base.go (SkippedPrefix + TrimSentinelPrefix) is
// byte-identical to before.
func (e *MissingSecretError) Error() string {
	return fmt.Sprintf("%s: %s %s/%s: secret %s/%s %s",
		manifest.ErrMissingSecret, e.Owner.Kind, e.Owner.Namespace, e.Owner.Name,
		e.Secret.Namespace, e.Secret.Name, e.Detail)
}

// Unwrap keeps errors.Is(err, manifest.ErrMissingSecret) working.
func (e *MissingSecretError) Unwrap() error { return manifest.ErrMissingSecret }

// MissingSecretErr builds a *MissingSecretError. The signature is unchanged
// from the prior string-only form so every call site compiles untouched. The
// owner and secret share the namespace ns (a SecretRef is same-namespace).
func MissingSecretErr(kind, ns, name, secretRef, reason string) error {
	return &MissingSecretError{
		Owner:  manifest.NamedResource{Kind: kind, Namespace: ns, Name: name},
		Secret: manifest.NamedResource{Kind: manifest.KindSecret, Namespace: ns, Name: secretRef},
		Detail: reason,
	}
}

// resolveSecretRef fetches the Secret a set *SecretRef points at, shared
// by ResolveProxy and ResolveCertSecret. field names the spec field
// (e.g. "proxySecretRef") for diagnostics. Callers handle the nil-ref
// "not configured" case before calling; once ref is set, an unwired
// SecretGetter or a missing Secret is a loud error matching
// source-controller's fail-loud behavior.
func resolveSecretRef(secrets SecretGetter, ns, ownerKind, ownerID, field string, ref *manifest.LocalObjectReference) (*manifest.Secret, error) {
	if secrets == nil {
		return nil, fmt.Errorf("%s %s references %s but no source.SecretGetter is wired",
			ownerKind, ownerID, field)
	}
	sec := secrets(ns, ref.Name)
	if sec == nil {
		return nil, MissingSecretErr(ownerKind, ns, ownerID, ref.Name, "not found")
	}
	return sec, nil
}

// StringFromSecret reads a key from a Secret, preferring StringData
// over Data. Data values are base64-decoded (per k8s Secret semantics)
// before being returned, so the same string surface holds regardless
// of whether the source manifest used `data:` or `stringData:`.
// PLACEHOLDER_-wiped values (the result of flate's secret wiping
// pre-processing) are reported as empty so callers surface a clear
// "missing keys" error rather than authenticating with the literal
// placeholder.
//
// Used by per-kind Fetchers (git, oci, bucket) to resolve auth + TLS
// material from the Secret a SecretRef points at.
func StringFromSecret(sec *manifest.Secret, key string) string {
	if v, ok := sec.StringData[key].(string); ok {
		if manifest.IsValuePlaceholder(v) {
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
		if manifest.IsValuePlaceholder(s) {
			return ""
		}
		return s
	}
	return ""
}

// BasicAuthFromSecret extracts the username/password pair every HTTP-auth
// fetcher reads from a Secret (git HTTPS, HelmRepository). It returns a uniform
// MissingSecretErr when either field is absent or PLACEHOLDER-wiped — empty
// covers both the missing-key and ExternalSecret-stub cases, so a caller's
// --allow-missing-secrets gate (errors.Is ErrMissingSecret) treats them
// alike. ownerKind/ns/name/secretRef only shape that error message.
func BasicAuthFromSecret(sec *manifest.Secret, ownerKind, ns, name, secretRef string) (username, password string, err error) {
	username = StringFromSecret(sec, "username")
	password = StringFromSecret(sec, "password")
	if username == "" || password == "" {
		return "", "", MissingSecretErr(ownerKind, ns, name, secretRef, "missing username/password")
	}
	return username, password, nil
}
