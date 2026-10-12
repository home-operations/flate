package source

import (
	"context"
	"fmt"

	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
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
	// RegistryConfig is the global credential path for an eligible retry;
	// empty selects Docker's default config.
	RegistryConfig string
	// RetryWithRegistryConfig retries using global credentials or reports a
	// config error. Nil means no matching credential. Call only after rendered
	// Secrets are exhausted.
	RetryWithRegistryConfig func(context.Context, string) (*store.SourceArtifact, error)
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

// An unwired getter is configuration failure; missing material retains the
// source and Secret identities for the existing whole-source skip policy.
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

// BasicAuthFromSecret reports absent, undecodable or wiped credentials through
// MissingSecretError. Placeholder tokens must never become credentials.
func BasicAuthFromSecret(sec *manifest.Secret, ownerKind, ns, name, secretRef string) (username, password string, err error) {
	username, _, _ = sec.Value("username")
	password, _, _ = sec.Value("password")
	if username == "" || password == "" || manifest.HasValuePlaceholder(username) || manifest.HasValuePlaceholder(password) {
		return "", "", MissingSecretErr(ownerKind, ns, name, secretRef, "missing username/password")
	}
	return username, password, nil
}
