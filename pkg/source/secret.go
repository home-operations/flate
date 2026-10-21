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

// Error identifies the source and its unavailable Secret material.
func (e *MissingSecretError) Error() string {
	return fmt.Sprintf("%s: %s %s/%s: secret %s/%s %s",
		manifest.ErrMissingSecret, e.Owner.Kind, e.Owner.Namespace, e.Owner.Name,
		e.Secret.Namespace, e.Secret.Name, e.Detail)
}

// Unwrap keeps errors.Is(err, manifest.ErrMissingSecret) working.
func (e *MissingSecretError) Unwrap() error { return manifest.ErrMissingSecret }

// MissingSecretErr builds a *MissingSecretError for a source-group owner.
func MissingSecretErr(owner manifest.NamedResource, secretRef, reason string) error {
	owner.Group = manifest.SourceDomain
	return &MissingSecretError{
		Owner:  owner,
		Secret: manifest.NamedResource{Kind: manifest.KindSecret, Namespace: owner.Namespace, Name: secretRef},
		Detail: reason,
	}
}

// An unwired getter is configuration failure; missing material retains the
// source and Secret identities for the existing whole-source skip policy.
func resolveSecretRef(secrets SecretGetter, owner manifest.NamedResource, field string, ref *manifest.LocalObjectReference) (*manifest.Secret, error) {
	if secrets == nil {
		return nil, fmt.Errorf("%s %s references %s but no source.SecretGetter is wired",
			owner.Kind, owner.Name, field)
	}
	sec := secrets(owner.Namespace, ref.Name)
	if sec == nil {
		return nil, MissingSecretErr(owner, ref.Name, "not found")
	}
	return sec, nil
}

// BasicAuthFromSecret rejects malformed credentials and reports absent or wiped
// credentials through MissingSecretError.
func BasicAuthFromSecret(sec *manifest.Secret, owner manifest.NamedResource, secretRef string) (username, password string, err error) {
	username, _, err = sec.Value("username")
	if err != nil {
		return "", "", fmt.Errorf("%s: secret %s/%s: %w", QualifiedName(owner.Kind, owner.Namespace, owner.Name), owner.Namespace, secretRef, err)
	}
	password, _, err = sec.Value("password")
	if err != nil {
		return "", "", fmt.Errorf("%s: secret %s/%s: %w", QualifiedName(owner.Kind, owner.Namespace, owner.Name), owner.Namespace, secretRef, err)
	}
	if username == "" || password == "" || manifest.HasValuePlaceholder(username) || manifest.HasValuePlaceholder(password) {
		return "", "", MissingSecretErr(owner, secretRef, "missing username/password")
	}
	return username, password, nil
}

// SnapshotSecrets memoizes one serial operation's lookups, including absent Secrets.
// A nil getter remains unwired; callers MUST finish preparation before sharing results.
func SnapshotSecrets(secrets SecretGetter) SecretGetter {
	if secrets == nil {
		return nil
	}
	captured := make(map[[2]string]*manifest.Secret)
	return func(namespace, name string) *manifest.Secret {
		key := [2]string{namespace, name}
		sec, ok := captured[key]
		if !ok {
			sec = secrets(namespace, name)
			captured[key] = sec
		}
		return sec
	}
}
