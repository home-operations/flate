package source

import (
	"context"
	"fmt"

	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
	"github.com/home-operations/flate/pkg/values"
)

// MissingSecretError retains unavailable source material and an optional Secret identity.
// Acquisition owners supply the ambient continuation; Unwrap returns manifest.ErrMissingSecret.
type MissingSecretError struct {
	// Owner is the source requiring unavailable material.
	Owner manifest.NamedResource
	// Secret identifies the unavailable Secret; zero denotes source-only material.
	Secret manifest.NamedResource
	// Detail is the human reason ("not found", "missing username/password").
	Detail string
	// RetryAmbient attempts the acquisition owner's eligible ambient credentials.
	RetryAmbient func(context.Context) (*store.SourceArtifact, error)
}

// Error identifies the source and its unavailable Secret material.
func (e *MissingSecretError) Error() string {
	if e.Secret == (manifest.NamedResource{}) {
		return fmt.Sprintf("%s: %s: %s", manifest.ErrMissingSecret, e.Owner, e.Detail)
	}
	return fmt.Sprintf("%s: %s %s/%s: secret %s/%s %s",
		manifest.ErrMissingSecret, e.Owner.Kind, e.Owner.Namespace, e.Owner.Name,
		e.Secret.Namespace, e.Secret.Name, e.Detail)
}

// Unwrap keeps errors.Is(err, manifest.ErrMissingSecret) working.
func (e *MissingSecretError) Unwrap() error { return manifest.ErrMissingSecret }

// MissingSecretErr builds a *MissingSecretError for a source-group owner.
func MissingSecretErr(owner manifest.NamedResource, secretRef, reason string) error {
	owner.Group = manifest.SourceDomain
	missing := &MissingSecretError{Owner: owner, Detail: reason}
	if secretRef != "" {
		missing.Secret = manifest.NamedResource{Kind: manifest.KindSecret, Namespace: owner.Namespace, Name: secretRef}
	}
	return missing
}

type machineError struct{ cause error }

func (e *machineError) Error() string { return "machine credential acquisition failed" }
func (e *machineError) Unwrap() error { return e.cause }

// MachineCredentialErr masks credential material while preserving the acquisition cause.
func MachineCredentialErr(cause error) error { return &machineError{cause} }

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

// SecretLookup binds credential reads to one execution's captured inputs.
func SecretLookup(reads *store.ReadSet) SecretGetter {
	if reads == nil {
		return nil
	}
	provider := values.NewStoreProvider(reads, true)
	return func(namespace, name string) *manifest.Secret {
		secret, _ := provider.Data(manifest.NamedResource{Kind: manifest.KindSecret, Namespace: namespace, Name: name}).(*manifest.Secret)
		return secret
	}
}
