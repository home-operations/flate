package source

import (
	"context"
	"errors"
	"fmt"

	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/store"
	"github.com/home-operations/flate/pkg/values"
)

// MissingSecretError retains unavailable source material and an optional Secret identity.
// Acquisition owners supply the ambient continuation; Unwrap returns manifest.ErrMissingSecret.
type MissingSecretError struct {
	// Owner is the source requiring unavailable material.
	Owner manifest.NamedResource
	// Subject is captured before unavailable material can be reported.
	Subject manifest.DiagnosticIdentity
	// Secret identifies the unavailable Secret; zero denotes source-only material.
	Secret manifest.NamedResource
	// Detail is the human reason ("not found", "missing username/password").
	Detail string
	// RetryAmbient attempts the acquisition owner's eligible ambient credentials.
	RetryAmbient func(context.Context) (*store.SourceArtifact, error)
}

// Error identifies the source and its unavailable Secret material.
func (e *MissingSecretError) Error() string {
	subject := e.Subject
	if subject.ID == (manifest.NamedResource{}) {
		subject = manifest.DiagnosticFor(e.Owner, nil)
	}
	if e.Secret == (manifest.NamedResource{}) {
		return fmt.Sprintf("%s: %s: %s", manifest.ErrMissingSecret, subject, e.Detail)
	}
	return fmt.Sprintf("%s: %s: secret %s/%s %s", manifest.ErrMissingSecret, subject.Label(), e.Secret.Namespace, e.Secret.Name, e.Detail)
}

// Unwrap keeps errors.Is(err, manifest.ErrMissingSecret) working.
func (e *MissingSecretError) Unwrap() error { return manifest.ErrMissingSecret }

// ProducerBacked reports whether captured declarations can supply the missing Secret.
func (e *MissingSecretError) ProducerBacked(reads *store.ReadSet) bool {
	if reads != nil && e.Secret != (manifest.NamedResource{}) {
		for _, p := range reads.Productions(e.Secret, nil) {
			if p.Declaration != (manifest.NamedResource{}) {
				return true
			}
		}
	}
	return false
}

// RetryMachine runs the acquisition owner's continuation when no producer can supply material.
func (e *MissingSecretError) RetryMachine(ctx context.Context, reads *store.ReadSet) (*store.SourceArtifact, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if e.RetryAmbient == nil || e.ProducerBacked(reads) {
		return nil, e
	}
	artifact, err := e.RetryAmbient(ctx)
	if errors.Is(err, manifest.ErrMissingSecret) && !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
		return artifact, e
	}
	return artifact, err
}

// AcquireMachine selects credentials before acquisition and classifies machine failures.
func AcquireMachine(ctx context.Context, policy cache.Policy, prepare func(context.Context) error, acquire func(context.Context, cache.Policy) (*store.SourceArtifact, error)) (*store.SourceArtifact, error) {
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("%w: %w", manifest.ErrFlux, err)
	}
	policy.Machine = true
	preparation := ctx
	var cancel context.CancelFunc
	if policy.Timeout != nil {
		preparation, cancel = context.WithTimeout(ctx, *policy.Timeout)
	} else {
		preparation, cancel = context.WithCancel(ctx)
	}
	err := prepare(preparation)
	if preparation.Err() != nil {
		err = errors.Join(err, preparation.Err())
	}
	cancel()
	var artifact *store.SourceArtifact
	if err == nil {
		artifact, err = acquire(ctx, policy)
	}
	if err != nil && !errors.Is(err, manifest.ErrMissingSecret) {
		if _, cached := errors.AsType[*cache.CacheError](err); !cached {
			if !errors.Is(err, manifest.ErrFlux) {
				err = fmt.Errorf("%w: %w", manifest.ErrFlux, err)
			}
			err = cache.MachineCredentialErr(err)
		}
	}
	return artifact, err
}

// MissingSecretErr captures unavailable material with raw and display identities.
func MissingSecretErr(owner manifest.DiagnosticIdentity, secretRef, reason string) error {
	missing := &MissingSecretError{Owner: owner.ID, Subject: owner, Detail: reason}
	if secretRef != "" {
		missing.Secret = manifest.NamedResource{Kind: manifest.KindSecret, Namespace: owner.ID.Namespace, Name: secretRef}
	}
	return missing
}

// An unwired getter is configuration failure; missing material retains the
// source and Secret identities for the existing whole-source skip policy.
func resolveSecretRef(secrets SecretGetter, owner manifest.DiagnosticIdentity, field string, ref *manifest.LocalObjectReference) (*manifest.Secret, error) {
	if secrets == nil {
		return nil, fmt.Errorf("%s references %s but no source.SecretGetter is wired",
			owner.Label(), field)
	}
	sec := secrets(owner.ID.Namespace, ref.Name)
	if sec == nil {
		return nil, MissingSecretErr(owner, ref.Name, "not found")
	}
	return sec, nil
}

// BasicAuthFromSecret rejects malformed credentials and reports absent or wiped
// credentials through MissingSecretError.
func BasicAuthFromSecret(sec *manifest.Secret, owner manifest.DiagnosticIdentity, secretRef string) (username, password string, err error) {
	username, _, err = sec.Value("username")
	if err != nil {
		return "", "", fmt.Errorf("%s: secret %s/%s: %w", owner.Label(), owner.ID.Namespace, secretRef, err)
	}
	password, _, err = sec.Value("password")
	if err != nil {
		return "", "", fmt.Errorf("%s: secret %s/%s: %w", owner.Label(), owner.ID.Namespace, secretRef, err)
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
