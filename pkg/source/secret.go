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
// Acquisition owners supply the ambient continuation.
type MissingSecretError struct {
	// Owner is the source requiring unavailable material.
	Owner manifest.NamedResource
	// Subject is captured before unavailable material can be reported.
	Subject manifest.DiagnosticIdentity
	// Secret identifies the unavailable Secret; zero denotes source-only material.
	Secret manifest.NamedResource
	// Detail is the human reason ("not found", "missing username/password").
	Detail string
	// AnonymousDenied records a typed registry denial after no credential was selected.
	AnonymousDenied bool
	// Cause preserves the acquisition owner's typed failure.
	Cause error
	// RetryAmbient attempts the acquisition owner's eligible ambient credentials.
	RetryAmbient func(context.Context) (PreparedFetch, error)
}

// Error identifies unavailable material.
func (e *MissingSecretError) Error() string {
	if e.Secret == (manifest.NamedResource{}) {
		return e.Detail
	}
	return fmt.Sprintf("secret %s/%s %s", e.Secret.Namespace, e.Secret.Name, e.Detail)
}

// Unwrap keeps [errors.Is](err, manifest.ErrMissingSecret) working.
func (e *MissingSecretError) Unwrap() error {
	if e.Cause != nil {
		return errors.Join(manifest.ErrMissingSecret, e.Cause)
	}
	return manifest.ErrMissingSecret
}

// ProducerBacked reports whether captured declarations can supply the missing Secret.
func (e *MissingSecretError) ProducerBacked(reads *store.ReadSet) bool {
	if reads != nil && e.Secret != (manifest.NamedResource{}) {
		productions := reads.Productions(e.Secret, nil)
		for i := range productions {
			if productions[i].Declaration != (manifest.NamedResource{}) {
				return true
			}
		}
	}
	return false
}

// RetryMachine runs the acquisition owner's continuation when no producer can supply material.
func (e *MissingSecretError) RetryMachine(ctx context.Context, reads *store.ReadSet) (PreparedFetch, error) {
	if err := ctx.Err(); err != nil {
		return PreparedFetch{}, fmt.Errorf("retry machine credentials: %w", err)
	}
	if e.RetryAmbient == nil || e.ProducerBacked(reads) {
		return PreparedFetch{}, e
	}
	prepared, err := e.RetryAmbient(ctx)
	normalize := func(err error) error {
		if errors.Is(err, manifest.ErrMissingSecret) && !errors.Is(err, context.Canceled) &&
			!errors.Is(err, context.DeadlineExceeded) {
			if missing, ok := errors.AsType[*MissingSecretError](
				err,
			); ok && missing.AnonymousDenied &&
				e.Secret == (manifest.NamedResource{}) {
				return err
			}
			return e
		}
		return err
	}
	if prepared.Fetch != nil {
		fetch := prepared.Fetch
		prepared.Fetch = func(ctx context.Context, local *store.SourceArtifact) (*store.SourceArtifact, error) {
			artifact, fetchErr := fetch(ctx, local)
			return artifact, normalize(fetchErr)
		}
	}
	return prepared, normalize(err)
}

// AcquireMachine bounds preparation independently of deferred content acquisition.
func AcquireMachine(
	ctx context.Context,
	policyValue *cache.Policy,
	prepare func(context.Context) error,
	acquire func(context.Context, *cache.Policy, *store.SourceArtifact) (*store.SourceArtifact, error),
) (PreparedFetch, error) {
	policy := *policyValue
	classify := func(err error) error {
		if err != nil && !errors.Is(err, manifest.ErrMissingSecret) {
			if _, cached := errors.AsType[*cache.CacheError](err); !cached {
				if !errors.Is(err, manifest.ErrFlux) {
					err = manifest.NewError(manifest.ErrFlux, err, "")
				}
				err = cache.MachineCredentialErr(err)
			}
		}
		return err
	}
	if err := ctx.Err(); err != nil {
		return PreparedFetch{}, classify(err)
	}
	policy.Machine = true
	var preparation context.Context
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
	if err != nil {
		return PreparedFetch{}, classify(err)
	}
	return PreparedFetch{Fetch: func(ctx context.Context, local *store.SourceArtifact) (*store.SourceArtifact, error) {
		artifact, fetchErr := acquire(ctx, &policy, local)
		return artifact, classify(fetchErr)
	}}, nil
}

// MissingSecretErr captures unavailable material with raw and display identities.
//
//nolint:gocritic // The SDK accepts a value snapshot; a pointer parameter would change its call and aliasing contracts.
func MissingSecretErr(owner manifest.DiagnosticIdentity, secretRef, reason string) error {
	missing := &MissingSecretError{Owner: owner.ID, Subject: owner, Detail: reason}
	if secretRef != "" {
		missing.Secret = manifest.NamedResource{
			Kind:      manifest.KindSecret,
			Namespace: owner.ID.Namespace,
			Name:      secretRef,
		}
	}
	return missing
}

// An unwired getter is configuration failure; missing material retains the
// source and Secret identities for the existing whole-source skip policy.
func resolveSecretRef(
	secrets SecretGetter,
	ownerValue *manifest.DiagnosticIdentity,
	field string,
	ref *manifest.LocalObjectReference,
) (store.ReferenceFacts, error) {
	owner := *ownerValue
	if secrets == nil {
		return store.ReferenceFacts{}, fmt.Errorf("references %s but no source.SecretGetter is wired", field)
	}
	return secrets(owner.ID.Namespace, ref.Name), nil
}

// SecretValue MUST classify each consumed field from the same captured facts.
func SecretValue(
	input *store.ReferenceFacts,
	owner *manifest.DiagnosticIdentity,
	secretRef, key string,
) (values.Result, error) {
	return values.Classify(new(manifest.Reference{
		Kind: manifest.KindSecret, Namespace: owner.ID.Namespace, Name: secretRef, Key: key, Use: "secretRef",
	}), input)
}

// UnavailableSecret MUST retain acquisition retry and whole-source policy.
func UnavailableSecret(owner *manifest.DiagnosticIdentity, secretRef string, result *values.Result) error {
	return MissingSecretErr(*owner, secretRef, result.Cause())
}

// BasicAuthFromSecret rejects malformed credentials and reports absent
// credentials through MissingSecretError.
//
//nolint:gocritic // The SDK accepts a value snapshot; a pointer parameter would change its call and aliasing contracts.
func BasicAuthFromSecret(
	input store.ReferenceFacts,
	owner manifest.DiagnosticIdentity,
	secretRef string,
) (string, string, error) {
	valueResult, err := SecretValue(&input, new(owner), secretRef, "username")
	username := valueResult.Value()
	if err != nil {
		return "", "", fmt.Errorf("secret %s/%s: %w", owner.ID.Namespace, secretRef, err)
	}
	password, err := SecretValue(&input, new(owner), secretRef, "password")
	if err != nil {
		return "", "", fmt.Errorf("secret %s/%s: %w", owner.ID.Namespace, secretRef, err)
	}
	for _, result := range []values.Result{valueResult, password} {
		if result.State() != values.Readable {
			return "", "", UnavailableSecret(new(owner), secretRef, new(result))
		}
	}
	if username == "" || password.Value() == "" {
		return "", "", MissingSecretErr(owner, secretRef, "missing username/password")
	}
	return username, password.Value(), nil
}

// SecretLookup binds credential reads to one execution's captured inputs.
func SecretLookup(reads *store.ReadSet) SecretGetter {
	if reads == nil {
		return nil
	}
	provider := values.NewStoreProvider(reads)
	return func(namespace, name string) store.ReferenceFacts {
		return provider.Lookup(manifest.NamedResource{Kind: manifest.KindSecret, Namespace: namespace, Name: name})
	}
}
