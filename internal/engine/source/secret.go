package source

import (
	"context"
	"errors"
	"fmt"

	"github.com/home-operations/flate/internal/engine/source/cache"
	"github.com/home-operations/flate/internal/engine/store"
	"github.com/home-operations/flate/internal/engine/values"
	"github.com/home-operations/flate/pkg/manifest"
)

const secretReferenceUse = "secretRef"

// MissingSecretError retains unavailable source material and an optional Secret identity.
// Acquisition owners supply the ambient continuation.
type MissingSecretError struct {
	Input values.ResolveRequest
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

// RetryMachine MUST consume a settled authorization without repeating policy.
func (e *MissingSecretError) RetryMachine(ctx context.Context, assessment *values.Result) (PreparedFetch, error) {
	if err := ctx.Err(); err != nil {
		return PreparedFetch{}, fmt.Errorf("retry machine credentials: %w", err)
	}
	if e.RetryAmbient == nil || assessment == nil || assessment.Effect() != values.Deferred ||
		len(assessment.Pending()) != 0 {
		return PreparedFetch{}, e
	}
	prepared, err := e.RetryAmbient(ctx)
	if prepared.Fetch != nil {
		fetch := prepared.Fetch
		prepared.Fetch = func(ctx context.Context, local *store.SourceArtifact) (*store.SourceArtifact, error) {
			artifact, fetchErr := fetch(ctx, local)
			fetchErr = e.normalizeMachineError(fetchErr, prepared.LocalUnavailable != nil)
			return artifact, fetchErr
		}
	}
	err = e.normalizeMachineError(err, prepared.LocalUnavailable != nil)
	return prepared, err
}

func (e *MissingSecretError) normalizeMachineError(err error, localUnavailable bool) error {
	if !errors.Is(err, manifest.ErrMissingSecret) || errors.Is(err, context.Canceled) ||
		errors.Is(err, context.DeadlineExceeded) {
		return err
	}
	missing, typed := errors.AsType[*MissingSecretError](err)
	if !typed {
		if localUnavailable {
			return e
		}
		return err
	}
	if missing.Cause != nil && !missing.AnonymousDenied ||
		missing.AnonymousDenied && e.Secret == (manifest.NamedResource{}) {
		return err
	}
	return e
}

// AcquireMachine bounds preparation independently of deferred content acquisition.
func AcquireMachine(
	ctx context.Context,
	policyValue *cache.Policy,
	prepare func(context.Context) error,
	acquire func(context.Context, *cache.Policy, *store.SourceArtifact) (*store.SourceArtifact, error),
) (PreparedFetch, error) {
	policy := *policyValue
	classifyPreparation := func(err error) error {
		if err != nil && !errors.Is(err, manifest.ErrMissingSecret) {
			if _, cached := errors.AsType[*cache.Error](err); !cached {
				if !errors.Is(err, manifest.ErrFlux) {
					err = manifest.NewError(manifest.ErrFlux, err, "")
				}
				err = cache.MachineCredentialErr(err)
			}
		}
		return err
	}
	if err := ctx.Err(); err != nil {
		return PreparedFetch{}, classifyPreparation(err)
	}
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
		return PreparedFetch{}, classifyPreparation(err)
	}
	return PreparedFetch{Fetch: func(ctx context.Context, local *store.SourceArtifact) (*store.SourceArtifact, error) {
		artifact, fetchErr := acquire(ctx, &policy, local)
		if fetchErr != nil && !errors.Is(fetchErr, manifest.ErrFlux) {
			fetchErr = manifest.NewError(manifest.ErrFlux, fetchErr, "")
		}
		return artifact, fetchErr
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
	missing.Input.Reference = manifest.Reference{NamedResource: missing.Secret, Use: secretReferenceUse}
	return missing
}

// ResolveSecretRef MUST retain authored absence without inventing producer evidence.
func ResolveSecretRef(
	secrets SecretGetter,
	ownerValue *manifest.DiagnosticIdentity,
	ref *manifest.LocalObjectReference,
) store.ReferenceFacts {
	owner := *ownerValue
	if secrets == nil || ref == nil {
		return store.ReferenceFacts{}
	}
	return secrets(owner.ID.Namespace, ref.Name)
}

// SecretValue MUST resolve each consumed field from the same captured facts.
func SecretValue(
	input *store.ReferenceFacts,
	owner *manifest.DiagnosticIdentity,
	secretRef, key string,
) (values.Result, error) {
	result := values.Resolve(&values.ResolveRequest{
		Reference: manifest.Reference{
			Kind:      manifest.KindSecret,
			Namespace: owner.ID.Namespace,
			Name:      secretRef,
			Key:       key,
			Use:       secretReferenceUse,
		},
		Facts:  *input,
		Demand: values.SourceDemand,
	})
	err := result.MaterialError()
	return result, err
}

// UnavailableSecret MUST retain acquisition retry and whole-source policy.
func UnavailableSecret(
	owner *manifest.DiagnosticIdentity,
	input *store.ReferenceFacts,
	result *values.Result,
) *MissingSecretError {
	missing := &MissingSecretError{
		Owner:   owner.ID,
		Subject: *owner,
		Secret:  result.ID(),
		Detail:  result.Cause(),
		Input: values.ResolveRequest{
			Reference: result.Reference(),
			Facts:     *input,
			Demand:    values.SourceDemand,
		},
	}
	return missing
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
		if result.Effect() != values.Used {
			return "", "", UnavailableSecret(new(owner), &input, new(result))
		}
	}
	if username == "" || password.Value() == "" {
		missing := UnavailableSecret(&owner, &input, &valueResult)
		missing.Detail = "missing username/password"
		return "", "", missing
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
