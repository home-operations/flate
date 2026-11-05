package oci

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"

	"oras.land/oras-go/v2/registry"
	"oras.land/oras-go/v2/registry/remote"
	"oras.land/oras-go/v2/registry/remote/auth"
	"oras.land/oras-go/v2/registry/remote/credentials"

	"github.com/home-operations/flate/internal/fetcherr"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source"
	"github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/store"
)

const dockerConfigJSONKey = ".dockerconfigjson"

func (f *operation) acquire(
	ctx context.Context,
	accessValue *manifest.RegistryAccess,
	policyValue *cache.Policy,
) (source.PreparedFetch, error) {
	access := *accessValue

	policy := *policyValue

	f.work.Scope = f.reads.Work().Scope().Source(access.Owner.ID)
	if policy.Work != nil {
		f.work.Scope = policy.Work.Attribution().Source(access.Owner.ID)
	}
	policy.Work = &f.work

	if err := ctx.Err(); err != nil {
		return source.PreparedFetch{}, err //nolint:wrapcheck // Cancellation already identifies its cause; another operation prefix would add no resource information.
	}
	if f.secrets != nil {
		for _, ref := range []*manifest.LocalObjectReference{access.SecretRef, access.CertSecretRef, access.ProxySecretRef} {
			if ref != nil {
				f.secrets(access.Namespace, ref.Name)
			}
		}
	}
	policy.Timeout = &access.Timeout
	request, err := f.newRepoClient(ctx, new(access))
	machine := f.machineAcquisition(&access, &request, &policy, err)
	if missing, ok := errors.AsType[*source.MissingSecretError](err); ok && request.reference.Registry != "" {
		missing.RetryAmbient = machine
	}
	if err != nil {
		return source.PreparedFetch{}, err
	}
	if access.SecretRef == nil {
		return machine(ctx)
	}
	return source.PreparedFetch{
		Fetch: func(ctx context.Context, local *store.SourceArtifact) (*store.SourceArtifact, error) {
			if local != nil {
				return local, ctx.Err()
			}
			return f.acquirePrepared(ctx, &access, &request, &policy)
		},
	}, nil
}

func (f *operation) machineAcquisition(
	accessValue *manifest.RegistryAccess,
	requestValue *registryAcquisition,
	policyValue *cache.Policy,
	unavailable error,
) func(context.Context) (source.PreparedFetch, error) {
	access := *accessValue
	request := *requestValue
	policy := *policyValue
	return func(ctx context.Context) (source.PreparedFetch, error) {
		machineRequest := struct {
			registryAcquisition

			work manifest.AcquisitionWork
		}{registryAcquisition: request}
		machineRequest.work.Scope = f.work.Scope
		machinePolicy := policy
		machinePolicy.Work = &machineRequest.work
		prepared, machineErr := source.AcquireMachine(ctx, &machinePolicy, func(ctx context.Context) error {
			selected, credentialsErr := f.Network.RegistryCredentials(
				ctx,
				f.RegistryConfig,
				machineRequest.reference.Host(),
			)
			if credentialsErr != nil {
				return credentialsErr
			}

			machineRequest.credential = *selected
			return nil
		}, func(ctx context.Context, policyValue *cache.Policy, local *store.SourceArtifact) (*store.SourceArtifact, error) {
			acquisitionPolicy := *policyValue
			if local != nil {
				return local, ctx.Err()
			}
			return f.acquirePrepared(ctx, &access, &machineRequest.registryAcquisition, &acquisitionPolicy)
		})
		if machineErr == nil && machineRequest.credential == auth.EmptyCredential {
			prepared.LocalUnavailable, _ = errors.AsType[*source.MissingSecretError](unavailable)
		}
		return prepared, machineErr
	}
}

func (f *operation) newRepoClient(
	ctx context.Context,
	accessValue *manifest.RegistryAccess,
) (registryAcquisition, error) {
	access := *accessValue
	owner := access.Owner
	if access.URL == "" {
		return registryAcquisition{}, manifest.NewError(manifest.ErrInput, nil, "missing url")
	}
	reference, err := parseOCIRef(access.URL)
	if err != nil {
		return registryAcquisition{}, err
	}
	parsed, err := registry.ParseReference(reference)
	if err != nil {
		return registryAcquisition{}, fmt.Errorf("oras: %w", err)
	}
	resolved, err := source.ResolveTransport(f.secrets, owner, []source.TransportInput{
		{Reference: access.CertSecretRef, Use: source.TransportCert},
		{Reference: access.ProxySecretRef, Use: source.TransportProxy},
	})
	if err != nil {
		return registryAcquisition{}, err
	}
	if _, proxyURLErr := resolved.ProxyURL(
		&url.URL{Scheme: "https", Host: parsed.Host()},
	); proxyURLErr != nil {
		return registryAcquisition{}, manifest.NewError(
			manifest.ErrInput,
			nil,
			"resolve registry proxy: invalid proxy configuration",
		)
	}
	request := registryAcquisition{reference: parsed, resolved: resolved}
	selected, err := f.resolveRegistryConfig(ctx, new(access), parsed.Host())
	if err == nil && *selected == auth.EmptyCredential && access.SecretRef != nil {
		err = source.MissingSecretErr(owner, access.SecretRef.Name, "no credentials for the source registry")
	}
	if err != nil {
		return request, err
	}
	request.credential = *selected
	return request, nil
}

func (f *operation) configure(
	accessValue *manifest.RegistryAccess,
	request *registryAcquisition,
	cred auth.Credential,
) error {
	access := *accessValue
	var material any
	request.anonymous = cred == auth.EmptyCredential
	if !request.anonymous {
		material = cred
	}
	request.material = request.resolved.Identity(material)
	request.client = &remote.Repository{Reference: request.reference}
	tr, err := f.Network.HTTPTransport(
		source.HTTPTransportConfig{Material: request.resolved, Insecure: access.Insecure, Credentials: material},
	)
	if err != nil {
		return err
	}
	request.client.Client = &auth.Client{
		Client:     &http.Client{Transport: tr},
		Cache:      tr.(auth.Cache), //nolint:errcheck // The owner stores this exact concrete type; a mismatched assertion must retain its panic.
		Credential: auth.StaticCredential(request.client.Reference.Registry, cred),
	}
	return nil
}

func (f *operation) acquirePrepared(
	ctx context.Context, accessValue *manifest.RegistryAccess, requestValue *registryAcquisition,
	policyValue *cache.Policy,
) (*store.SourceArtifact, error) {
	policy := *policyValue

	access := *accessValue
	request := *requestValue
	if err := f.configure(&access, &request, request.credential); err != nil {
		return nil, err
	}
	artifact, err := f.produce(ctx, request, &policy)
	if access.Insecure && ctx.Err() == nil && errors.Is(err, http.ErrSchemeMismatch) {
		if requestErr, ok := errors.AsType[*url.Error](err); ok {
			target, parseErr := url.Parse(requestErr.URL)
			if parseErr == nil && target.Host == request.client.Reference.Host() {
				request.client.PlainHTTP = true
				artifact, err = f.produce(ctx, request, &policy)
			}
		}
	}
	if f.allowAnonymousDenied && access.SecretRef == nil && request.anonymous && err != nil &&
		!errors.Is(err, context.Canceled) &&
		!errors.Is(err, context.DeadlineExceeded) {
		denied := errors.Is(err, auth.ErrBasicCredentialNotFound)
		if response, ok := errors.AsType[*fetcherr.ResponseError](err); ok {
			denied = denied || response.StatusCode == http.StatusUnauthorized ||
				response.StatusCode == http.StatusForbidden
		}
		if denied {
			return nil, &source.MissingSecretError{
				Owner:           access.Owner.ID,
				Subject:         access.Owner,
				Detail:          "no registry credentials available",
				AnonymousDenied: true,
				Cause:           err,
			}
		}
	}
	return artifact, err
}

// Machine credentials MUST wait for shared credential settlement to exhaust rendered Secrets.
func (f *operation) resolveRegistryConfig(
	ctx context.Context,
	accessValue *manifest.RegistryAccess,
	hostname string,
) (*auth.Credential, error) {
	access := *accessValue
	if access.SecretRef == nil {
		return &auth.Credential{}, nil
	}
	if f.secrets == nil {
		return nil, errors.New("secretRef requires source.SecretGetter")
	}
	sec := f.secrets(access.Namespace, access.SecretRef.Name)
	if sec == nil {
		return nil, source.MissingSecretErr(access.Owner, access.SecretRef.Name, "not found")
	}
	valueResult, err := sec.Value(dockerConfigJSONKey)
	configJSON := valueResult.Value
	if err != nil {
		return nil, manifest.NewError(manifest.ErrInput, fmt.Errorf("read .dockerconfigjson: %w", err), "")
	}
	if configJSON == "" {
		return nil, source.MissingSecretErr(
			access.Owner,
			access.SecretRef.Name,
			"missing .dockerconfigjson (must be type kubernetes.io/dockerconfigjson)",
		)
	}
	credStore, err := credentials.NewMemoryStoreFromDockerConfig([]byte(configJSON))
	if err != nil {
		return nil, manifest.NewError(manifest.ErrInput, fmt.Errorf("parse .dockerconfigjson: %w", err), "")
	}
	// ORAS normalizes Docker Hub's credential key while its callback expects the
	// canonical Docker server address. Reconcile these in the same memory store.
	if cred, _ := credStore.Get(ctx, "index.docker.io"); cred != auth.EmptyCredential {
		_ = credStore.Put(ctx, credentials.ServerAddressFromRegistry("docker.io"), cred)
	}
	cred, err := credentials.Credential(credStore)(ctx, hostname)
	if err != nil {
		return nil, manifest.NewError(manifest.ErrInput, fmt.Errorf("resolve registry credentials: %w", err), "")
	}
	return &cred, nil
}
