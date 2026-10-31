package oci

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"

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

func (f *operation) acquire(ctx context.Context, access manifest.RegistryAccess, policy cache.Policy) (*store.SourceArtifact, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if f.secrets != nil {
		for _, ref := range []*manifest.LocalObjectReference{access.SecretRef, access.CertSecretRef, access.ProxySecretRef} {
			if ref != nil {
				f.secrets(access.Namespace, ref.Name)
			}
		}
	}
	policy.Timeout = &access.Timeout
	request, err := f.newRepoClient(ctx, access)
	machine := func(ctx context.Context) (*store.SourceArtifact, error) {
		machineRequest := request
		machineRequest.client = &remote.Repository{Reference: request.client.Reference}
		return source.AcquireMachine(ctx, policy, func(ctx context.Context) error {
			selected, err := f.Network.RegistryCredentials(ctx, f.RegistryConfig, machineRequest.client.Reference.Host())
			if err != nil {
				return err
			}
			return f.configure(access, &machineRequest, *selected)
		}, func(ctx context.Context, policy cache.Policy) (*store.SourceArtifact, error) {
			return f.acquirePrepared(ctx, access, machineRequest, policy)
		})
	}
	if missing, ok := errors.AsType[*source.MissingSecretError](err); ok && request.client != nil {
		missing.RetryAmbient = machine
	}
	if err != nil {
		return nil, err
	}
	if access.SecretRef == nil {
		return machine(ctx)
	}
	return f.acquirePrepared(ctx, access, request, policy)
}

func (f *operation) newRepoClient(ctx context.Context, access manifest.RegistryAccess) (registryAcquisition, error) {
	owner := access.Owner
	if access.URL == "" {
		return registryAcquisition{}, manifest.NewError(manifest.ErrInput, nil, "missing url")
	}
	reference, err := parseOCIRef(access.URL)
	if err != nil {
		return registryAcquisition{}, err
	}
	client, err := remote.NewRepository(reference)
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
	if _, err := resolved.ProxyURL(&url.URL{Scheme: "https", Host: client.Reference.Host()}); err != nil {
		return registryAcquisition{}, manifest.NewError(manifest.ErrInput, nil, "resolve registry proxy: invalid proxy configuration")
	}
	request := registryAcquisition{client: client, resolved: resolved}
	selected, err := f.resolveRegistryConfig(ctx, access, client.Reference.Host())
	if err == nil && *selected == auth.EmptyCredential && access.SecretRef != nil {
		err = source.MissingSecretErr(owner, access.SecretRef.Name, "no credentials for the source registry")
	}
	if err != nil {
		return request, err
	}
	err = f.configure(access, &request, *selected)
	return request, err
}

func (f *operation) configure(access manifest.RegistryAccess, request *registryAcquisition, cred auth.Credential) error {
	var material any
	request.anonymous = cred == auth.EmptyCredential
	if !request.anonymous {
		material = cred
	}
	request.material = request.resolved.Identity(material)
	tr, err := f.Network.HTTPTransport(source.HTTPTransportConfig{Material: request.resolved, Insecure: access.Insecure, Credentials: material})
	if err != nil {
		return err
	}
	request.client.Client = &auth.Client{Client: &http.Client{Transport: tr}, Cache: tr.(auth.Cache), Credential: auth.StaticCredential(request.client.Reference.Registry, cred)}
	return nil
}

func (f *operation) acquirePrepared(ctx context.Context, access manifest.RegistryAccess, request registryAcquisition, policy cache.Policy) (*store.SourceArtifact, error) {
	artifact, err := f.produce(ctx, request, policy)
	if access.Insecure && ctx.Err() == nil && errors.Is(err, http.ErrSchemeMismatch) {
		if requestErr, ok := errors.AsType[*url.Error](err); ok {
			target, parseErr := url.Parse(requestErr.URL)
			if parseErr == nil && target.Host == request.client.Reference.Host() {
				request.client.PlainHTTP = true
				artifact, err = f.produce(ctx, request, policy)
			}
		}
	}
	if f.allowAnonymousDenied && access.SecretRef == nil && request.anonymous && err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
		denied := errors.Is(err, auth.ErrBasicCredentialNotFound)
		if response, ok := errors.AsType[*fetcherr.ResponseError](err); ok {
			denied = denied || response.StatusCode == http.StatusUnauthorized || response.StatusCode == http.StatusForbidden
		}
		if denied {
			return nil, &source.MissingSecretError{Owner: access.Owner.ID, Subject: access.Owner, Detail: "no registry credentials available", AnonymousDenied: true, Cause: err}
		}
	}
	return artifact, err
}

// Machine credentials MUST wait for shared credential settlement to exhaust rendered Secrets.
func (f *operation) resolveRegistryConfig(ctx context.Context, access manifest.RegistryAccess, hostname string) (*auth.Credential, error) {
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
	configJSON, _, err := sec.Value(dockerConfigJSONKey)
	if err != nil {
		return nil, manifest.NewError(manifest.ErrInput, fmt.Errorf("read .dockerconfigjson: %w", err), "")
	}
	if configJSON == "" || manifest.HasValuePlaceholder(configJSON) {
		return nil, source.MissingSecretErr(access.Owner, access.SecretRef.Name, "missing .dockerconfigjson (must be type kubernetes.io/dockerconfigjson)")
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
