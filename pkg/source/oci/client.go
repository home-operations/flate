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

	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source"
	"github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/store"
)

const dockerConfigJSONKey = ".dockerconfigjson"

func (f *operation) newRepoClient(ctx context.Context, repo *manifest.OCIRepository, policy cache.Policy) (registryAcquisition, error) {
	owner := manifest.DiagnosticFor(repo.Named(), repo)
	reference, err := parseOCIRef(repo.URL)
	if err != nil {
		return registryAcquisition{}, err
	}
	client, err := remote.NewRepository(reference)
	if err != nil {
		return registryAcquisition{}, fmt.Errorf("oras: %w", err)
	}
	resolved, err := source.ResolveTransport(f.secrets, owner, []source.TransportInput{
		{Reference: repo.CertSecretRef, Use: source.TransportCert},
		{Reference: repo.ProxySecretRef, Use: source.TransportProxy},
	})
	if err != nil {
		return registryAcquisition{}, err
	}
	if _, err := resolved.ProxyURL(&url.URL{Scheme: "https", Host: client.Reference.Host()}); err != nil {
		return registryAcquisition{}, manifest.NewError(manifest.ErrInput, nil, "resolve registry proxy: invalid proxy configuration")
	}
	request := registryAcquisition{client: client, resolved: resolved}

	selected, err := f.resolveRegistryConfig(ctx, repo, client.Reference.Host())
	if err == nil && *selected == auth.EmptyCredential && repo.SecretRef != nil {
		err = source.MissingSecretErr(owner, repo.SecretRef.Name, "no credentials for the source registry")
	}
	if missing, ok := errors.AsType[*source.MissingSecretError](err); ok {
		missing.RetryAmbient = func(ctx context.Context) (*store.SourceArtifact, error) {
			return f.machineFetch(ctx, repo, request, policy)
		}
	}
	if err != nil {
		return registryAcquisition{}, err
	}

	err = f.configure(repo, &request, *selected)
	return request, err
}

func (f *operation) configure(repo *manifest.OCIRepository, request *registryAcquisition, cred auth.Credential) error {
	var material any
	if cred != auth.EmptyCredential {
		material = cred
	}
	request.material = request.resolved.Identity(material)
	tr, err := f.Network.HTTPTransport(source.HTTPTransportConfig{Material: request.resolved, Insecure: repo.Insecure, Credentials: material})
	if err != nil {
		return err
	}
	request.client.Client = &auth.Client{Client: &http.Client{Transport: tr}, Cache: tr.(auth.Cache), Credential: auth.StaticCredential(request.client.Reference.Registry, cred)}
	return nil
}

func (f *operation) machineFetch(ctx context.Context, repo *manifest.OCIRepository, request registryAcquisition, policy cache.Policy) (*store.SourceArtifact, error) {
	timeout := repo.OperationTimeout()
	policy.Timeout = &timeout
	request.client = &remote.Repository{Reference: request.client.Reference}
	return source.AcquireMachine(ctx, policy, func(ctx context.Context) error {
		selected, err := f.Network.RegistryCredentials(ctx, f.RegistryConfig, request.client.Reference.Host())
		if err != nil {
			return err
		}
		return f.configure(repo, &request, *selected)
	}, func(ctx context.Context, policy cache.Policy) (*store.SourceArtifact, error) {
		return f.fetchPrepared(ctx, repo, request, policy)
	})
}

// Machine credentials MUST wait for the source controller to exhaust rendered Secrets.
func (f *operation) resolveRegistryConfig(ctx context.Context, repo *manifest.OCIRepository, hostname string) (*auth.Credential, error) {
	if repo.SecretRef == nil {
		return &auth.Credential{}, nil
	}
	if f.secrets == nil {
		return nil, errors.New("secretRef requires source.SecretGetter")
	}
	sec := f.secrets(repo.Namespace, repo.SecretRef.Name)
	if sec == nil {
		return nil, source.MissingSecretErr(manifest.DiagnosticFor(repo.Named(), repo), repo.SecretRef.Name, "not found")
	}
	configJSON, _, err := sec.Value(dockerConfigJSONKey)
	if err != nil {
		return nil, manifest.NewError(manifest.ErrInput, fmt.Errorf("read .dockerconfigjson: %w", err), "")
	}
	if configJSON == "" || manifest.HasValuePlaceholder(configJSON) {
		return nil, source.MissingSecretErr(manifest.DiagnosticFor(repo.Named(), repo), repo.SecretRef.Name, "missing .dockerconfigjson (must be type kubernetes.io/dockerconfigjson)")
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
