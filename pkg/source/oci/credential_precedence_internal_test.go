package oci

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	sourcev1 "github.com/fluxcd/source-controller/api/v1"
	"github.com/google/go-cmp/cmp"
	"oras.land/oras-go/v2/registry/remote/auth"
	"oras.land/oras-go/v2/registry/remote/credentials"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
)

func TestFetcher_ResolveConfig_CredentialSnapshot(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "config.json")
	write := func(password string) {
		t.Helper()
		testutil.WriteFileAt(
			t,
			path,
			fmt.Sprintf(`{"auths":{"https://index.docker.io/v1/":{"username":"fixture","password":%q}}}`, password),
		)
	}
	write("first")
	op := &operation{Fetcher: &Fetcher{Network: newTestNetwork(t), RegistryConfig: path}}
	repo := ociRepo("snapshot", func(s *sourcev1.OCIRepositorySpec) { s.URL = "oci://docker.io/library/test" })
	selected, err := newTestNetwork(t).RegistryCredentials(t.Context(), path, "registry-1.docker.io")
	assert.NoError(t, err)
	request, err := op.newRepoClient(t.Context(), new(repo.RegistryAccess()))
	if err == nil {
		err = op.configure(new(repo.RegistryAccess()), &request, *selected)
	}
	assert.NoError(t, err)
	write("second")
	cred, err := request.client.Client.(*auth.Client).Credential(t.Context(), "registry-1.docker.io")
	assert.FatalIf(t, err != nil || cred.Password != "first", "credential was not frozen: %v", err)
	selected, err = newTestNetwork(t).RegistryCredentials(t.Context(), path, "registry-1.docker.io")
	assert.NoError(t, err)
	next, err := op.newRepoClient(t.Context(), new(repo.RegistryAccess()))
	if err == nil {
		err = op.configure(new(repo.RegistryAccess()), &next, *selected)
	}
	assert.FatalIf(
		t,
		err != nil || cmp.Equal(request.material, next.material),
		"next acquisition did not resolve rotated credential: %v",
		err,
	)
	testutil.WriteFileAt(t, path, `{"auths":{"https://index.docker.io/v1/":{"auth":"!!!"}}}`)
	_, err = newTestNetwork(t).RegistryCredentials(t.Context(), path, "registry-1.docker.io")
	assert.FatalIf(t, err == nil, "failed lookup admitted acquisition: %v", err)
	assert.FatalIf(
		t,
		!strings.Contains(err.Error(), "failed to decode auth field"),
		"credential decode detail lost: %v",
		err,
	)
	op.RegistryConfig = filepath.Join(path, "config.json")
	_, err = newTestNetwork(t).RegistryCredentials(t.Context(), op.RegistryConfig, "registry-1.docker.io")
	{
		_, ok := errors.AsType[*os.PathError](err)
		assert.FatalIf(t, !ok, "file lookup error chain lost: %v", err)
	}
}

func testFetcherResolveConfigCredentialPrecedenceCase(
	t *testing.T, tt *struct {
		name     string
		registry string
		secret   string
		config   string
		user     string
		pass     string
	},
) {
	t.Helper()

	dockerDir := t.TempDir()
	t.Setenv("DOCKER_CONFIG", dockerDir)

	t.Setenv("HELM_REGISTRY_CONFIG", filepath.Join(t.TempDir(), "absent.json"))
	testutil.WriteFile(
		t,
		dockerDir,
		"config.json",
		`{"auths":{"ghcr.io":{"username":"default","password":"default"}}}`,
	)
	fInputs := testInputs{}
	f := &Fetcher{Network: newTestNetwork(t)}
	if tt.config != "" {
		f.RegistryConfig = filepath.Join(t.TempDir(), "config.json")
		testutil.WriteFileAt(t, f.RegistryConfig, tt.config)
	}
	fInputs.secrets = func(_, _ string) *manifest.Secret {
		return &manifest.Secret{StringData: map[string]any{".dockerconfigjson": tt.secret}}
	}
	repo := ociRepo("o", func(s *sourcev1.OCIRepositorySpec) {
		s.URL = "oci://" + tt.registry + "/x/y"
		s.Provider = sourcev1.AmazonOCIProvider
		if tt.secret != "" {
			s.SecretRef = &manifest.LocalObjectReference{Name: "creds"}
		}
	})
	op := &operation{Fetcher: f, secrets: fInputs.lookup(repo)}
	var selected *auth.Credential
	var err error
	if repo.SecretRef == nil {
		selected, err = newTestNetwork(
			t,
		).RegistryCredentials(t.Context(), f.RegistryConfig, tt.registry)
		assert.NoError(t, err)
	}
	request, err := op.newRepoClient(t.Context(), new(repo.RegistryAccess()))
	if err == nil && selected != nil {
		err = op.configure(new(repo.RegistryAccess()), &request, *selected)
	}
	assert.NoError(t, err)

	got, err := request.client.Client.(*auth.Client).Credential(t.Context(), request.client.Reference.Host())
	want := auth.Credential{Username: tt.user, Password: tt.pass}
	assert.FatalIf(t, err != nil || got != want, "credentials = %+v, %v; want %+v", got, err, want)
	assert.FatalIf(t, request.material == nil, "%s", "authenticated acquisition has anonymous identity")

	if tt.secret != "" {
		op.secrets = func(_, _ string) *manifest.Secret {
			config := fmt.Sprintf(
				`{"auths":{%q:{"username":"rotated","password":"rotated"}}}`,
				credentials.ServerAddressFromRegistry(tt.registry),
			)
			return &manifest.Secret{StringData: map[string]any{".dockerconfigjson": config}}
		}
		captured, err := request.client.Client.(*auth.Client).Credential(
			t.Context(),
			request.client.Reference.Host(),
		)
		assert.FatalIf(t, err != nil || captured != want, "%s", "producer reread mutable credentials")

		rotated, err := op.newRepoClient(t.Context(), new(repo.RegistryAccess()))
		assert.FatalIf(
			t,
			err != nil || cmp.Equal(request.material, rotated.material),
			"%s",
			"credential rotation reused identity",
		)
	}
}
