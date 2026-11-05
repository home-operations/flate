package oci

import (
	"errors"
	"strings"
	"testing"

	sourcev1 "github.com/fluxcd/source-controller/api/v1"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source"
)

func testFetcherSecretReferencesCase(t *testing.T, tt *struct {
	name        string
	secret      string
	wantText    string
	material    map[string]any
	cert        bool
	proxy       bool
	unwired     bool
	wantMissing bool
	wantOwner   bool
},
) {
	t.Helper()
	fInputs := testInputs{}
	f := &Fetcher{Network: newTestNetwork(t)}
	if !tt.unwired {
		fInputs.secrets = func(_, _ string) *manifest.Secret {
			if tt.material == nil {
				return nil
			}
			return &manifest.Secret{StringData: tt.material}
		}
	}
	repo := ociRepo("o", func(s *sourcev1.OCIRepositorySpec) {
		testFetcherSecretReferencesCaseCallback(s, testFetcherSecretReferencesCaseCallbackInputs{tt: &tt})
	},
	)
	if tt.wantOwner {
		repo.Name = "repo"
	}
	var err error
	switch {
	case tt.proxy:
		_, err = (&operation{Fetcher: f, secrets: fInputs.lookup(repo)}).resolvedRepository(
			t.Context(), new(repo.RegistryAccess()),
		)
	case tt.cert:
		_, err = (&operation{Fetcher: f, secrets: fInputs.lookup(repo)}).resolvedRepository(
			t.Context(), new(repo.RegistryAccess()),
		)
	default:
		_, err = (&operation{Fetcher: f, secrets: fInputs.lookup(repo)}).resolveRegistryConfig(
			t.Context(), new(repo.RegistryAccess()), "ghcr.io",
		)
	}
	assert.FatalIf(t, err == nil || errors.Is(err, manifest.ErrMissingSecret) != tt.wantMissing ||
		!strings.Contains(err.Error(), tt.wantText),

		"error = %v, want missing=%t containing %q",
		err, tt.
			wantMissing, tt.
			wantText)

	if tt.wantOwner {
		missing, ok := errors.AsType[*source.MissingSecretError](err)
		assert.FatalIf(t, !ok, "expected MissingSecretError; got %T", err)
		assert.Equal(
			t,
			missing.Owner,
			manifest.NamedResource{
				Group:     manifest.SourceDomain,
				Kind:      manifest.KindOCIRepository,
				Namespace: "ns",
				Name:      "repo",
			},
		)
		assert.Equal(
			t,
			missing.Secret,
			manifest.NamedResource{Kind: manifest.KindSecret, Namespace: "ns", Name: "missing"},
		)
		assert.Equal(t, err.Error(), "secret ns/missing not found")
	}
}

type testFetcherSecretReferencesCaseCallbackInputs struct {
	tt **struct {
		name   string
		secret string

		wantText    string
		material    map[string]any
		cert        bool
		proxy       bool
		unwired     bool
		wantMissing bool
		wantOwner   bool
	}
}

func testFetcherSecretReferencesCaseCallback(
	s *sourcev1.OCIRepositorySpec,
	testCaseInput testFetcherSecretReferencesCaseCallbackInputs,
) {
	s.URL = "oci://ghcr.io/x/y"
	ref := &manifest.LocalObjectReference{Name: (*testCaseInput.tt).secret}
	switch {
	case (*testCaseInput.tt).proxy:
		s.ProxySecretRef = ref
	case (*testCaseInput.tt).cert:
		s.CertSecretRef = ref
	default:
		s.SecretRef = ref
	}
}
