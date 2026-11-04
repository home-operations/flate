package oci

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	sourcev1 "github.com/fluxcd/source-controller/api/v1"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source"
	sourcecache "github.com/home-operations/flate/pkg/source/cache"
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
		_, err = (&operation{Fetcher: f, secrets: fInputs.lookup(repo)}).newRepoClient(
			t.Context(), new(repo.RegistryAccess()),
		)
	case tt.cert:
		_, err = (&operation{Fetcher: f, secrets: fInputs.lookup(repo)}).newRepoClient(
			t.Context(), new(repo.RegistryAccess()),
		)
	default:
		_, err = (&operation{Fetcher: f, secrets: fInputs.lookup(repo)}).resolveRegistryConfig(
			t.Context(), new(repo.RegistryAccess()), "ghcr.io",
		)
	}
	if err == nil || errors.Is(err, manifest.ErrMissingSecret) != tt.wantMissing ||
		!strings.Contains(err.Error(), tt.wantText) {
		t.Fatalf(
			"error = %v, want missing=%t containing %q",
			err, tt.
				wantMissing, tt.
				wantText,
		)
	}
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

func testFetcherSecretCredentialsAcquisitionPolicyCase(
	t *testing.T, tt *struct {
		name        string
		cert        bool
		proxy       bool
		encoded     bool
		material    map[string]any
		wantMissing bool
		wantInput   bool
		canceled    bool
	},
) {
	t.Helper()
	var requests atomic.Int64
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		requests.Add(1)
		w.WriteHeader(http.StatusInternalServerError)
	}))
	t.Cleanup(srv.Close)
	cache := cachetest.New(t, "", sourcecache.New)
	fInputs := testInputs{
		secrets: func(callbackArg1, name string) *manifest.Secret {
			return testFetcherSecretCredentialsAcquisitionPolicyCaseCallback(
				callbackArg1,
				name,
				testFetcherSecretCredentialsAcquisitionPolicyCaseCallbackInputs{tt: &tt},
			)
		},
	}
	f := &Fetcher{Network: newTestNetwork(t), Cache: cache}
	repo := ociRepo("o", func(s *sourcev1.OCIRepositorySpec) {
		s.URL, s.Insecure = "oci://"+mustURL(t, srv.URL).Host+"/app", true
		s.SecretRef = &manifest.LocalObjectReference{Name: "creds"}
		if tt.cert {
			s.CertSecretRef = &manifest.LocalObjectReference{Name: "tls"}
		}
		if tt.proxy {
			s.ProxySecretRef = &manifest.LocalObjectReference{Name: "proxy"}
		}
	})
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	if tt.canceled {
		cancel()
	}
	if _, err := f.Fetch(
		ctx,
		repo,
		fInputs.readSet(repo),
		sourcecache.Policy{MaxAge: repo.SourceInterval()},
	); err == nil || errors.Is(err, manifest.ErrMissingSecret) != tt.wantMissing ||
		errors.Is(err, context.Canceled) != tt.canceled {
		t.Fatalf(
			"Fetch error = %v, want missing=%t canceled=%t",
			err, tt.
				wantMissing, tt.
				canceled,
		)
	} else {
		assert.FatalIf(t, tt.wantInput &&
			!errors.Is(
				err,
				manifest.ErrInput,
			), "Fetch error = %v, want malformed material to be an input error", err)
	}
	{
		got := requests.Load()
		assert.FatalIf(t, got != 0, "registry requests = %d, want 0", got)
	}
}

type testFetcherSecretCredentialsAcquisitionPolicyCaseCallbackInputs struct {
	tt **struct {
		name string
		cert bool

		proxy       bool
		encoded     bool
		material    map[string]any
		wantMissing bool
		wantInput   bool
		canceled    bool
	}
}

func testFetcherSecretCredentialsAcquisitionPolicyCaseCallback(
	_, name string,
	testCaseInput testFetcherSecretCredentialsAcquisitionPolicyCaseCallbackInputs,
) *manifest.Secret {
	if name == "creds" &&
		((*testCaseInput.tt).cert || (*testCaseInput.tt).proxy || (*testCaseInput.tt).canceled) {
		return &manifest.Secret{
			StringData: map[string]any{
				".dockerconfigjson": `{"auths":{"ghcr.io":{"username":"alice","password":"hunter2"}}}`,
			},
		}
	}
	if (*testCaseInput.tt).material != nil {
		if (*testCaseInput.tt).encoded {
			return &manifest.Secret{Data: (*testCaseInput.tt).material}
		}
		return &manifest.Secret{StringData: (*testCaseInput.tt).material}
	}
	return nil
}
