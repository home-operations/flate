package git

import (
	"errors"
	"strings"
	"testing"

	"github.com/go-git/go-git/v5"
	githttp "github.com/go-git/go-git/v5/plumbing/transport/http"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source"
	sourcecache "github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/store"
)

func testFetcherResolveTransportSecretMaterialCase(
	t *testing.T, ca *string,
	tc *struct {
		name        string
		errorKey    string
		data        map[string]any
		encodedData map[string]any
		wantInput   bool
		wantMissing bool
		wantBearer  bool
		proxyRead   bool
	},
) {
	t.Helper()

	sec := &manifest.Secret{
		Name:       "different",
		Namespace:  "elsewhere",
		StringData: tc.data,
		Data:       tc.encodedData,
	}
	s := store.New()
	captured := *sec
	captured.Name, captured.Namespace = "creds", "ns"
	s.AddObject(&captured)
	s.AddObject(
		&manifest.Secret{
			Name:       "proxy",
			Namespace:  "ns",
			StringData: map[string]any{"address": "http://proxy.invalid/%"},
		},
	)

	f := &Fetcher{}
	repo := gitRepoWithSecret("g", "https://host/repo", "creds")
	if tc.wantInput || tc.wantMissing {
		repo.ProxySecretRef = &manifest.LocalObjectReference{Name: "proxy"}
	}
	var reads store.ReadSet
	reads.Init(s, repo.Named(), nil)
	request, err := (&operation{Fetcher: f, reads: &reads, secrets: source.SecretLookup(&reads)}).resolveTransport(
		repo,
		sourcecache.Policy{},
	)
	s.AddObject(
		&manifest.Secret{
			Name:       "proxy",
			Namespace:  "ns",
			StringData: map[string]any{"address": "http://changed.invalid"},
		},
	)
	assert.Equal(t, reads.Valid(), !tc.proxyRead)
	s.AddObject(&manifest.Secret{Name: "creds", Namespace: "ns"})
	assert.Equal(t, reads.Valid(), false)
	if tc.wantInput || tc.wantMissing {
		assert.Equal(t, request.spec == nil, true)
		assert.Equal(t, request.material == nil, true)
		assert.Diff(t, request.options, git.CloneOptions{})
		assert.Equal(t, errors.Is(err, manifest.ErrInput), tc.wantInput)
		assert.Equal(t, errors.Is(err, manifest.ErrFlux), true)
		assert.Equal(t, errors.Is(err, manifest.ErrMissingSecret), tc.wantMissing)
		secret, owner := "ns/creds", manifest.DiagnosticFor(repo.Named(), repo).Label()
		if tc.proxyRead {
			secret, owner = "ns/proxy", "GitRepository ns/g"
		}
		assert.FatalIf(t, err == nil || !strings.Contains(err.Error(), tc.errorKey) ||
			!strings.Contains(err.Error(), secret) ||
			strings.Contains(err.Error(), owner), "wrong selected error context: %v", err)
		assert.FatalIf(
			t,
			strings.Contains(err.Error(), "%%%fixture") || strings.Contains(err.Error(), "credential-fixture"),
			"%s",
			"credential payload leaked",
		)

		if tc.wantMissing {
			missing, ok := errors.AsType[*source.MissingSecretError](err)
			assert.FatalIf(t, !ok, "expected MissingSecretError; got %v", err)
			assert.Equal(t, missing.Owner, repo.Named())
			assert.Equal(
				t,
				missing.Secret,
				manifest.NamedResource{Kind: manifest.KindSecret, Namespace: "ns", Name: "creds"},
			)
		}
		return
	}
	assert.NoError(t, err)
	assert.Equal(t, request.spec, repo)
	assert.Equal(t, string(request.options.CABundle), (*ca))
	assert.Equal(t, request.material != nil, true)
	if tc.wantBearer {
		auth, ok := request.options.Auth.(*githttp.TokenAuth)
		if !ok {
			t.Fatalf("expected token auth; got %T", request.options.Auth)
		}
		assert.Equal(t, auth.Token, "fixture-token")
	} else {
		auth, ok := request.options.Auth.(*githttp.BasicAuth)
		if !ok {
			t.Fatalf("expected basic auth; got %T", request.options.Auth)
		}
		assert.Equal(t, auth.Username, "u")
		assert.Equal(t, auth.Password, "p")
	}
}
