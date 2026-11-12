package git

import (
	"context"
	"crypto/ed25519"
	"encoding/base64"
	"encoding/pem"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/home-operations/flate/pkg/store"

	"github.com/home-operations/flate/internal/testutil/fetchtest"

	sourcev1 "github.com/fluxcd/source-controller/api/v1"
	"github.com/go-git/go-git/v5/plumbing/transport"
	githttp "github.com/go-git/go-git/v5/plumbing/transport/http"
	gitssh "github.com/go-git/go-git/v5/plumbing/transport/ssh"
	"golang.org/x/crypto/ssh"
	"golang.org/x/crypto/ssh/knownhosts"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source"
	sourcecache "github.com/home-operations/flate/pkg/source/cache"
)

func TestFetcher_SecretReferences(t *testing.T) {
	t.Parallel()
	public, key, err := ed25519.GenerateKey(nil)
	assert.NoError(t, err)
	block, err := ssh.MarshalPrivateKey(key, "fixture")
	assert.NoError(t, err)
	identity := string(pem.EncodeToMemory(block))
	badPEM := string(pem.EncodeToMemory(&pem.Block{Type: "credential-fixture", Bytes: []byte("fixture")}))
	encryptedBlock, err := ssh.MarshalPrivateKeyWithPassphrase(key, "fixture", []byte("fixture-password"))
	assert.NoError(t, err)
	encryptedIdentity := string(pem.EncodeToMemory(encryptedBlock))
	hostKey, err := ssh.NewPublicKey(public)
	assert.NoError(t, err)

	cases := []gitSecretCase{
		{name: "missing proxy", proxy: true, wantMissing: true},
		{name: "missing auth", wantMissing: true},
		{name: "incomplete basic", data: map[string]any{"username": "alice"}, wantMissing: true},
		{name: "unwired getter", unwired: true, wantError: "SecretGetter"},
		{name: "empty bearer uses basic", data: map[string]any{"bearerToken": "", "username": "u", "password": "p"}},
		{
			name:        "empty known hosts",
			ssh:         true,
			data:        map[string]any{"identity": identity, "password": "", "known_hosts": ""},
			wantMissing: true,
		},
		{
			name: "valid known hosts",
			ssh:  true,
			data: map[string]any{
				"identity":    identity,
				"known_hosts": "host " + string(ssh.MarshalAuthorizedKey(hostKey)),
			},
		},
		{
			name:      "invalid identity",
			ssh:       true,
			data:      map[string]any{"identity": "invalid-private-fixture"},
			wantInput: true,
			wantKey:   "identity",
		},
		{
			name:      "invalid known hosts",
			ssh:       true,
			data:      map[string]any{"identity": identity, "known_hosts": "invalid-known-hosts-fixture"},
			wantInput: true,
			wantKey:   "known_hosts",
		},
		{
			name:        "empty identity malformed password",
			ssh:         true,
			data:        map[string]any{"identity": ""},
			encodedData: map[string]any{"password": "%%%fixture"},
			wantInput:   true,
			wantKey:     "password",
		},
		{
			name:        "missing identity malformed known hosts",
			ssh:         true,
			encodedData: map[string]any{"known_hosts": "%%%fixture"},
			wantInput:   true,
			wantKey:     "known_hosts",
		},
		{
			name:        "empty password malformed known hosts",
			ssh:         true,
			data:        map[string]any{"identity": identity, "password": ""},
			encodedData: map[string]any{"known_hosts": "%%%fixture"},
			wantInput:   true,
			wantKey:     "known_hosts",
		},
		{
			name:        "malformed identity empty password",
			ssh:         true,
			data:        map[string]any{"password": ""},
			encodedData: map[string]any{"identity": "%%%fixture"},
			wantInput:   true,
			wantKey:     "identity",
		},
		{
			name:      "identity PEM label sanitized",
			ssh:       true,
			data:      map[string]any{"identity": badPEM, "known_hosts": ""},
			wantInput: true,
			wantKey:   "identity",
		},
		{
			name:      "host marker sanitized",
			ssh:       true,
			data:      map[string]any{"identity": "", "known_hosts": "@credential-fixture host ignored"},
			wantInput: true,
			wantKey:   "known_hosts",
		},
		{
			name:      "invalid identity empty hosts",
			ssh:       true,
			data:      map[string]any{"identity": "invalid-private-fixture", "known_hosts": ""},
			wantInput: true,
			wantKey:   "identity",
		},
		{
			name:      "invalid identity empty password",
			ssh:       true,
			data:      map[string]any{"identity": "invalid-private-fixture", "password": ""},
			wantInput: true,
			wantKey:   "identity",
		},
		{
			name:      "encrypted identity empty password",
			ssh:       true,
			data:      map[string]any{"identity": encryptedIdentity, "password": ""},
			wantInput: true,
			wantKey:   "identity",
		},
		{
			name: "encrypted identity correct password",
			ssh:  true,
			data: map[string]any{
				"identity": encryptedIdentity, "password": "fixture-password",
				"known_hosts": "host " + string(ssh.MarshalAuthorizedKey(hostKey)),
			},
		},
		{
			name:      "encrypted identity wrong password",
			ssh:       true,
			data:      map[string]any{"identity": encryptedIdentity, "password": "wrong-password"},
			wantInput: true,
			wantKey:   "identity",
		},
		{
			name:      "absent identity invalid hosts",
			ssh:       true,
			data:      map[string]any{"known_hosts": "invalid-known-hosts-fixture"},
			wantInput: true,
			wantKey:   "known_hosts",
		},
		{
			name:      "empty identity invalid hosts",
			ssh:       true,
			data:      map[string]any{"identity": "", "known_hosts": "invalid-known-hosts-fixture"},
			wantInput: true,
			wantKey:   "known_hosts",
		},
		{
			name: "encrypted identity empty password invalid hosts",
			ssh:  true,
			data: map[string]any{
				"identity":    encryptedIdentity,
				"password":    "",
				"known_hosts": "invalid-known-hosts-fixture",
			},
			wantInput: true,
			wantKey:   "identity",
		},
		{
			name: "plain identity empty password invalid hosts",
			ssh:  true,
			data: map[string]any{
				"identity":    identity,
				"password":    "",
				"known_hosts": "invalid-known-hosts-fixture",
			},
			wantInput: true,
			wantKey:   "known_hosts",
		},
		{
			name:        "malformed raw data",
			encodedData: map[string]any{"bearerToken": "%%%fixture"},
			wantInput:   true,
			wantKey:     "bearerToken",
		},
	}
	knownHosts := "host " + string(ssh.MarshalAuthorizedKey(hostKey))
	testFetcherSecretReferencesTail(
		t,
		&testFetcherSecretReferencesTailInputs{cases: &cases, identity: &identity, knownHosts: &knownHosts},
	)
}

func TestFetcher_ProviderUnavailable(t *testing.T) {
	t.Parallel()
	f := &Fetcher{}
	repo := &manifest.GitRepository{
		Name: "g", Namespace: "ns",
		URL:      "https://github.com/x/y.git",
		Provider: sourcev1.GitProviderGitHub,
	}
	_, err := fetchtest.Fetch(
		context.Background(),
		f.Fetch,
		repo,
		(testInputs{}).readSet(repo),
		&sourcecache.Policy{MaxAge: repo.SourceInterval()},
		nil,
	)
	assert.FatalIf(t, err == nil, "expected error for unimplemented provider")
	assert.ErrorIf(t, !errors.Is(err, manifest.ErrMissingSecret), "expected missing provider credentials; got %v", err)
}

func TestFetcher_HTTPSBasicAuth(t *testing.T) {
	t.Parallel()
	fInputs := testInputs{secrets: func(_, _ string) store.ReferenceFacts {
		secretInput := &manifest.Secret{
			StringData: map[string]any{
				"username": "alice",
				"password": "hunter2",
			},
		}

		return store.ReferenceFacts{Data: secretInput, Original: secretInput}
	}}
	f := &Fetcher{}
	for _, url := range []string{
		"https://github.com/x/y.git",
		"http://example.com/x@y",
		"./a@b",
	} {
		t.Run(url, func(t *testing.T) {
			t.Parallel()
			auth, err := (&operation{Fetcher: f, secrets: fInputs.lookup(gitRepoWithSecret("g", url, "creds"))}).resolveAuth(
				gitRepoWithSecret("g", url, "creds"),
				new(fInputs.secrets("ns", "creds")),
				testEndpoint(t, url),
			)
			assert.FatalIf(t, err != nil, "resolveAuth: %v", err)
			basic, ok := auth.(*githttp.BasicAuth)
			assert.FatalIf(t, !ok, "got %T, want *BasicAuth", auth)
			assert.ErrorIf(t, basic.Username != "alice" || basic.Password != "hunter2", "credentials lost: %+v", basic)
		})
	}
}

func TestFetcher_HTTPSBearerWinsOverBasic(t *testing.T) {
	t.Parallel()
	fInputs := testInputs{secrets: func(_, _ string) store.ReferenceFacts {
		secretInput := &manifest.Secret{
			StringData: map[string]any{
				"username":    "alice",
				"password":    "ignored",
				"bearerToken": "tkn_abc",
			},
		}

		return store.ReferenceFacts{Data: secretInput, Original: secretInput}
	}}
	f := &Fetcher{}
	repo := &manifest.GitRepository{
		Name: "g", Namespace: "ns",
		URL:       "https://github.com/x/y.git",
		SecretRef: &manifest.LocalObjectReference{Name: "creds"},
	}
	auth, err := (&operation{Fetcher: f, secrets: fInputs.lookup(repo)}).resolveAuth(
		repo,
		new(fInputs.secrets(repo.Namespace, repo.SecretRef.Name)),
		testEndpoint(t, repo.URL),
	)
	assert.FatalIf(t, err != nil, "resolveAuth: %v", err)
	tok, ok := auth.(*githttp.TokenAuth)
	assert.FatalIf(t, !ok, "got %T, want *TokenAuth", auth)
	assert.ErrorIf(t, tok.Token != "tkn_abc",
		"token: %q", tok.Token)
}

func TestFetcher_NoSecretIsAnonymous(t *testing.T) {
	t.Parallel()
	f := &Fetcher{}
	repo := &manifest.GitRepository{
		Name: "g", Namespace: "ns",
		URL: "https://github.com/x/y.git",
	}
	auth, err := (&operation{Fetcher: f}).resolveAuth(repo, new(store.ReferenceFacts{}), testEndpoint(t, repo.URL))
	assert.FatalIf(t, err != nil, "resolveAuth: %v", err)
	assert.ErrorIf(t, auth != nil, "expected nil auth (anonymous); got %T", auth)
}

func TestFetcher_SSHAuthEndpointUser(t *testing.T) {
	t.Parallel()
	identity, signer := testSSHIdentity(t)

	fInputs := testInputs{secrets: func(_, _ string) store.ReferenceFacts {
		secretInput := &manifest.Secret{StringData: map[string]any{
			"identity":    identity,
			"known_hosts": "host " + string(ssh.MarshalAuthorizedKey(signer.PublicKey())),
		}}

		return store.ReferenceFacts{Data: secretInput, Original: secretInput}
	}}
	f := &Fetcher{}
	for _, tc := range []struct{ url, user string }{
		{"git@github.com:owner/repo.git", "git"},
		{"ssh://buildbot@example.com/r.git", "buildbot"},
		{"ssh://@example.com/r.git", "git"},
	} {
		t.Run(tc.url, func(t *testing.T) {
			t.Parallel()
			testFetcherSSHAuthEndpointUserCase(
				t,
				&fInputs, &f, &tc,
			)
		},
		)
	}
}

func TestFetcher_ProviderMissingSecret(t *testing.T) {
	t.Parallel()
	fInputs := testInputs{secrets: func(_, _ string) store.ReferenceFacts { return store.ReferenceFacts{} }}
	f := &Fetcher{}
	repo := &manifest.GitRepository{
		Name: "g", Namespace: "ns",
		URL:       "https://github.com/x/y.git",
		Provider:  sourcev1.GitProviderGitHub,
		SecretRef: &manifest.LocalObjectReference{Name: "creds"},
	}
	_, err := fetchtest.Fetch(
		context.Background(),
		f.Fetch,
		repo,
		fInputs.readSet(repo),
		&sourcecache.Policy{MaxAge: repo.SourceInterval()},
		nil,
	)
	assert.FatalIf(t, err == nil, "expected missing-secret error")
	assert.ErrorIf(
		t,
		!errors.Is(err, manifest.ErrMissingSecret),
		"want ErrMissingSecret from the generic path; got %v",
		err,
	)
}

func testEndpoint(t *testing.T, url string) *transport.Endpoint {
	t.Helper()
	endpoint, err := transport.NewEndpoint(url)
	assert.NoError(t, err)
	return endpoint
}

//nolint:ireturn // The upstream SSH key parser exposes only its signer interface.
func testSSHIdentity(t *testing.T) (string, ssh.Signer) {
	t.Helper()
	_, key, err := ed25519.GenerateKey(nil)
	assert.NoError(t, err)
	block, err := ssh.MarshalPrivateKey(key, "fixture")
	assert.NoError(t, err)
	signer, err := ssh.NewSignerFromKey(key)
	assert.NoError(t, err)
	return string(pem.EncodeToMemory(block)), signer
}

func testFetcherSSHAuthEndpointUserCase(t *testing.T, fInputs *testInputs,
	f **Fetcher,
	tc *struct {
		url  string
		user string
	},
) {
	t.Helper()

	auth, err := (&operation{Fetcher: (*f), secrets: fInputs.lookup(gitRepoWithSecret("g", tc.url, "creds"))}).resolveAuth(
		gitRepoWithSecret("g", tc.url, "creds"),
		new(fInputs.
			secrets("ns", "creds")),
		testEndpoint(t, tc.url),
	)
	assert.NoError(t, err)

	keys, ok := auth.(*gitssh.PublicKeys)
	assert.FatalIf(t, !ok, "got %T, want SSH keys", auth)
	assert.FatalIf(t, keys.User != tc.user || keys.HostKeyCallback == nil,

		"SSH user = %q, want %q; host callback = %v",
		keys.User, tc.
			user, keys.HostKeyCallback != nil)
}

type gitSecretCase struct {
	name, field, wantKey, wantError                      string
	data, encodedData                                    map[string]any
	encoded, ssh, proxy, unwired, wantMissing, wantInput bool
}
type testFetcherSecretReferencesTailInputs struct {
	cases *[]gitSecretCase

	identity   *string
	knownHosts *string
}

func testFetcherSecretReferencesTail(t *testing.T, inputs *testFetcherSecretReferencesTailInputs) {
	t.Helper()

	for _, field := range []string{"tls.crt", "identity", "known_hosts", "password", "bearerToken"} {
		isSSH := field == "identity" || field == "known_hosts" || field == "password"
		for _, encoded := range []bool{false, true} {
			(*inputs.cases) = append((*inputs.cases), gitSecretCase{
				name:    fmt.Sprintf("%s/encoded=%t", field, encoded),
				field:   field,
				encoded: encoded,
				ssh:     isSSH,
				data: map[string]any{
					"identity":    (*inputs.identity),
					"known_hosts": (*inputs.knownHosts),
					"username":    "u",
					"password":    "p",
					field:         "",
				},
				wantMissing: field == "identity" || field == "known_hosts",
			})
		}
		if field == "tls.crt" {
			continue
		}
		for _, malformed := range []struct {
			name  string
			value any
		}{
			{"encoding", "%%%fixture"}, {"nonscalar", map[string]any{"credential-fixture": "payload"}},
		} {
			data := map[string]any{"identity": (*inputs.identity), "username": "u", "password": "p"}
			delete(data, field)
			(*inputs.cases) = append((*inputs.cases), gitSecretCase{
				name:        field + "/" + malformed.name,
				ssh:         isSSH,
				data:        data,
				encodedData: map[string]any{field: malformed.value},
				wantInput:   true,
				wantKey:     field,
			},
			)
		}
	}
	for _, tc := range *inputs.cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			testFetcherSecretReferencesCase(t, &testFetcherSecretReferencesCaseInputs{tc: &tc})
		})
	}
}

type testFetcherSecretReferencesCaseInputs struct {
	tc *gitSecretCase
}

func testFetcherSecretReferencesCase(t *testing.T, inputs *testFetcherSecretReferencesCaseInputs) {
	t.Helper()

	var sec *manifest.Secret
	if inputs.tc.data != nil || inputs.tc.encodedData != nil {
		sec = &manifest.Secret{
			Name:       "different",
			Namespace:  "elsewhere",
			StringData: inputs.tc.data,
			Data:       inputs.tc.encodedData,
		}
		if inputs.tc.encoded {
			sec.StringData, sec.Data = nil, map[string]any{}
			for k, v := range inputs.tc.data {
				sec.Data[k] = base64.StdEncoding.EncodeToString([]byte(v.(string)))
			}
		}
	}
	fInputs := testInputs{}
	f := &Fetcher{}
	if !inputs.tc.unwired {
		fInputs.secrets = func(_, _ string) store.ReferenceFacts {
			secretInput := sec
			if secretInput == nil {
				return store.ReferenceFacts{}
			}
			return store.ReferenceFacts{Data: secretInput, Original: secretInput}
		}
	}
	repo := gitRepoWithSecret("g", "https://host/repo", "creds")
	if inputs.tc.ssh {
		repo.URL = "ssh://git@host/repo"
	}
	var auth transport.AuthMethod
	var err error
	owner, secretName := repo.Named(), "creds"
	switch {
	case inputs.tc.proxy:
		repo.Name, repo.SecretRef = "repo", nil
		repo.ProxySecretRef = &manifest.LocalObjectReference{Name: "missing"}
		_, err = (&operation{Fetcher: f, secrets: fInputs.lookup(repo)}).resolveTransport(
			repo,
			&sourcecache.Policy{},
		)
		owner.Name, secretName = "repo", "missing"
	case inputs.tc.field == "tls.crt":
		_, err = (&operation{Fetcher: f, secrets: fInputs.lookup(repo)}).resolveTransport(
			repo,
			&sourcecache.Policy{},
		)
	default:

		var input store.ReferenceFacts
		if getter := fInputs.lookup(repo); getter != nil {
			input = getter(repo.Namespace, repo.SecretRef.Name)
		}
		auth, err = (&operation{Fetcher: f, secrets: fInputs.lookup(repo)}).resolveAuth(
			repo,
			&input,
			testEndpoint(t, repo.URL),
		)
	}

	assertSecretReferenceResult(t, inputs, &secretReferenceResult{repo, auth, err, owner, secretName})
}

type secretReferenceResult struct {
	repo       *manifest.GitRepository
	auth       transport.AuthMethod
	err        error
	owner      manifest.NamedResource
	secretName string
}

func assertSecretReferenceResult(
	t *testing.T, inputs *testFetcherSecretReferencesCaseInputs, result *secretReferenceResult,
) {
	t.Helper()
	repo, auth, err, owner, secretName := result.repo, result.auth, result.err, result.owner, result.secretName
	if err != nil {
		assert.Equal(t, auth == nil, true)
	}
	switch {
	case inputs.tc.wantInput:
		assert.Equal(t, errors.Is(err, manifest.ErrInput), true)
		assert.Equal(t, errors.Is(err, manifest.ErrFlux), true)
		assert.Equal(t, errors.Is(err, manifest.ErrMissingSecret), false)
		assert.FatalIf(t, err == nil || !strings.Contains(err.Error(), inputs.tc.wantKey) ||
			!strings.Contains(err.Error(), "secret ns/creds") ||
			strings.Contains(err.Error(), manifest.DiagnosticFor(repo.Named(), repo).Label()),
			"missing input key/source context: %v", err)

		for _, payload := range []string{"%%%fixture", "credential-fixture", "invalid-private-fixture", "invalid-known-hosts-fixture"} {
			assert.FatalIf(t, strings.Contains(err.Error(), payload), "%s", "credential payload leaked")
		}
	case inputs.tc.wantMissing:
		missing, ok := errors.AsType[*source.MissingSecretError](err)
		assert.FatalIf(t, !ok,
			"expected MissingSecretError; got %v", err)

		assert.Equal(t, errors.Is(err, manifest.ErrMissingSecret), true)
		assert.Equal(t, missing.Owner, owner)
		assert.Equal(
			t,
			missing.Secret,
			manifest.NamedResource{Kind: manifest.KindSecret, Namespace: "ns", Name: secretName},
		)
	case inputs.tc.wantError != "":
		assert.FatalIf(t, err == nil || !strings.Contains(err.Error(), inputs.tc.wantError),
			"expected %q error; got %v", inputs.tc.wantError, err)
	default:

		assert.NoError(t, err)
		assert.FatalIf(
			t,
			auth == nil && inputs.tc.field != "tls.crt",
			"%s",
			"valid credentials did not produce auth",
		)
	}
}

func TestFetcher_CapturedCredentialAlternatives(t *testing.T) {
	t.Parallel()
	t.Run("malformed basic auth beside encrypted bearer", func(t *testing.T) {
		t.Parallel()
		repo := &manifest.GitRepository{
			Name:      "repo",
			Namespace: "apps",
			URL:       "https://source.invalid/repo",
			SecretRef: &manifest.LocalObjectReference{Name: "auth"},
		}
		secret := &manifest.Secret{
			Data:       map[string]any{"password": "!"},
			StringData: map[string]any{"username": "user"},
		}
		facts := store.ReferenceFacts{
			Data:     secret,
			Original: secret,
			Warnings: []manifest.Warning{{Category: manifest.WarnSOPSUnreadable, Detail: []string{"bearerToken"}}},
		}
		getter := func(string, string) store.ReferenceFacts { return facts }
		_, err := (&operation{secrets: getter}).resolveAuth(repo, new(facts), testEndpoint(t, repo.URL))
		assert.Equal(t, errors.Is(err, manifest.ErrInput), true)
		assert.Equal(t, errors.Is(err, manifest.ErrMissingSecret), false)
	})
	t.Run("supplied SSH without passphrase", func(t *testing.T) {
		t.Parallel()
		identity, signer := testSSHIdentity(t)
		repo := &manifest.GitRepository{
			Name:      "repo",
			Namespace: "apps",
			URL:       "ssh://git@source.invalid/repo",
			SecretRef: &manifest.LocalObjectReference{Name: "auth"},
		}
		secret := &manifest.Secret{
			StringData: map[string]any{
				"identity":    identity,
				"known_hosts": knownhosts.Line([]string{"source.invalid"}, signer.PublicKey()),
			},
		}
		facts := store.ReferenceFacts{Data: secret, Generated: true}
		getter := func(string, string) store.ReferenceFacts { return facts }
		auth, err := (&operation{secrets: getter}).resolveAuth(repo, new(facts), testEndpoint(t, repo.URL))
		assert.NoError(t, err)
		assert.Equal(t, auth != nil, true)
	})
}
