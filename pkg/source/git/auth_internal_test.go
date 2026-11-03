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

	sourcev1 "github.com/fluxcd/source-controller/api/v1"
	"github.com/go-git/go-git/v5/plumbing/transport"
	githttp "github.com/go-git/go-git/v5/plumbing/transport/http"
	gitssh "github.com/go-git/go-git/v5/plumbing/transport/ssh"
	"golang.org/x/crypto/ssh"

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

	cases := []testFetcherSecretReferencesSecretCase{
		{name: "missing proxy", proxy: true, wantMissing: true},
		{name: "missing auth", wantMissing: true},
		{name: "incomplete basic", data: map[string]any{"username": "alice"}, wantMissing: true},
		{name: "unwired getter", unwired: true, wantError: "SecretGetter"},
		{name: "empty bearer uses basic", data: map[string]any{"bearerToken": "", "username": "u", "password": "p"}},
		{
			name: "empty optional SSH",
			ssh:  true,
			data: map[string]any{"identity": identity, "password": "", "known_hosts": ""},
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
			name:        "wiped identity malformed password",
			ssh:         true,
			data:        map[string]any{"identity": "..PLACEHOLDER_identity.."},
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
			name:        "wiped password malformed known hosts",
			ssh:         true,
			data:        map[string]any{"identity": identity, "password": "..PLACEHOLDER_password.."},
			encodedData: map[string]any{"known_hosts": "%%%fixture"},
			wantInput:   true,
			wantKey:     "known_hosts",
		},
		{
			name:        "malformed identity wiped password",
			ssh:         true,
			data:        map[string]any{"password": "..PLACEHOLDER_password.."},
			encodedData: map[string]any{"identity": "%%%fixture"},
			wantInput:   true,
			wantKey:     "identity",
		},
		{
			name:      "identity PEM label sanitized",
			ssh:       true,
			data:      map[string]any{"identity": badPEM, "known_hosts": "..PLACEHOLDER_known_hosts.."},
			wantInput: true,
			wantKey:   "identity",
		},
		{
			name: "host marker sanitized",
			ssh:  true,
			data: map[string]any{
				"identity":    "..PLACEHOLDER_identity..",
				"known_hosts": "@credential-fixture host ignored",
			},
			wantInput: true,
			wantKey:   "known_hosts",
		},
		{
			name: "invalid identity wiped hosts",
			ssh:  true,
			data: map[string]any{
				"identity":    "invalid-private-fixture",
				"known_hosts": "..PLACEHOLDER_known_hosts..",
			},
			wantInput: true,
			wantKey:   "identity",
		},
		{
			name:      "invalid identity wiped password",
			ssh:       true,
			data:      map[string]any{"identity": "invalid-private-fixture", "password": "..PLACEHOLDER_password.."},
			wantInput: true,
			wantKey:   "identity",
		},
		{
			name:        "encrypted identity wiped password",
			ssh:         true,
			data:        map[string]any{"identity": encryptedIdentity, "password": "..PLACEHOLDER_password.."},
			wantMissing: true,
		},
		{
			name: "encrypted identity correct password",
			ssh:  true,
			data: map[string]any{"identity": encryptedIdentity, "password": "fixture-password"},
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
			name: "wiped identity invalid hosts",
			ssh:  true,
			data: map[string]any{
				"identity":    "..PLACEHOLDER_identity..",
				"known_hosts": "invalid-known-hosts-fixture",
			},
			wantInput: true,
			wantKey:   "known_hosts",
		},
		{
			name: "encrypted identity wiped password invalid hosts",
			ssh:  true,
			data: map[string]any{
				"identity":    encryptedIdentity,
				"password":    "..PLACEHOLDER_password..",
				"known_hosts": "invalid-known-hosts-fixture",
			},
			wantInput: true,
			wantKey:   "known_hosts",
		},
		{
			name: "plain identity wiped password invalid hosts",
			ssh:  true,
			data: map[string]any{
				"identity":    identity,
				"password":    "..PLACEHOLDER_password..",
				"known_hosts": "invalid-known-hosts-fixture",
			},
			wantInput: true,
			wantKey:   "known_hosts",
		},
		{
			name:        "raw data placeholder is malformed",
			encodedData: map[string]any{"bearerToken": "..PLACEHOLDER_bearerToken.."},
			wantInput:   true,
			wantKey:     "bearerToken",
		},
	}
	for _, field := range []string{"tls.crt", "identity", "known_hosts", "password", "bearerToken"} {
		isSSH := field == "identity" || field == "known_hosts" || field == "password"
		for _, encoded := range []bool{false, true} {
			cases = append(cases, testFetcherSecretReferencesSecretCase{
				name:    fmt.Sprintf("%s/encoded=%t", field, encoded),
				field:   field,
				encoded: encoded,
				ssh:     isSSH,
				data: map[string]any{
					"identity": identity,
					"username": "u",
					"password": "p",
					field:      "..PLACEHOLDER_" + field + "..",
				},
				wantMissing: true,
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
			data := map[string]any{"identity": identity, "username": "u", "password": "p"}
			delete(data, field)
			cases = append(
				cases,
				testFetcherSecretReferencesSecretCase{
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
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			testFetcherSecretReferencesCase(t, testFetcherSecretReferencesCaseInputs{tc: &tc})
		},
		)
	}
}

func TestFetcher_ProviderUnavailable(t *testing.T) {
	t.Parallel()
	f := &Fetcher{}
	repo := &manifest.GitRepository{
		Name: "g", Namespace: "ns",
		URL:      "https://github.com/x/y.git",
		Provider: sourcev1.GitProviderGitHub,
	}
	_, err := f.Fetch(
		context.Background(),
		repo,
		(testInputs{}).readSet(repo),
		sourcecache.Policy{MaxAge: repo.SourceInterval()},
	)
	assert.FatalIf(t, err == nil, "expected error for unimplemented provider")
	assert.ErrorIf(t, !errors.Is(err, manifest.ErrMissingSecret), "expected missing provider credentials; got %v", err)
}

func TestFetcher_HTTPSBasicAuth(t *testing.T) {
	t.Parallel()
	fInputs := testInputs{secrets: func(_, _ string) *manifest.Secret {
		return &manifest.Secret{
			StringData: map[string]any{
				"username": "alice",
				"password": "hunter2",
			},
		}
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
				fInputs.secrets("ns", "creds"),
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
	fInputs := testInputs{secrets: func(_, _ string) *manifest.Secret {
		return &manifest.Secret{
			StringData: map[string]any{
				"username":    "alice",
				"password":    "ignored",
				"bearerToken": "tkn_abc",
			},
		}
	}}
	f := &Fetcher{}
	repo := &manifest.GitRepository{
		Name: "g", Namespace: "ns",
		URL:       "https://github.com/x/y.git",
		SecretRef: &manifest.LocalObjectReference{Name: "creds"},
	}
	auth, err := (&operation{Fetcher: f, secrets: fInputs.lookup(repo)}).resolveAuth(
		repo,
		fInputs.secrets(repo.Namespace, repo.SecretRef.Name),
		testEndpoint(t, repo.URL),
	)
	assert.FatalIf(t, err != nil, "resolveAuth: %v", err)
	tok, ok := auth.(*githttp.TokenAuth)
	assert.FatalIf(t, !ok, "got %T, want *TokenAuth", auth)
	if tok.Token != "tkn_abc" {
		t.Errorf("token: %q", tok.Token)
	}
}

func TestFetcher_NoSecretIsAnonymous(t *testing.T) {
	t.Parallel()
	f := &Fetcher{}
	repo := &manifest.GitRepository{
		Name: "g", Namespace: "ns",
		URL: "https://github.com/x/y.git",
	}
	auth, err := (&operation{Fetcher: f}).resolveAuth(repo, nil, testEndpoint(t, repo.URL))
	assert.FatalIf(t, err != nil, "resolveAuth: %v", err)
	assert.ErrorIf(t, auth != nil, "expected nil auth (anonymous); got %T", auth)
}

func TestFetcher_SSHAuthEndpointUser(t *testing.T) {
	t.Parallel()
	_, key, err := ed25519.GenerateKey(nil)
	assert.NoError(t, err)

	block, err := ssh.MarshalPrivateKey(key, "fixture")
	assert.NoError(t, err)

	fInputs := testInputs{secrets: func(_, _ string) *manifest.Secret {
		return &manifest.Secret{StringData: map[string]any{"identity": string(pem.EncodeToMemory(block))}}
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
	fInputs := testInputs{secrets: func(_, _ string) *manifest.Secret { return nil }}
	f := &Fetcher{}
	repo := &manifest.GitRepository{
		Name: "g", Namespace: "ns",
		URL:       "https://github.com/x/y.git",
		Provider:  sourcev1.GitProviderGitHub,
		SecretRef: &manifest.LocalObjectReference{Name: "creds"},
	}
	_, err := f.Fetch(
		context.Background(),
		repo,
		fInputs.readSet(repo),
		sourcecache.Policy{MaxAge: repo.SourceInterval()},
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
		fInputs.
			secrets("ns", "creds"),
		testEndpoint(t, tc.url),
	)
	assert.NoError(t, err)

	keys, ok := auth.(*gitssh.PublicKeys)
	assert.FatalIf(t, !ok, "got %T, want SSH keys", auth)
	if keys.User != tc.user || keys.HostKeyCallback == nil {
		t.Fatalf(
			"SSH user = %q, want %q; host callback = %v",
			keys.User, tc.
				user, keys.HostKeyCallback != nil,
		)
	}
}

type testFetcherSecretReferencesSecretCase struct {
	name, field, wantKey, wantError                      string
	data, encodedData                                    map[string]any
	encoded, ssh, proxy, unwired, wantMissing, wantInput bool
}
type testFetcherSecretReferencesCaseInputs struct {
	tc *testFetcherSecretReferencesSecretCase
}

func testFetcherSecretReferencesCase(t *testing.T, testCaseInput testFetcherSecretReferencesCaseInputs) {
	t.Helper()

	var sec *manifest.Secret
	if testCaseInput.tc.data != nil || testCaseInput.tc.encodedData != nil {
		sec = &manifest.Secret{
			Name:       "different",
			Namespace:  "elsewhere",
			StringData: testCaseInput.tc.data,
			Data:       testCaseInput.tc.encodedData,
		}
		if testCaseInput.tc.encoded {
			sec.StringData, sec.Data = nil, map[string]any{}
			for k, v := range testCaseInput.tc.data {
				sec.Data[k] = base64.StdEncoding.EncodeToString([]byte(v.(string)))
			}
		}
	}
	fInputs := testInputs{}
	f := &Fetcher{}
	if !testCaseInput.tc.unwired {
		fInputs.secrets = func(_, _ string) *manifest.Secret { return sec }
	}
	repo := gitRepoWithSecret("g", "https://host/repo", "creds")
	if testCaseInput.tc.ssh {
		repo.URL = "ssh://git@host/repo"
	}
	var auth transport.AuthMethod
	var err error
	owner, secretName := repo.Named(), "creds"
	if (*testCaseInput.tc).proxy {
		repo.Name, repo.SecretRef = "repo", nil
		repo.ProxySecretRef = &manifest.LocalObjectReference{Name: "missing"}
		_, err = (&operation{Fetcher: f, secrets: fInputs.lookup(repo)}).resolveTransport(
			repo,
			sourcecache.Policy{},
		)
		owner.Name, secretName = "repo", "missing"
	} else if testCaseInput.tc.field == "tls.crt" {
		_, err = (&operation{Fetcher: f, secrets: fInputs.lookup(repo)}).resolveTransport(
			repo,
			sourcecache.Policy{},
		)
	} else {
		auth, err = (&operation{Fetcher: f, secrets: fInputs.lookup(repo)}).resolveAuth(
			repo,
			sec,
			testEndpoint(t, repo.URL),
		)
	}
	testFetcherSecretReferencesCaseCompletion(
		t,
		&testFetcherSecretReferencesCaseCompletionInputs{
			err:           &err,
			auth:          &auth,
			testCaseInput: &testCaseInput,
			repo:          &repo,
			owner:         &owner,
			secretName:    &secretName,
		},
	)
}

func testFetcherSecretReferencesCaseCompletion(
	t *testing.T,
	captures *testFetcherSecretReferencesCaseCompletionInputs,
) {
	t.Helper()
	if (*captures.err) != nil {
		assert.Equal(t, (*captures.auth) == nil, true)
	}
	if (*(*captures.testCaseInput).tc).wantInput {
		assert.Equal(t, errors.Is((*captures.err), manifest.ErrInput), true)
		assert.Equal(t, errors.Is((*captures.err), manifest.ErrFlux), true)
		assert.Equal(t, errors.Is((*captures.err), manifest.ErrMissingSecret), false)
		assert.FatalIf(
			t,
			(*captures.err) == nil || !strings.Contains((*captures.err).Error(), captures.testCaseInput.tc.wantKey) ||
				!strings.Contains((*captures.err).Error(), "secret ns/creds") ||
				strings.Contains((*captures.err).
					Error(), manifest.DiagnosticFor((*captures.repo).Named(), (*captures.repo)).Label(),
				),
			"missing input key/source context: %v",
			(*captures.err),
		)
		for _, payload := range []string{"%%%fixture", "credential-fixture", "invalid-private-fixture", "invalid-known-hosts-fixture"} {
			assert.FatalIf(t, strings.Contains((*captures.err).Error(), payload), "%s", "credential payload leaked")
		}
	} else if captures.testCaseInput.tc.wantMissing {
		missing, ok := errors.AsType[*source.MissingSecretError]((*captures.err))
		assert.FatalIf(t, !ok, "expected MissingSecretError; got %v", (*captures.err))
		assert.Equal(t, errors.Is((*captures.err), manifest.ErrMissingSecret), true)
		assert.Equal(t, missing.Owner, (*captures.owner))
		assert.Equal(
			t,
			missing.Secret,
			manifest.NamedResource{Kind: manifest.KindSecret, Namespace: "ns", Name: (*captures.secretName)},
		)
	} else if captures.testCaseInput.tc.wantError != "" {
		if (*captures.err) == nil || !strings.Contains((*captures.err).Error(), captures.testCaseInput.tc.wantError) {
			t.Fatalf("expected %q error; got %v", captures.testCaseInput.tc.wantError, (*captures.err))
		}
	} else {
		assert.NoError(t, (*captures.err))
		assert.FatalIf(t, (*captures.auth) == nil, "%s", "valid credentials did not produce auth")
	}
}

type testFetcherSecretReferencesCaseCompletionInputs struct {
	err           *error
	auth          *transport.AuthMethod
	testCaseInput *testFetcherSecretReferencesCaseInputs
	repo          **manifest.GitRepository
	owner         *manifest.NamedResource
	secretName    *string
}
