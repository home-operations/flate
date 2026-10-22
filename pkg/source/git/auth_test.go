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
	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source"
	sourcecache "github.com/home-operations/flate/pkg/source/cache"
	"golang.org/x/crypto/ssh"
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
	type secretCase struct {
		name, field, wantKey, wantError                      string
		data, encodedData                                    map[string]any
		encoded, ssh, proxy, unwired, wantMissing, wantInput bool
	}
	cases := []secretCase{
		{name: "missing proxy", proxy: true, wantMissing: true},
		{name: "missing auth", wantMissing: true},
		{name: "incomplete basic", data: map[string]any{"username": "alice"}, wantMissing: true},
		{name: "unwired getter", unwired: true, wantError: "SecretGetter"},
		{name: "empty bearer uses basic", data: map[string]any{"bearerToken": "", "username": "u", "password": "p"}},
		{name: "empty optional SSH", ssh: true, data: map[string]any{"identity": identity, "password": "", "known_hosts": ""}},
		{name: "valid known hosts", ssh: true, data: map[string]any{"identity": identity, "known_hosts": "host " + string(ssh.MarshalAuthorizedKey(hostKey))}},
		{name: "invalid identity", ssh: true, data: map[string]any{"identity": "invalid-private-fixture"}, wantInput: true, wantKey: "identity"},
		{name: "invalid known hosts", ssh: true, data: map[string]any{"identity": identity, "known_hosts": "invalid-known-hosts-fixture"}, wantInput: true, wantKey: "known_hosts"},
		{name: "wiped identity malformed password", ssh: true, data: map[string]any{"identity": "..PLACEHOLDER_identity.."}, encodedData: map[string]any{"password": "%%%fixture"}, wantInput: true, wantKey: "password"},
		{name: "missing identity malformed known hosts", ssh: true, encodedData: map[string]any{"known_hosts": "%%%fixture"}, wantInput: true, wantKey: "known_hosts"},
		{name: "wiped password malformed known hosts", ssh: true, data: map[string]any{"identity": identity, "password": "..PLACEHOLDER_password.."}, encodedData: map[string]any{"known_hosts": "%%%fixture"}, wantInput: true, wantKey: "known_hosts"},
		{name: "malformed identity wiped password", ssh: true, data: map[string]any{"password": "..PLACEHOLDER_password.."}, encodedData: map[string]any{"identity": "%%%fixture"}, wantInput: true, wantKey: "identity"},
		{name: "identity PEM label sanitized", ssh: true, data: map[string]any{"identity": badPEM, "known_hosts": "..PLACEHOLDER_known_hosts.."}, wantInput: true, wantKey: "identity"},
		{name: "host marker sanitized", ssh: true, data: map[string]any{"identity": "..PLACEHOLDER_identity..", "known_hosts": "@credential-fixture host ignored"}, wantInput: true, wantKey: "known_hosts"},
		{name: "invalid identity wiped hosts", ssh: true, data: map[string]any{"identity": "invalid-private-fixture", "known_hosts": "..PLACEHOLDER_known_hosts.."}, wantInput: true, wantKey: "identity"},
		{name: "invalid identity wiped password", ssh: true, data: map[string]any{"identity": "invalid-private-fixture", "password": "..PLACEHOLDER_password.."}, wantInput: true, wantKey: "identity"},
		{name: "encrypted identity wiped password", ssh: true, data: map[string]any{"identity": encryptedIdentity, "password": "..PLACEHOLDER_password.."}, wantMissing: true},
		{name: "encrypted identity correct password", ssh: true, data: map[string]any{"identity": encryptedIdentity, "password": "fixture-password"}},
		{name: "encrypted identity wrong password", ssh: true, data: map[string]any{"identity": encryptedIdentity, "password": "wrong-password"}, wantInput: true, wantKey: "identity"},
		{name: "absent identity invalid hosts", ssh: true, data: map[string]any{"known_hosts": "invalid-known-hosts-fixture"}, wantInput: true, wantKey: "known_hosts"},
		{name: "wiped identity invalid hosts", ssh: true, data: map[string]any{"identity": "..PLACEHOLDER_identity..", "known_hosts": "invalid-known-hosts-fixture"}, wantInput: true, wantKey: "known_hosts"},
		{name: "encrypted identity wiped password invalid hosts", ssh: true, data: map[string]any{"identity": encryptedIdentity, "password": "..PLACEHOLDER_password..", "known_hosts": "invalid-known-hosts-fixture"}, wantInput: true, wantKey: "known_hosts"},
		{name: "plain identity wiped password invalid hosts", ssh: true, data: map[string]any{"identity": identity, "password": "..PLACEHOLDER_password..", "known_hosts": "invalid-known-hosts-fixture"}, wantInput: true, wantKey: "known_hosts"},
		{name: "raw data placeholder is malformed", encodedData: map[string]any{"bearerToken": "..PLACEHOLDER_bearerToken.."}, wantInput: true, wantKey: "bearerToken"},
	}
	for _, field := range []string{"tls.crt", "identity", "known_hosts", "password", "bearerToken"} {
		isSSH := field == "identity" || field == "known_hosts" || field == "password"
		for _, encoded := range []bool{false, true} {
			cases = append(cases, secretCase{
				name: fmt.Sprintf("%s/encoded=%t", field, encoded), field: field, encoded: encoded, ssh: isSSH,
				data: map[string]any{"identity": identity, "username": "u", "password": "p", field: "..PLACEHOLDER_" + field + ".."}, wantMissing: true,
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
			cases = append(cases, secretCase{name: field + "/" + malformed.name, ssh: isSSH, data: data, encodedData: map[string]any{field: malformed.value}, wantInput: true, wantKey: field})
		}
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var sec *manifest.Secret
			if tc.data != nil || tc.encodedData != nil {
				sec = &manifest.Secret{Name: "different", Namespace: "elsewhere", StringData: tc.data, Data: tc.encodedData}
				if tc.encoded {
					sec.StringData, sec.Data = nil, map[string]any{}
					for k, v := range tc.data {
						sec.Data[k] = base64.StdEncoding.EncodeToString([]byte(v.(string)))
					}
				}
			}
			fInputs := testInputs{}
			f := &Fetcher{}
			if !tc.unwired {
				fInputs.secrets = func(_, _ string) *manifest.Secret { return sec }
			}
			repo := gitRepoWithSecret("g", "https://host/repo", "creds")
			if tc.ssh {
				repo.URL = "ssh://git@host/repo"
			}
			var auth transport.AuthMethod
			var err error
			owner, secretName := repo.Named(), "creds"
			if tc.proxy {
				repo.Name, repo.SecretRef = "repo", nil
				repo.ProxySecretRef = &manifest.LocalObjectReference{Name: "missing"}
				_, err = (&operation{Fetcher: f, secrets: fInputs.lookup(repo)}).resolveTransport(repo)
				owner.Name, secretName = "repo", "missing"
			} else if tc.field == "tls.crt" {
				_, err = (&operation{Fetcher: f, secrets: fInputs.lookup(repo)}).resolveTransport(repo)
			} else {
				auth, err = (&operation{Fetcher: f, secrets: fInputs.lookup(repo)}).resolveAuth(repo, sec)
			}
			if err != nil {
				assert.Equal(t, auth == nil, true)
			}
			if tc.wantInput {
				assert.Equal(t, errors.Is(err, manifest.ErrInput), true)
				assert.Equal(t, errors.Is(err, manifest.ErrFlux), true)
				assert.Equal(t, errors.Is(err, manifest.ErrMissingSecret), false)
				if err == nil || !strings.Contains(err.Error(), tc.wantKey) || !strings.Contains(err.Error(), "secret ns/creds") || !strings.Contains(err.Error(), source.QualifiedName(manifest.KindGitRepository, repo.Namespace, repo.Name)) {
					t.Fatalf("missing input key/source context: %v", err)
				}
				for _, payload := range []string{"%%%fixture", "credential-fixture", "invalid-private-fixture", "invalid-known-hosts-fixture"} {
					if strings.Contains(err.Error(), payload) {
						t.Fatal("credential payload leaked")
					}
				}
			} else if tc.wantMissing {
				missing, ok := errors.AsType[*source.MissingSecretError](err)
				if !ok {
					t.Fatalf("expected MissingSecretError; got %v", err)
				}
				assert.Equal(t, errors.Is(err, manifest.ErrMissingSecret), true)
				assert.Equal(t, missing.Owner, owner)
				assert.Equal(t, missing.Secret, manifest.NamedResource{Kind: manifest.KindSecret, Namespace: "ns", Name: secretName})
			} else if tc.wantError != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantError) {
					t.Fatalf("expected %q error; got %v", tc.wantError, err)
				}
			} else {
				assert.NoError(t, err)
				if auth == nil {
					t.Fatal("valid credentials did not produce auth")
				}
			}
		})
	}
}

func TestFetcher_NonGenericProvider(t *testing.T) {
	t.Parallel()
	f := &Fetcher{}
	repo := &manifest.GitRepository{
		Name: "g", Namespace: "ns",
		URL:      "https://github.com/x/y.git",
		Provider: sourcev1.GitProviderGitHub,
	}
	_, err := f.Fetch(context.Background(), repo, (testInputs{}).readSet(repo), sourcecache.Policy{MaxAge: repo.SourceInterval()})
	if err == nil {
		t.Fatalf("expected error for unimplemented provider")
	}
	if !strings.Contains(err.Error(), "not implemented") {
		t.Errorf("error should say 'not implemented'; got %v", err)
	}
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
			auth, err := (&operation{Fetcher: f, secrets: fInputs.lookup(gitRepoWithSecret("g", url, "creds"))}).resolveAuth(gitRepoWithSecret("g", url, "creds"), fInputs.secrets("ns", "creds"))
			if err != nil {
				t.Fatalf("resolveAuth: %v", err)
			}
			basic, ok := auth.(*githttp.BasicAuth)
			if !ok {
				t.Fatalf("got %T, want *BasicAuth", auth)
			}
			if basic.Username != "alice" || basic.Password != "hunter2" {
				t.Errorf("credentials lost: %+v", basic)
			}
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
	auth, err := (&operation{Fetcher: f, secrets: fInputs.lookup(repo)}).resolveAuth(repo, fInputs.secrets(repo.Namespace, repo.SecretRef.Name))
	if err != nil {
		t.Fatalf("resolveAuth: %v", err)
	}
	tok, ok := auth.(*githttp.TokenAuth)
	if !ok {
		t.Fatalf("got %T, want *TokenAuth", auth)
	}
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
	auth, err := (&operation{Fetcher: f}).resolveAuth(repo, nil)
	if err != nil {
		t.Fatalf("resolveAuth: %v", err)
	}
	if auth != nil {
		t.Errorf("expected nil auth (anonymous); got %T", auth)
	}
}

func TestFetcher_SSHAuthEndpointUser(t *testing.T) {
	t.Parallel()
	_, key, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatal(err)
	}
	block, err := ssh.MarshalPrivateKey(key, "fixture")
	if err != nil {
		t.Fatal(err)
	}
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
			auth, err := (&operation{Fetcher: f, secrets: fInputs.lookup(gitRepoWithSecret("g", tc.url, "creds"))}).resolveAuth(gitRepoWithSecret("g", tc.url, "creds"), fInputs.secrets("ns", "creds"))
			if err != nil {
				t.Fatal(err)
			}
			keys, ok := auth.(*gitssh.PublicKeys)
			if !ok {
				t.Fatalf("got %T, want SSH keys", auth)
			}
			if keys.User != tc.user || keys.HostKeyCallback == nil {
				t.Fatalf("SSH user = %q, want %q; host callback = %v", keys.User, tc.user, keys.HostKeyCallback != nil)
			}
		})
	}
}

func TestFetcher_ForceGenericProvider(t *testing.T) {
	t.Parallel()
	// Gate bypassed: the generic path runs and fails on the absent secret.
	fInputs := testInputs{secrets: func(_, _ string) *manifest.Secret { return nil }}
	f := &Fetcher{
		ForceGeneric: true,
	}
	repo := &manifest.GitRepository{
		Name: "g", Namespace: "ns",
		URL:       "https://github.com/x/y.git",
		Provider:  sourcev1.GitProviderGitHub,
		SecretRef: &manifest.LocalObjectReference{Name: "creds"},
	}
	_, err := f.Fetch(context.Background(), repo, fInputs.readSet(repo), sourcecache.Policy{MaxAge: repo.SourceInterval()})
	if err == nil {
		t.Fatalf("expected missing-secret error")
	}
	if strings.Contains(err.Error(), "not implemented") {
		t.Errorf("provider gate should be bypassed with ForceGeneric; got %v", err)
	}
	if !errors.Is(err, manifest.ErrMissingSecret) {
		t.Errorf("want ErrMissingSecret from the generic path; got %v", err)
	}
}
