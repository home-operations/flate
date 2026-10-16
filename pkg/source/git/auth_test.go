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
	"github.com/home-operations/flate/pkg/manifest"
	sourcecache "github.com/home-operations/flate/pkg/source/cache"
	"golang.org/x/crypto/ssh"
)

func TestFetcher_WipedCredentials(t *testing.T) {
	_, key, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatal(err)
	}
	block, err := ssh.MarshalPrivateKey(key, "fixture")
	if err != nil {
		t.Fatal(err)
	}
	for _, field := range []string{"tls.crt", "identity", "known_hosts", "bearerToken"} {
		for _, encoded := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/encoded=%t", field, encoded), func(t *testing.T) {
				data := map[string]any{"identity": string(pem.EncodeToMemory(block)), "username": "u", "password": "p", field: "..PLACEHOLDER_" + field + ".."}
				sec := &manifest.Secret{StringData: data}
				if encoded {
					sec.StringData, sec.Data = nil, map[string]any{}
					for k, v := range data {
						sec.Data[k] = base64.StdEncoding.EncodeToString([]byte(v.(string)))
					}
				}
				f := &Fetcher{Secrets: func(_, _ string) *manifest.Secret { return sec }}
				repo := &manifest.GitRepository{Name: "g", Namespace: "ns", URL: "https://host/repo", SecretRef: &manifest.LocalObjectReference{Name: "creds"}}
				if field == "identity" || field == "known_hosts" {
					repo.URL = "ssh://git@host/repo"
				}
				if field == "tls.crt" {
					_, err = f.resolveTLS(repo, f.Secrets(repo.Namespace, repo.SecretRef.Name))
				} else {
					var auth transport.AuthMethod
					auth, err = f.resolveAuth(repo, f.Secrets(repo.Namespace, repo.SecretRef.Name))
					if field == "bearerToken" {
						if _, ok := auth.(*githttp.BasicAuth); !ok {
							t.Fatalf("got %T, want basic auth", auth)
						}
					}
				}
				missing := field == "tls.crt" || field == "identity"
				if missing && !errors.Is(err, manifest.ErrMissingSecret) || !missing && err != nil {
					t.Fatalf("missing=%t, error=%v", missing, err)
				}
			})
		}
	}
}

func TestFetcher_NonGenericProvider(t *testing.T) {
	f := &Fetcher{}
	repo := &manifest.GitRepository{
		Name: "g", Namespace: "ns",
		URL:      "https://github.com/x/y.git",
		Provider: sourcev1.GitProviderGitHub,
	}
	_, err := f.Fetch(context.Background(), repo, sourcecache.RetryConfig{})
	if err == nil {
		t.Fatalf("expected error for unimplemented provider")
	}
	if !strings.Contains(err.Error(), "not implemented") {
		t.Errorf("error should say 'not implemented'; got %v", err)
	}
}

func TestFetcher_HTTPSBasicAuth(t *testing.T) {
	f := &Fetcher{
		Secrets: func(_, _ string) *manifest.Secret {
			return &manifest.Secret{
				StringData: map[string]any{
					"username": "alice",
					"password": "hunter2",
				},
			}
		},
	}
	for _, url := range []string{
		"https://github.com/x/y.git",
		"http://example.com/x@y",
		"./a@b",
	} {
		t.Run(url, func(t *testing.T) {
			auth, err := f.resolveAuth(gitRepoWithSecret("g", url, "creds"), f.Secrets("ns", "creds"))
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
	f := &Fetcher{
		Secrets: func(_, _ string) *manifest.Secret {
			return &manifest.Secret{
				StringData: map[string]any{
					"username":    "alice",
					"password":    "ignored",
					"bearerToken": "tkn_abc",
				},
			}
		},
	}
	repo := &manifest.GitRepository{
		Name: "g", Namespace: "ns",
		URL:       "https://github.com/x/y.git",
		SecretRef: &manifest.LocalObjectReference{Name: "creds"},
	}
	auth, err := f.resolveAuth(repo, f.Secrets(repo.Namespace, repo.SecretRef.Name))
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

func TestFetcher_HTTPSMissingCreds(t *testing.T) {
	f := &Fetcher{
		Secrets: func(_, _ string) *manifest.Secret {
			return &manifest.Secret{StringData: map[string]any{"username": "alice"}}
		},
	}
	repo := &manifest.GitRepository{
		Name: "g", Namespace: "ns",
		URL:       "https://github.com/x/y.git",
		SecretRef: &manifest.LocalObjectReference{Name: "creds"},
	}
	_, err := f.resolveAuth(repo, f.Secrets(repo.Namespace, repo.SecretRef.Name))
	if err == nil || !strings.Contains(err.Error(), "missing username/password") {
		t.Errorf("expected missing-creds error; got %v", err)
	}
}

func TestFetcher_NoSecretIsAnonymous(t *testing.T) {
	f := &Fetcher{}
	repo := &manifest.GitRepository{
		Name: "g", Namespace: "ns",
		URL: "https://github.com/x/y.git",
	}
	auth, err := f.resolveAuth(repo, nil)
	if err != nil {
		t.Fatalf("resolveAuth: %v", err)
	}
	if auth != nil {
		t.Errorf("expected nil auth (anonymous); got %T", auth)
	}
}

func TestFetcher_SecretRefMissingGetter(t *testing.T) {
	f := &Fetcher{} // no Secrets
	repo := &manifest.GitRepository{
		Name: "g", Namespace: "ns",
		URL:       "https://github.com/x/y.git",
		SecretRef: &manifest.LocalObjectReference{Name: "creds"},
	}
	_, err := f.resolveAuth(repo, nil)
	if err == nil || !strings.Contains(err.Error(), "SecretGetter") {
		t.Errorf("expected SecretGetter error; got %v", err)
	}
}

func TestFetcher_SSHAuthEndpointUser(t *testing.T) {
	_, key, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatal(err)
	}
	block, err := ssh.MarshalPrivateKey(key, "fixture")
	if err != nil {
		t.Fatal(err)
	}
	f := &Fetcher{Secrets: func(_, _ string) *manifest.Secret {
		return &manifest.Secret{StringData: map[string]any{"identity": string(pem.EncodeToMemory(block))}}
	}}
	for _, tc := range []struct{ url, user string }{
		{"git@github.com:owner/repo.git", "git"},
		{"ssh://buildbot@example.com/r.git", "buildbot"},
		{"ssh://@example.com/r.git", "git"},
	} {
		t.Run(tc.url, func(t *testing.T) {
			auth, err := f.resolveAuth(gitRepoWithSecret("g", tc.url, "creds"), f.Secrets("ns", "creds"))
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
	// Gate bypassed: the generic path runs and fails on the absent secret.
	f := &Fetcher{
		ForceGeneric: true,
		Secrets:      func(_, _ string) *manifest.Secret { return nil },
	}
	repo := &manifest.GitRepository{
		Name: "g", Namespace: "ns",
		URL:       "https://github.com/x/y.git",
		Provider:  sourcev1.GitProviderGitHub,
		SecretRef: &manifest.LocalObjectReference{Name: "creds"},
	}
	_, err := f.Fetch(context.Background(), repo, sourcecache.RetryConfig{})
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
