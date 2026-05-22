package source

import (
	"context"
	"strings"
	"testing"

	githttp "github.com/go-git/go-git/v5/plumbing/transport/http"

	"github.com/home-operations/flate/pkg/manifest"
)

func TestGitFetcher_NonGenericProvider(t *testing.T) {
	f := &GitFetcher{}
	repo := &manifest.GitRepository{
		Name: "g", Namespace: "ns",
		URL:      "https://github.com/x/y.git",
		Provider: manifest.GitProviderGitHub,
	}
	_, err := f.Fetch(context.Background(), repo)
	if err == nil {
		t.Fatalf("expected error for unimplemented provider")
	}
	if !strings.Contains(err.Error(), "not implemented") {
		t.Errorf("error should say 'not implemented'; got %v", err)
	}
}

func TestGitFetcher_HTTPSBasicAuth(t *testing.T) {
	f := &GitFetcher{
		Secrets: func(_, _ string) *manifest.Secret {
			return &manifest.Secret{
				StringData: map[string]any{
					"username": "alice",
					"password": "hunter2",
				},
			}
		},
	}
	repo := &manifest.GitRepository{
		Name: "g", Namespace: "ns",
		URL:       "https://github.com/x/y.git",
		SecretRef: &manifest.LocalObjectReference{Name: "creds"},
	}
	auth, err := f.resolveAuth(repo)
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
}

func TestGitFetcher_HTTPSBearerWinsOverBasic(t *testing.T) {
	f := &GitFetcher{
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
		URL: "https://github.com/x/y.git", Name: "g", Namespace: "ns",
		SecretRef: &manifest.LocalObjectReference{Name: "creds"},
	}
	auth, err := f.resolveAuth(repo)
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

func TestGitFetcher_HTTPSMissingCreds(t *testing.T) {
	f := &GitFetcher{
		Secrets: func(_, _ string) *manifest.Secret {
			return &manifest.Secret{StringData: map[string]any{"username": "alice"}}
		},
	}
	repo := &manifest.GitRepository{
		URL: "https://github.com/x/y.git", Name: "g", Namespace: "ns",
		SecretRef: &manifest.LocalObjectReference{Name: "creds"},
	}
	_, err := f.resolveAuth(repo)
	if err == nil || !strings.Contains(err.Error(), "missing username/password") {
		t.Errorf("expected missing-creds error; got %v", err)
	}
}

func TestGitFetcher_NoSecretIsAnonymous(t *testing.T) {
	f := &GitFetcher{}
	repo := &manifest.GitRepository{URL: "https://github.com/x/y.git", Name: "g", Namespace: "ns"}
	auth, err := f.resolveAuth(repo)
	if err != nil {
		t.Fatalf("resolveAuth: %v", err)
	}
	if auth != nil {
		t.Errorf("expected nil auth (anonymous); got %T", auth)
	}
}

func TestGitFetcher_SecretRefMissingGetter(t *testing.T) {
	f := &GitFetcher{} // no Secrets
	repo := &manifest.GitRepository{
		URL: "https://github.com/x/y.git", Name: "g", Namespace: "ns",
		SecretRef: &manifest.LocalObjectReference{Name: "creds"},
	}
	_, err := f.resolveAuth(repo)
	if err == nil || !strings.Contains(err.Error(), "SecretGetter") {
		t.Errorf("expected SecretGetter error; got %v", err)
	}
}

func TestSshUserFromURL(t *testing.T) {
	cases := map[string]string{
		"git@github.com:owner/repo.git":      "git",
		"ssh://buildbot@example.com/r.git":   "buildbot",
		"https://github.com/x/y.git":         "git", // not actually SSH, but tests default
		"":                                   "git",
	}
	for url, want := range cases {
		if got := sshUserFromURL(url); got != want {
			t.Errorf("sshUserFromURL(%q) = %q, want %q", url, got, want)
		}
	}
}

func TestIsSSHURL(t *testing.T) {
	yes := []string{
		"git@github.com:o/r.git",
		"ssh://git@example.com/r",
	}
	no := []string{
		"https://github.com/o/r.git",
		"http://example.com/r",
		"file:///tmp/r",
	}
	for _, u := range yes {
		if !isSSHURL(u) {
			t.Errorf("isSSHURL(%q) = false, want true", u)
		}
	}
	for _, u := range no {
		if isSSHURL(u) {
			t.Errorf("isSSHURL(%q) = true, want false", u)
		}
	}
}
