package git

import (
	"strings"
	"testing"

	sourcev1 "github.com/fluxcd/source-controller/api/v1"

	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
)

func gitRepoWithSecret(name, url, secretName string) *manifest.GitRepository {
	repo := &manifest.GitRepository{
		Name: name, Namespace: "ns",
		GitRepositorySpec: sourcev1.GitRepositorySpec{URL: url},
	}
	if secretName != "" {
		repo.SecretRef = &manifest.LocalObjectReference{Name: secretName}
	}
	return repo
}

func TestFetcher_ResolveTLS_NoSecretRefIsNil(t *testing.T) {
	f := &Fetcher{}
	repo := gitRepoWithSecret("g", "https://example.com/x.git", "")
	cfg, err := f.resolveTLS(repo)
	if err != nil {
		t.Fatalf("resolveTLS: %v", err)
	}
	if cfg != nil {
		t.Errorf("expected nil TLS config when no SecretRef")
	}
}

func TestFetcher_ResolveTLS_SSHURLIsNil(t *testing.T) {
	f := &Fetcher{
		Secrets: func(_, _ string) *manifest.Secret {
			return &manifest.Secret{StringData: map[string]any{"ca.crt": "x"}}
		},
	}
	repo := gitRepoWithSecret("g", "ssh://git@example.com/x.git", "creds")
	cfg, err := f.resolveTLS(repo)
	if err != nil {
		t.Fatalf("resolveTLS: %v", err)
	}
	if cfg != nil {
		t.Errorf("expected nil TLS config for SSH URL")
	}
}

func TestFetcher_ResolveTLS_NoCAKeyInSecretIsNil(t *testing.T) {
	f := &Fetcher{
		Secrets: func(_, _ string) *manifest.Secret {
			return &manifest.Secret{StringData: map[string]any{"username": "alice", "password": "p"}}
		},
	}
	repo := gitRepoWithSecret("g", "https://example.com/x.git", "creds")
	cfg, err := f.resolveTLS(repo)
	if err != nil {
		t.Fatalf("resolveTLS: %v", err)
	}
	if cfg != nil {
		t.Errorf("expected nil TLS config when SecretRef carries no CA")
	}
}

func TestFetcher_ResolveTLS_CAFromCACrt(t *testing.T) {
	caPEM := testutil.SelfSignedCA(t)
	f := &Fetcher{
		Secrets: func(_, _ string) *manifest.Secret {
			return &manifest.Secret{StringData: map[string]any{"ca.crt": caPEM}}
		},
	}
	repo := gitRepoWithSecret("g", "https://example.com/x.git", "creds")
	cfg, err := f.resolveTLS(repo)
	if err != nil {
		t.Fatalf("resolveTLS: %v", err)
	}
	if cfg == nil || cfg.RootCAs == nil {
		t.Fatalf("expected RootCAs populated from ca.crt: %+v", cfg)
	}
}

func TestFetcher_ResolveTLS_CAFromCAFileLegacyKey(t *testing.T) {
	caPEM := testutil.SelfSignedCA(t)
	f := &Fetcher{
		Secrets: func(_, _ string) *manifest.Secret {
			return &manifest.Secret{StringData: map[string]any{"caFile": caPEM}}
		},
	}
	repo := gitRepoWithSecret("g", "https://example.com/x.git", "creds")
	cfg, err := f.resolveTLS(repo)
	if err != nil {
		t.Fatalf("resolveTLS: %v", err)
	}
	if cfg == nil || cfg.RootCAs == nil {
		t.Fatalf("expected RootCAs populated from caFile: %+v", cfg)
	}
}

func TestFetcher_ResolveTLS_ClientCertMTLS(t *testing.T) {
	crt, key := testutil.SelfSignedClientCert(t)
	caPEM := testutil.SelfSignedCA(t)
	f := &Fetcher{
		Secrets: func(_, _ string) *manifest.Secret {
			return &manifest.Secret{StringData: map[string]any{
				"tls.crt": crt, "tls.key": key, "ca.crt": caPEM,
			}}
		},
	}
	repo := gitRepoWithSecret("g", "https://example.com/x.git", "creds")
	cfg, err := f.resolveTLS(repo)
	if err != nil {
		t.Fatalf("resolveTLS: %v", err)
	}
	if cfg == nil || len(cfg.Certificates) != 1 {
		t.Fatalf("expected one client certificate from tls.crt/tls.key: %+v", cfg)
	}
	if cfg.RootCAs == nil {
		t.Errorf("expected RootCAs populated alongside the client cert")
	}
}

func TestFetcher_ResolveTLS_ClientCertWithoutKeyErrors(t *testing.T) {
	crt, _ := testutil.SelfSignedClientCert(t)
	f := &Fetcher{
		Secrets: func(_, _ string) *manifest.Secret {
			return &manifest.Secret{StringData: map[string]any{"tls.crt": crt}}
		},
	}
	repo := gitRepoWithSecret("g", "https://example.com/x.git", "creds")
	_, err := f.resolveTLS(repo)
	if err == nil || !strings.Contains(err.Error(), "tls.crt and tls.key") {
		t.Errorf("expected paired-cert error; got %v", err)
	}
}

func TestFetcher_ResolveTLS_InvalidPEM(t *testing.T) {
	f := &Fetcher{
		Secrets: func(_, _ string) *manifest.Secret {
			return &manifest.Secret{StringData: map[string]any{"ca.crt": "-----BEGIN CERTIFICATE-----\nnot-pem\n-----END CERTIFICATE-----"}}
		},
	}
	repo := gitRepoWithSecret("g", "https://example.com/x.git", "creds")
	_, err := f.resolveTLS(repo)
	if err == nil || !strings.Contains(err.Error(), "did not parse as PEM") {
		t.Errorf("expected PEM parse error; got %v", err)
	}
}
