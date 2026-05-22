package git

import (
	"strings"
	"testing"

	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
)

func TestFetcher_ResolveTLS_NoSecretRefIsNil(t *testing.T) {
	f := &Fetcher{}
	repo := &manifest.GitRepository{Name: "g", Namespace: "ns", URL: "https://example.com/x.git"}
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
	repo := &manifest.GitRepository{
		Name: "g", Namespace: "ns",
		URL:       "ssh://git@example.com/x.git",
		SecretRef: &manifest.LocalObjectReference{Name: "creds"},
	}
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
	repo := &manifest.GitRepository{
		Name: "g", Namespace: "ns",
		URL:       "https://example.com/x.git",
		SecretRef: &manifest.LocalObjectReference{Name: "creds"},
	}
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
	repo := &manifest.GitRepository{
		Name: "g", Namespace: "ns",
		URL:       "https://example.com/x.git",
		SecretRef: &manifest.LocalObjectReference{Name: "creds"},
	}
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
	repo := &manifest.GitRepository{
		Name: "g", Namespace: "ns",
		URL:       "https://example.com/x.git",
		SecretRef: &manifest.LocalObjectReference{Name: "creds"},
	}
	cfg, err := f.resolveTLS(repo)
	if err != nil {
		t.Fatalf("resolveTLS: %v", err)
	}
	if cfg == nil || cfg.RootCAs == nil {
		t.Fatalf("expected RootCAs populated from caFile: %+v", cfg)
	}
}

func TestFetcher_ResolveTLS_InvalidPEM(t *testing.T) {
	f := &Fetcher{
		Secrets: func(_, _ string) *manifest.Secret {
			return &manifest.Secret{StringData: map[string]any{"ca.crt": "-----BEGIN CERTIFICATE-----\nnot-pem\n-----END CERTIFICATE-----"}}
		},
	}
	repo := &manifest.GitRepository{
		Name: "g", Namespace: "ns",
		URL:       "https://example.com/x.git",
		SecretRef: &manifest.LocalObjectReference{Name: "creds"},
	}
	_, err := f.resolveTLS(repo)
	if err == nil || !strings.Contains(err.Error(), "did not parse as PEM") {
		t.Errorf("expected PEM parse error; got %v", err)
	}
}

