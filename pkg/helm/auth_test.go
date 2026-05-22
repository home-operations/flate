package helm

import (
	"strings"
	"testing"

	"github.com/home-operations/flate/pkg/manifest"
)

func TestHelmRepoTLS_NoCertSecretIsNoOp(t *testing.T) {
	c, err := NewClient(t.TempDir(), t.TempDir())
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	r := &manifest.HelmRepository{Name: "r", Namespace: "ns", URL: "https://charts.example"}
	opts, cleanup, err := c.helmRepoTLSOptions(r)
	defer cleanup()
	if err != nil {
		t.Fatalf("helmRepoTLSOptions: %v", err)
	}
	if opts != nil {
		t.Errorf("expected nil opts when no CertSecretRef; got %v", opts)
	}
}

func TestHelmRepoTLS_FromSecret(t *testing.T) {
	c, err := NewClient(t.TempDir(), t.TempDir())
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	c.SetSecretGetter(func(_, _ string) *manifest.Secret {
		return &manifest.Secret{
			StringData: map[string]any{
				"tls.crt": "-----BEGIN CERTIFICATE-----\nfake\n-----END CERTIFICATE-----\n",
				"tls.key": "-----BEGIN PRIVATE KEY-----\nfake\n-----END PRIVATE KEY-----\n",
				"ca.crt":  "-----BEGIN CERTIFICATE-----\nca\n-----END CERTIFICATE-----\n",
			},
		}
	})
	r := &manifest.HelmRepository{
		Name: "r", Namespace: "ns", URL: "https://charts.example",
		CertSecretRef: &manifest.LocalObjectReference{Name: "tls-creds"},
	}
	opts, cleanup, err := c.helmRepoTLSOptions(r)
	defer cleanup()
	if err != nil {
		t.Fatalf("helmRepoTLSOptions: %v", err)
	}
	if len(opts) == 0 {
		t.Errorf("expected non-empty TLS opts when cert/key/ca present")
	}
}

func TestHelmRepoTLS_AllKeysMissing(t *testing.T) {
	c, err := NewClient(t.TempDir(), t.TempDir())
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	c.SetSecretGetter(func(_, _ string) *manifest.Secret {
		return &manifest.Secret{StringData: map[string]any{"unrelated": "x"}}
	})
	r := &manifest.HelmRepository{
		Name: "r", Namespace: "ns",
		CertSecretRef: &manifest.LocalObjectReference{Name: "wrong-shape"},
	}
	_, cleanup, err := c.helmRepoTLSOptions(r)
	cleanup()
	if err == nil || !strings.Contains(err.Error(), "tls.crt") {
		t.Errorf("expected error mentioning expected keys; got %v", err)
	}
}

func TestHelmRepoTLS_CertSecretNotFound(t *testing.T) {
	c, err := NewClient(t.TempDir(), t.TempDir())
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	c.SetSecretGetter(func(_, _ string) *manifest.Secret { return nil })
	r := &manifest.HelmRepository{
		Name: "r", Namespace: "ns",
		CertSecretRef: &manifest.LocalObjectReference{Name: "missing"},
	}
	_, cleanup, err := c.helmRepoTLSOptions(r)
	cleanup()
	if err == nil || !strings.Contains(err.Error(), "cert secret ns/missing not found") {
		t.Errorf("expected secret-not-found error; got %v", err)
	}
}

func TestHelmRepoAuth_NoSecretIsAnonymous(t *testing.T) {
	c, err := NewClient(t.TempDir(), t.TempDir())
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	r := &manifest.HelmRepository{Name: "r", Namespace: "ns", URL: "https://charts.example"}
	opts, err := c.helmRepoAuthOptions(r)
	if err != nil {
		t.Fatalf("helmRepoAuthOptions: %v", err)
	}
	if opts != nil {
		t.Errorf("expected nil opts (anonymous); got %v", opts)
	}
}

func TestHelmRepoAuth_BasicAuthFromSecret(t *testing.T) {
	c, err := NewClient(t.TempDir(), t.TempDir())
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	c.SetSecretGetter(func(_, _ string) *manifest.Secret {
		return &manifest.Secret{
			StringData: map[string]any{
				"username": "alice",
				"password": "hunter2",
			},
		}
	})
	r := &manifest.HelmRepository{
		Name: "r", Namespace: "ns", URL: "https://charts.example",
		SecretRef: &manifest.LocalObjectReference{Name: "creds"},
	}
	opts, err := c.helmRepoAuthOptions(r)
	if err != nil {
		t.Fatalf("helmRepoAuthOptions: %v", err)
	}
	// We can't read getter.Option fields back, but we can verify a
	// non-empty options slice is returned (WithBasicAuth).
	if len(opts) == 0 {
		t.Errorf("expected non-empty opts when SecretRef has creds")
	}
}

func TestHelmRepoAuth_MissingCreds(t *testing.T) {
	c, err := NewClient(t.TempDir(), t.TempDir())
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	c.SetSecretGetter(func(_, _ string) *manifest.Secret {
		return &manifest.Secret{StringData: map[string]any{"username": "alice"}}
	})
	r := &manifest.HelmRepository{
		Name: "r", Namespace: "ns",
		SecretRef: &manifest.LocalObjectReference{Name: "creds"},
	}
	_, err = c.helmRepoAuthOptions(r)
	if err == nil || !strings.Contains(err.Error(), "missing username/password") {
		t.Errorf("expected missing-creds error; got %v", err)
	}
}

func TestHelmRepoAuth_SecretWipedTreatedAsMissing(t *testing.T) {
	c, err := NewClient(t.TempDir(), t.TempDir())
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	c.SetSecretGetter(func(_, _ string) *manifest.Secret {
		return &manifest.Secret{
			StringData: map[string]any{
				"username": "..PLACEHOLDER_username..",
				"password": "..PLACEHOLDER_password..",
			},
		}
	})
	r := &manifest.HelmRepository{
		Name: "r", Namespace: "ns",
		SecretRef: &manifest.LocalObjectReference{Name: "creds"},
	}
	_, err = c.helmRepoAuthOptions(r)
	if err == nil || !strings.Contains(err.Error(), "missing username/password") {
		t.Errorf("--wipe-secrets placeholders should be treated as missing; got %v", err)
	}
}

func TestHelmRepoAuth_NoGetter(t *testing.T) {
	c, err := NewClient(t.TempDir(), t.TempDir())
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	r := &manifest.HelmRepository{
		Name: "r", Namespace: "ns",
		SecretRef: &manifest.LocalObjectReference{Name: "creds"},
	}
	_, err = c.helmRepoAuthOptions(r)
	if err == nil || !strings.Contains(err.Error(), "SecretGetter") {
		t.Errorf("expected SecretGetter error; got %v", err)
	}
}

func TestHelmRepoAuth_SecretNotFound(t *testing.T) {
	c, err := NewClient(t.TempDir(), t.TempDir())
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	c.SetSecretGetter(func(_, _ string) *manifest.Secret { return nil })
	r := &manifest.HelmRepository{
		Name: "r", Namespace: "ns",
		SecretRef: &manifest.LocalObjectReference{Name: "missing"},
	}
	_, err = c.helmRepoAuthOptions(r)
	if err == nil || !strings.Contains(err.Error(), "secret ns/missing not found") {
		t.Errorf("expected secret-not-found error; got %v", err)
	}
}
