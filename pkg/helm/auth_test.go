package helm

import (
	"strings"
	"testing"

	"github.com/home-operations/flate/pkg/manifest"
)

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
