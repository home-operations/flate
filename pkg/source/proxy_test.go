package source

import (
	"strings"
	"testing"

	"github.com/home-operations/flate/pkg/manifest"
)

func TestResolveProxy_NilRef(t *testing.T) {
	p, err := ResolveProxy(nil, "ns", "GitRepository", "ns/r", nil)
	if err != nil {
		t.Fatalf("ResolveProxy: %v", err)
	}
	if p != nil {
		t.Errorf("expected nil ProxyConfig for nil ref")
	}
}

func TestResolveProxy_NoGetter(t *testing.T) {
	_, err := ResolveProxy(nil, "ns", "GitRepository", "ns/r",
		&manifest.LocalObjectReference{Name: "px"})
	if err == nil || !strings.Contains(err.Error(), "source.SecretGetter") {
		t.Errorf("expected SecretGetter error; got %v", err)
	}
}

func TestResolveProxy_SecretNotFound(t *testing.T) {
	_, err := ResolveProxy(
		func(_, _ string) *manifest.Secret { return nil },
		"ns", "OCIRepository", "ns/r",
		&manifest.LocalObjectReference{Name: "px"})
	if err == nil || !strings.Contains(err.Error(), "proxy secret ns/px not found") {
		t.Errorf("expected not-found error; got %v", err)
	}
}

func TestResolveProxy_MissingAddress(t *testing.T) {
	getter := func(_, _ string) *manifest.Secret {
		return &manifest.Secret{StringData: map[string]any{"username": "alice"}}
	}
	_, err := ResolveProxy(getter, "ns", "Bucket", "ns/b",
		&manifest.LocalObjectReference{Name: "px"})
	if err == nil || !strings.Contains(err.Error(), "missing required 'address' key") {
		t.Errorf("expected missing-address error; got %v", err)
	}
}

func TestResolveProxy_FullSecret(t *testing.T) {
	getter := func(_, _ string) *manifest.Secret {
		return &manifest.Secret{StringData: map[string]any{
			"address":  "http://proxy.example.com:8080",
			"username": "alice",
			"password": "hunter2",
		}}
	}
	p, err := ResolveProxy(getter, "ns", "GitRepository", "ns/r",
		&manifest.LocalObjectReference{Name: "px"})
	if err != nil {
		t.Fatalf("ResolveProxy: %v", err)
	}
	if p.Address != "http://proxy.example.com:8080" {
		t.Errorf("Address = %q", p.Address)
	}
	if p.Username != "alice" || p.Password != "hunter2" { //nolint:gosec // test fixture
		t.Errorf("creds = %q/%q", p.Username, p.Password)
	}
}

func TestProxyConfig_URL_BasicAuth(t *testing.T) {
	p := &ProxyConfig{
		Address:  "http://proxy.example.com:8080",
		Username: "alice",
		Password: "hunter2", //nolint:gosec // test fixture
	}
	u, err := p.URL()
	if err != nil {
		t.Fatalf("URL: %v", err)
	}
	if u.User == nil {
		t.Fatalf("expected userinfo in URL")
	}
	if u.User.Username() != "alice" {
		t.Errorf("Username = %q", u.User.Username())
	}
	if pw, _ := u.User.Password(); pw != "hunter2" {
		t.Errorf("Password = %q", pw)
	}
	if u.Host != "proxy.example.com:8080" {
		t.Errorf("Host = %q", u.Host)
	}
}

func TestProxyConfig_URL_InvalidAddress(t *testing.T) {
	p := &ProxyConfig{Address: "://broken"}
	_, err := p.URL()
	if err == nil {
		t.Errorf("expected parse error")
	}
}
