package source

import (
	"errors"
	"strings"
	"testing"

	"github.com/home-operations/flate/pkg/manifest"
)

func TestResolveProxy_NilRef(t *testing.T) {
	p, err := ResolveProxy(nil, manifest.NamedResource{Kind: manifest.KindGitRepository, Namespace: "ns", Name: "ns/r"}, nil)
	if err != nil {
		t.Fatalf("ResolveProxy: %v", err)
	}
	if p != nil {
		t.Errorf("expected nil ProxyConfig for nil ref")
	}
}

func TestResolveProxy_NoGetter(t *testing.T) {
	for _, tc := range []struct{ kind, name, want string }{
		{manifest.KindGitRepository, "ns/r", "GitRepository ns/r references proxySecretRef but no source.SecretGetter is wired"},
		{manifest.KindHelmRepository, "HelmRepository ns/r", "HelmRepository HelmRepository ns/r references proxySecretRef but no source.SecretGetter is wired"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := ResolveProxy(nil, manifest.NamedResource{Kind: tc.kind, Namespace: "ns", Name: tc.name},
				&manifest.LocalObjectReference{Name: "px"})
			if err == nil || err.Error() != tc.want {
				t.Errorf("expected %q; got %v", tc.want, err)
			}
		})
	}
}

func TestResolveProxy_SecretNotFound(t *testing.T) {
	var lookup [2]string
	calls := 0
	owner := manifest.NamedResource{Group: manifest.SourceDomain, Kind: manifest.KindOCIRepository, Namespace: "ns", Name: "ns/r"}
	_, err := ResolveProxy(func(ns, name string) *manifest.Secret {
		lookup = [2]string{ns, name}
		calls++
		return nil
	}, owner, &manifest.LocalObjectReference{Name: "px"})
	missing, ok := errors.AsType[*MissingSecretError](err)
	if !ok || !errors.Is(err, manifest.ErrMissingSecret) {
		t.Fatalf("expected missing-secret error; got %v", err)
	}
	wantSecret := manifest.NamedResource{Kind: manifest.KindSecret, Namespace: "ns", Name: "px"}
	const want = "flux error: missing secret: OCIRepository ns/ns/r: secret ns/px not found"
	if lookup != [2]string{"ns", "px"} || calls != 1 || missing.Owner != owner || missing.Secret != wantSecret || err.Error() != want {
		t.Fatalf("lookup = %v (%d calls), identities = %+v, error = %v", lookup, calls, missing, err)
	}
}

func TestResolveProxy_MissingAddress(t *testing.T) {
	getter := func(_, _ string) *manifest.Secret {
		return &manifest.Secret{StringData: map[string]any{"username": "alice"}}
	}
	_, err := ResolveProxy(getter, manifest.NamedResource{Kind: manifest.KindBucket, Namespace: "ns", Name: "ns/b"},
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
	p, err := ResolveProxy(getter, manifest.NamedResource{Kind: manifest.KindGitRepository, Namespace: "ns", Name: "ns/r"},
		&manifest.LocalObjectReference{Name: "px"})
	if err != nil {
		t.Fatalf("ResolveProxy: %v", err)
	}
	if p.Address != "http://proxy.example.com:8080" {
		t.Errorf("Address = %q", p.Address)
	}
	if p.Username != "alice" || p.Password != "hunter2" {
		t.Errorf("creds = %q/%q", p.Username, p.Password)
	}
}

func TestProxyConfig_URL_BasicAuth(t *testing.T) {
	p := &ProxyConfig{
		Address:  "http://proxy.example.com:8080",
		Username: "alice",
		Password: "hunter2",
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
