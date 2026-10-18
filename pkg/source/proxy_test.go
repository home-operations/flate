package source

import (
	"errors"
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/pkg/manifest"
)

func TestResolveProxy(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name, kind, owner, wantError string
		secret                       *manifest.Secret
		unwired                      bool
	}{
		{name: "unwired Git", kind: manifest.KindGitRepository, owner: "ns/r", unwired: true,
			wantError: "GitRepository ns/r references proxySecretRef but no source.SecretGetter is wired"},
		{name: "unwired Helm", kind: manifest.KindHelmRepository, owner: "HelmRepository ns/r", unwired: true,
			wantError: "HelmRepository HelmRepository ns/r references proxySecretRef but no source.SecretGetter is wired"},
		{name: "absent", kind: manifest.KindOCIRepository, owner: "ns/r",
			wantError: "flux error: missing secret: OCIRepository ns/ns/r: secret ns/px not found"},
		{name: "missing address", kind: manifest.KindBucket, owner: "ns/b",
			secret:    &manifest.Secret{StringData: map[string]any{"username": "alice"}},
			wantError: "flux error: input error: proxy secret ns/px missing required 'address' key"},
		{name: "full secret", kind: manifest.KindGitRepository, owner: "ns/r",
			secret: &manifest.Secret{StringData: map[string]any{"address": "http://proxy.example.com:8080", "username": "alice", "password": "hunter2"}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var lookup [2]string
			calls := 0
			var getter SecretGetter
			if !tc.unwired {
				getter = func(ns, name string) *manifest.Secret {
					lookup = [2]string{ns, name}
					calls++
					return tc.secret
				}
			}
			owner := manifest.NamedResource{Group: manifest.SourceDomain, Kind: tc.kind, Namespace: "ns", Name: tc.owner}
			p, err := ResolveProxy(getter, owner, &manifest.LocalObjectReference{Name: "px"})
			if !tc.unwired {
				assert.Equal(t, lookup, [2]string{"ns", "px"})
				assert.Equal(t, calls, 1)
			}
			if tc.wantError != "" {
				if err == nil || err.Error() != tc.wantError {
					t.Fatalf("ResolveProxy: %v, want %q", err, tc.wantError)
				}
				if !tc.unwired && tc.secret == nil {
					missing, ok := errors.AsType[*MissingSecretError](err)
					if !ok || !errors.Is(err, manifest.ErrMissingSecret) {
						t.Fatalf("expected missing-secret error: %v", err)
					}
					assert.Equal(t, missing.Owner, owner)
					assert.Equal(t, missing.Secret, manifest.NamedResource{Kind: manifest.KindSecret, Namespace: "ns", Name: "px"})
				}
				return
			}
			assert.NoError(t, err)
			assert.Equal(t, *p, ProxyConfig{Address: "http://proxy.example.com:8080", Username: "alice", Password: "hunter2"})
		})
	}
}

func TestProxyConfig_URL_BasicAuth(t *testing.T) {
	t.Parallel()
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
