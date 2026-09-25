package helmchart

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
)

// TestHelmRepoTransport_NoCertRef: a repo without certSecretRef returns no
// per-repo transport (nil) — httpGet's package-default guarded transport
// applies. That default must carry the egress guard's dial hook.
func TestHelmRepoTransport_NoCertRef(t *testing.T) {
	r := httpRepo("https://charts.example")
	f := newHTTPFetcherWithSecrets(t, r, nil)

	tr, err := f.helmRepoTransport(r)
	if err != nil {
		t.Fatalf("helmRepoTransport: %v", err)
	}
	if tr != nil {
		t.Fatalf("no certSecretRef should yield a nil per-repo transport, got %v", tr)
	}
	if helmGuardedTransport.DialContext == nil {
		t.Fatal("the default helm transport must be egress-guarded (DialContext set)")
	}
}

// TestHelmRepoTransport_CertSecretYieldsGuardedTransport: a certSecretRef with
// CA material yields a non-nil transport that is BOTH egress-guarded
// (DialContext set) and TLS-configured (RootCAs from the CA), passed to helm via
// WithTransport. Uses a real CA PEM so source.BuildTLSConfig accepts it.
func TestHelmRepoTransport_CertSecretYieldsGuardedTransport(t *testing.T) {
	r := httpRepo("https://charts.example")
	r.CertSecretRef = &manifest.LocalObjectReference{Name: "tls"}
	f := newHTTPFetcherWithSecrets(t, r, func(_, _ string) *manifest.Secret {
		return &manifest.Secret{StringData: map[string]any{"ca.crt": testutil.SelfSignedCA(t)}}
	})

	tr, err := f.helmRepoTransport(r)
	if err != nil {
		t.Fatalf("helmRepoTransport: %v", err)
	}
	if tr == nil {
		t.Fatal("certSecretRef should yield a per-repo transport")
	}
	if tr.DialContext == nil {
		t.Error("per-repo helm transport must be egress-guarded (DialContext set)")
	}
	if tr.TLSClientConfig == nil || tr.TLSClientConfig.RootCAs == nil {
		t.Error("certSecretRef CA must populate the transport's RootCAs")
	}
}

// TestHelmRepoTransport_MissingSecretIsSoftSkippable: an absent certSecretRef
// Secret surfaces the shared missing-secret sentinel (so --allow-missing-secrets
// covers it), matching git/OCI/bucket via source.ResolveCertSecret.
func TestHelmRepoTransport_MissingSecretIsSoftSkippable(t *testing.T) {
	r := httpRepo("https://charts.example")
	r.CertSecretRef = &manifest.LocalObjectReference{Name: "tls"}
	f := newHTTPFetcherWithSecrets(t, r, func(_, _ string) *manifest.Secret { return nil })

	tr, err := f.helmRepoTransport(r)
	if err == nil {
		t.Fatal("expected an error for an absent certSecretRef Secret")
	}
	if !errors.Is(err, manifest.ErrMissingSecret) {
		t.Errorf("missing certSecretRef Secret must be the ErrMissingSecret sentinel; got %v", err)
	}
	if tr != nil {
		t.Errorf("transport = %v, want nil on error", tr)
	}
}

// TestHelmRepoTransport_MalformedFailsLoud: a present Secret carrying none of
// tls.crt/tls.key/ca.crt is malformed config and fails LOUD (not the
// soft-skippable missing-secret sentinel).
func TestHelmRepoTransport_MalformedFailsLoud(t *testing.T) {
	r := httpRepo("https://charts.example")
	r.CertSecretRef = &manifest.LocalObjectReference{Name: "tls"}
	f := newHTTPFetcherWithSecrets(t, r, func(_, _ string) *manifest.Secret {
		return &manifest.Secret{StringData: map[string]any{"unrelated": "x"}}
	})

	tr, err := f.helmRepoTransport(r)
	if err == nil {
		t.Fatal("expected a loud error for a TLS secret with none of the keys")
	}
	if errors.Is(err, manifest.ErrMissingSecret) {
		t.Errorf("a malformed (key-less) TLS secret must fail loud, not as ErrMissingSecret; got %v", err)
	}
	if tr != nil {
		t.Errorf("transport = %v, want nil on error", tr)
	}
}

// writeRegistryConfig materializes a docker-style config.json with the
// given auths entries — the #999 fallback probes these files, so the
// positive cases need a real config on disk, not just a path. Mirrors the
// oci package's writeDockerConfig.
func writeRegistryConfig(t *testing.T, auths string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "config.json")
	if err := os.WriteFile(path, []byte(`{"auths":{`+auths+`}}`), 0o600); err != nil {
		t.Fatalf("write registry config: %v", err)
	}
	return path
}

// #999: an unresolvable secretRef falls through to --registry-config when
// it holds a credential for the repo's host.
func TestHelmRepoAuthOptions_UnresolvableSecretFallsBackToRegistryConfig(t *testing.T) {
	cfg := writeRegistryConfig(t, `"charts.example":{"auth":"YWxpY2U6aHVudGVyMg=="}`)
	r := httpRepo("https://charts.example")
	r.SecretRef = &manifest.LocalObjectReference{Name: "creds"}
	f := newHTTPFetcherWithSecrets(t, r, func(_, _ string) *manifest.Secret { return nil })
	f.registryConfig = cfg

	opts, err := f.helmRepoAuthOptions(context.Background(), r)
	if err != nil {
		t.Fatalf("helmRepoAuthOptions: %v", err)
	}
	if len(opts) == 0 {
		t.Fatal("expected basic-auth options from the --registry-config fallback")
	}
}

// Same fallback via docker's default lookup when --registry-config is unset.
func TestHelmRepoAuthOptions_UnresolvableSecretFallsBackToDockerDefault(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "config.json"), []byte(`{"auths":{"charts.example":{"auth":"YWxpY2U6aHVudGVyMg=="}}}`), 0o600); err != nil {
		t.Fatalf("write docker config: %v", err)
	}
	t.Setenv("DOCKER_CONFIG", dir)
	r := httpRepo("https://charts.example")
	r.SecretRef = &manifest.LocalObjectReference{Name: "creds"}
	f := newHTTPFetcherWithSecrets(t, r, func(_, _ string) *manifest.Secret { return nil })

	opts, err := f.helmRepoAuthOptions(context.Background(), r)
	if err != nil {
		t.Fatalf("helmRepoAuthOptions: %v", err)
	}
	if len(opts) == 0 {
		t.Fatal("expected basic-auth options from the docker-default fallback")
	}
}

// Negative twins: without an actual credential for the repo's host, an
// unresolvable secretRef keeps the ErrMissingSecret sentinel —
// --allow-missing-secrets and producer-backed skips depend on it. Each
// shape must keep its reason wording ("not found" vs "missing
// username/password") — it surfaces verbatim in skip messages.
func TestHelmRepoAuthOptions_NoFallbackKeepsSentinel(t *testing.T) {
	t.Setenv("DOCKER_CONFIG", t.TempDir())
	cases := []struct {
		name    string
		secret  *manifest.Secret
		wantMsg string
	}{
		{"secret absent", nil, "not found"},
		{"wrong shape", &manifest.Secret{StringData: map[string]any{"username": "alice"}}, "missing username/password"},
		{"placeholder-wiped", &manifest.Secret{StringData: map[string]any{
			"username": "..PLACEHOLDER_username..",
			"password": "..PLACEHOLDER_password..",
		}}, "missing username/password"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r := httpRepo("https://charts.example")
			r.SecretRef = &manifest.LocalObjectReference{Name: "creds"}
			f := newHTTPFetcherWithSecrets(t, r, func(_, _ string) *manifest.Secret { return tc.secret })

			_, err := f.helmRepoAuthOptions(context.Background(), r)
			if !errors.Is(err, manifest.ErrMissingSecret) {
				t.Fatalf("err = %v, want ErrMissingSecret", err)
			}
			if !strings.Contains(err.Error(), tc.wantMsg) {
				t.Errorf("err = %v, want wording %q", err, tc.wantMsg)
			}
		})
	}
}

// A global config covering some other registry must not mask the sentinel.
func TestHelmRepoAuthOptions_GlobalWithoutHostCredKeepsSentinel(t *testing.T) {
	t.Setenv("DOCKER_CONFIG", t.TempDir())
	cfg := writeRegistryConfig(t, `"other.registry.io":{"auth":"YWxpY2U6aHVudGVyMg=="}`)
	r := httpRepo("https://charts.example")
	r.SecretRef = &manifest.LocalObjectReference{Name: "creds"}
	f := newHTTPFetcherWithSecrets(t, r, func(_, _ string) *manifest.Secret { return nil })
	f.registryConfig = cfg

	_, err := f.helmRepoAuthOptions(context.Background(), r)
	if !errors.Is(err, manifest.ErrMissingSecret) {
		t.Fatalf("err = %v, want ErrMissingSecret when the global config lacks the host's credential", err)
	}
}

// A corrupt --registry-config fails loudly rather than degrading to the
// sentinel: the operator pointed flate at that file explicitly.
func TestHelmRepoAuthOptions_CorruptRegistryConfigFailsLoud(t *testing.T) {
	t.Setenv("DOCKER_CONFIG", t.TempDir())
	path := filepath.Join(t.TempDir(), "config.json")
	if err := os.WriteFile(path, []byte(`{"auths":`), 0o600); err != nil {
		t.Fatalf("write corrupt config: %v", err)
	}
	r := httpRepo("https://charts.example")
	r.SecretRef = &manifest.LocalObjectReference{Name: "creds"}
	f := newHTTPFetcherWithSecrets(t, r, func(_, _ string) *manifest.Secret { return nil })
	f.registryConfig = path

	_, err := f.helmRepoAuthOptions(context.Background(), r)
	if err == nil {
		t.Fatal("expected corrupt --registry-config to fail loudly")
	}
	if errors.Is(err, manifest.ErrMissingSecret) {
		t.Errorf("corrupt config must not degrade to ErrMissingSecret; got %v", err)
	}
}
