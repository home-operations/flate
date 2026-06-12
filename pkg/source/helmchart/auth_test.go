package helmchart

import (
	"errors"
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
