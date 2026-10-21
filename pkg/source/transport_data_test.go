package source

import (
	"errors"
	"github.com/home-operations/flate/internal/testutil"
	"net/url"
	"strings"
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/pkg/manifest"
)

func TestResolveTransport_Identity(t *testing.T) {
	t.Parallel()
	ca := testutil.SelfSignedCA(t)
	secret := &manifest.Secret{StringData: map[string]any{"ca.crt": ca, "unused": "first"}}
	getter := func(_, _ string) *manifest.Secret { return secret }
	owner := manifest.NamedResource{Kind: manifest.KindOCIRepository, Namespace: "ns", Name: "repo"}
	inputs := []TransportInput{{Reference: &manifest.LocalObjectReference{Name: "tls"}, Use: TransportCert}}
	first, err := ResolveTransport(getter, owner, inputs)
	assert.NoError(t, err)
	firstIdentity := manifest.Fingerprint(first.Identity(nil))
	secret = &manifest.Secret{StringData: map[string]any{"ca.crt": ca, "unused": "second"}}
	second, err := ResolveTransport(getter, owner, inputs)
	assert.NoError(t, err)
	if manifest.Fingerprint(first.Identity(nil)) == manifest.Fingerprint(second.Identity(nil)) {
		t.Fatal("full certificate Secret identity lost an unused key")
	}
	assert.Equal(t, manifest.Fingerprint(first.Identity(nil)), firstIdentity)
	if manifest.Fingerprint(first.Identity("first")) == manifest.Fingerprint(first.Identity("second")) {
		t.Fatal("acquisition identity lost protocol credentials")
	}
	assert.Equal(t, first.Certificate(), second.Certificate())
	var network Network
	t.Cleanup(network.Close)
	firstTransport, err := network.HTTPTransport(HTTPTransportConfig{Material: first})
	assert.NoError(t, err)
	secondTransport, err := network.HTTPTransport(HTTPTransportConfig{Material: second})
	assert.NoError(t, err)
	assert.Equal(t, firstTransport, secondTransport)
}

func TestResolveTransport_ProxySnapshot(t *testing.T) {
	for _, key := range []string{"HTTP_PROXY", "HTTPS_PROXY", "NO_PROXY", "http_proxy", "https_proxy", "no_proxy", "REQUEST_METHOD"} {
		t.Setenv(key, "")
	}
	owner := manifest.NamedResource{Kind: manifest.KindOCIRepository, Namespace: "ns", Name: "repo"}
	anonymous, err := ResolveTransport(nil, owner, nil)
	assert.NoError(t, err)
	if anonymous.Identity(nil) != nil {
		t.Fatal("anonymous material gained an acquisition identity")
	}
	t.Setenv("HTTP_PROXY", "http://first.invalid:8080")
	t.Setenv("NO_PROXY", "bypass.invalid")
	material, err := ResolveTransport(nil, owner, nil)
	assert.NoError(t, err)
	t.Setenv("HTTP_PROXY", "http://second.invalid:8080")
	t.Setenv("NO_PROXY", "other.invalid")
	selected, err := material.ProxyURL(&url.URL{Scheme: "http", Host: "bypass.invalid"})
	assert.NoError(t, err)
	if selected != nil {
		t.Fatal("captured NO_PROXY changed")
	}
	selected, err = material.ProxyURL(&url.URL{Scheme: "http", Host: "chart.invalid"})
	assert.NoError(t, err)
	assert.Equal(t, selected.Host, "first.invalid:8080")
	getter := func(_, _ string) *manifest.Secret {
		return &manifest.Secret{StringData: map[string]any{"address": "http://explicit.invalid", "username": "user", "password": "password"}}
	}
	explicit, err := ResolveTransport(getter, owner, []TransportInput{{Reference: &manifest.LocalObjectReference{Name: "proxy"}, Use: TransportProxy}})
	assert.NoError(t, err)
	selected, err = explicit.ProxyURL(&url.URL{Scheme: "http", Host: "other.invalid"})
	assert.NoError(t, err)
	assert.Equal(t, selected.Host, "explicit.invalid")
	assert.Equal(t, selected.User.String(), "user:password")
	selected.Host = "mutated.invalid"
	selected, err = explicit.ProxyURL(&url.URL{Scheme: "http", Host: "chart.invalid"})
	assert.NoError(t, err)
	assert.Equal(t, selected.Host, "explicit.invalid")
}

func TestResolveTransport_InputOrder(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name, wantLabel, wantError string
		first, second              transportUse
	}{
		{name: "cert before proxy", first: TransportCert, second: TransportProxy, wantLabel: "certSecretRef", wantError: "ca.crt did not parse as PEM"},
		{name: "proxy before cert", first: TransportProxy, second: TransportCert, wantLabel: "proxySecretRef", wantError: "invalid proxy address"},
		{name: "secret before proxy", first: TransportSecret, second: TransportProxy, wantLabel: "secretRef", wantError: "ca.crt did not parse as PEM"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			getter := func(_, _ string) *manifest.Secret {
				return &manifest.Secret{StringData: map[string]any{"ca.crt": "invalid", "address": "http://proxy.invalid/%"}}
			}
			owner := manifest.NamedResource{Kind: manifest.KindGitRepository, Namespace: "ns", Name: "repo"}
			ref := &manifest.LocalObjectReference{Name: "creds"}
			_, err := ResolveTransport(getter, owner, []TransportInput{
				{Reference: ref, Use: tc.first}, {Reference: ref, Use: tc.second},
			})
			if !errors.Is(err, manifest.ErrInput) || !strings.Contains(err.Error(), tc.wantLabel) || !strings.Contains(err.Error(), tc.wantError) {
				t.Fatalf("wrong first input diagnostic: %v", err)
			}
		})
	}
}

func TestResolveTransport_UnavailableMaterial(t *testing.T) {
	t.Parallel()
	crt, _ := testutil.SelfSignedClientCert(t)
	for _, tc := range []struct {
		name                      string
		data                      map[string]any
		encodedData               map[string]any
		cert                      bool
		optional, absent, success bool
		missing                   bool
		want                      string
	}{

		{name: "optional auth only", cert: true, optional: true, success: true, data: map[string]any{"username": "alice", "password": "p"}},
		{name: "optional absent TLS", cert: true, optional: true, absent: true, success: true},
		{name: "required absent TLS", cert: true, absent: true, missing: true, want: "not found"},
		{name: "wiped CA", data: map[string]any{"ca.crt": "..PLACEHOLDER_ca.crt.."}, cert: true, missing: true, want: "TLS material unavailable"},
		{name: "mixed cert pair", data: map[string]any{"tls.crt": crt, "tls.key": "..PLACEHOLDER_tls.key.."}, cert: true, missing: true, want: "TLS material unavailable"},
		{name: "empty TLS", cert: true, missing: true, want: "TLS material unavailable"},
		{name: "real incomplete pair", data: map[string]any{"tls.crt": "real"}, cert: true, want: "must provide both tls.crt and tls.key together"},
		{name: "real invalid CA", data: map[string]any{"ca.crt": "invalid PEM"}, cert: true, want: "ca.crt did not parse as PEM"},
		{name: "invalid cert wiped key", data: map[string]any{"tls.crt": "real", "tls.key": "..PLACEHOLDER_tls.key.."}, cert: true, want: "parse tls.crt/tls.key"},
		{name: "wiped cert invalid CA", data: map[string]any{"tls.crt": "..PLACEHOLDER_tls.crt..", "ca.crt": "invalid PEM"}, cert: true, want: "ca.crt"},
		{name: "wiped cert bad key encoding", data: map[string]any{"tls.crt": "..PLACEHOLDER_tls.crt.."}, encodedData: map[string]any{"tls.key": "%%%fixture"}, cert: true, want: "tls.key"},
		{name: "wiped key bad CA scalar", data: map[string]any{"tls.key": "..PLACEHOLDER_tls.key..", "ca.crt": map[string]any{"credential-fixture": "payload"}}, cert: true, want: "ca.crt"},
		{name: "real proxy credentials without address", data: map[string]any{"username": "u", "password": "p"}, want: "missing required 'address' key"},
		{name: "empty proxy", missing: true, want: "missing required 'address' key"},
		{name: "wiped proxy", data: map[string]any{"address": "..PLACEHOLDER_address.."}, missing: true, want: "proxy material unavailable"},
		{name: "wiped malformed proxy address", data: map[string]any{"address": "http://..PLACEHOLDER_address../%"}, missing: true, want: "proxy material unavailable"},
		{name: "wiped proxy password", data: map[string]any{"address": "http://proxy", "password": "..PLACEHOLDER_password.."}, missing: true, want: "proxy material unavailable"},
		{name: "wiped address bad password encoding", data: map[string]any{"address": "..PLACEHOLDER_address.."}, encodedData: map[string]any{"password": "%%%fixture"}, want: "password"},
		{name: "wiped username bad password scalar", data: map[string]any{"address": "http://proxy", "username": "..PLACEHOLDER_username..", "password": map[string]any{"credential-fixture": "payload"}}, want: "password"},
		{name: "invalid address wiped password", data: map[string]any{"address": "http://proxy-credential-marker@proxy.invalid/%", "password": "..PLACEHOLDER_password.."}, want: "invalid proxy address"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var lookup [2]string
			calls := 0
			getter := func(ns, name string) *manifest.Secret {
				lookup = [2]string{ns, name}
				calls++
				if tc.absent {
					return nil
				}
				return &manifest.Secret{Name: "metadata-secret", Namespace: "elsewhere", StringData: tc.data, Data: tc.encodedData}
			}
			owner := manifest.NamedResource{Group: manifest.SourceDomain, Kind: manifest.KindOCIRepository, Namespace: "ns", Name: "repo"}
			ref := &manifest.LocalObjectReference{Name: "creds"}
			use := TransportProxy
			if tc.cert {
				use = TransportCert
			}
			material, err := ResolveTransport(getter, owner, []TransportInput{{Reference: ref, Use: use, OptionalTLS: tc.optional}})
			if tc.success {
				assert.NoError(t, err)
				if material.config != nil || material.Certificate() != [3]string{} {
					t.Fatal("optional auth-only input supplied TLS")
				}
				assert.Equal(t, calls, 1)
				return
			}

			if tc.missing {
				missing, ok := errors.AsType[*MissingSecretError](err)
				if !ok {
					t.Fatalf("expected structured MissingSecretError: %v", err)
				}
				if missing.Owner != owner || missing.Secret != (manifest.NamedResource{Kind: manifest.KindSecret, Namespace: "ns", Name: "creds"}) {
					t.Fatalf("credential identities = %+v", missing)
				}
			}
			if err == nil || errors.Is(err, manifest.ErrMissingSecret) != tc.missing || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("missing=%t, error=%v", tc.missing, err)
			}
			if !errors.Is(err, manifest.ErrFlux) || errors.Is(err, manifest.ErrInput) == tc.missing {
				t.Fatalf("wrong error classification: %v", err)
			}
			if (tc.cert || tc.encodedData != nil || tc.name == "wiped username bad password scalar" || tc.name == "invalid address wiped password") && !strings.Contains(err.Error(), "OCIRepository") {
				t.Fatalf("missing owner context: %v", err)
			}
			if !strings.Contains(err.Error(), "ns/creds") {
				t.Fatalf("missing referenced Secret: %v", err)
			}
			for _, marker := range []string{"%%%fixture", "credential-fixture", "proxy-credential-marker"} {
				if strings.Contains(err.Error(), marker) {
					t.Fatal("credential payload leaked")
				}
			}
			if lookup != [2]string{"ns", "creds"} || calls != 1 {
				t.Fatalf("lookup = %v (%d calls)", lookup, calls)
			}
		})
	}
}
