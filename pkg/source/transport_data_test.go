package source

import (
	"errors"
	"github.com/home-operations/flate/internal/testutil"
	"strings"
	"testing"

	"github.com/home-operations/flate/pkg/manifest"
)

func TestResolveTransport_UnavailableMaterial(t *testing.T) {
	t.Parallel()
	crt, _ := testutil.SelfSignedClientCert(t)
	for _, tc := range []struct {
		name        string
		data        map[string]any
		encodedData map[string]any
		cert        bool
		missing     bool
		want        string
	}{

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
				return &manifest.Secret{Name: "metadata-secret", Namespace: "elsewhere", StringData: tc.data, Data: tc.encodedData}
			}
			owner := manifest.NamedResource{Group: manifest.SourceDomain, Kind: manifest.KindOCIRepository, Namespace: "ns", Name: "repo"}
			ref := &manifest.LocalObjectReference{Name: "creds"}
			var err error
			if tc.cert {
				_, err = ResolveCertSecret(getter, owner, ref)
			} else {
				_, err = ResolveProxy(getter, owner, ref)
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
