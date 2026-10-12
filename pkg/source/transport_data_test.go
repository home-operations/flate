package source

import (
	"errors"
	"testing"

	"github.com/home-operations/flate/pkg/manifest"
)

func TestResolveTransport_UnavailableMaterial(t *testing.T) {
	for _, tc := range []struct {
		name    string
		data    map[string]any
		cert    bool
		missing bool
	}{
		{"wiped CA", map[string]any{"ca.crt": "..PLACEHOLDER_ca.crt.."}, true, true},
		{"mixed cert pair", map[string]any{"tls.crt": "real", "tls.key": "..PLACEHOLDER_tls.key.."}, true, true},
		{"empty TLS", nil, true, true},
		{"real incomplete pair", map[string]any{"tls.crt": "real"}, true, false},
		{"real invalid CA", map[string]any{"ca.crt": "invalid PEM"}, true, false},
		{"real proxy credentials without address", map[string]any{"username": "u", "password": "p"}, false, false},
		{"empty proxy", nil, false, true},
		{"wiped proxy", map[string]any{"address": "..PLACEHOLDER_address.."}, false, true},
		{"wiped proxy password", map[string]any{"address": "http://proxy", "password": "..PLACEHOLDER_password.."}, false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			getter := func(_, _ string) *manifest.Secret { return &manifest.Secret{StringData: tc.data} }
			ref := &manifest.LocalObjectReference{Name: "creds"}
			var err error
			if tc.cert {
				_, err = ResolveCertSecret(getter, "ns", "OCIRepository", "repo", ref)
			} else {
				_, err = ResolveProxy(getter, "ns", "OCIRepository", "repo", ref)
			}
			if err == nil || errors.Is(err, manifest.ErrMissingSecret) != tc.missing {
				t.Fatalf("missing=%t, error=%v", tc.missing, err)
			}
		})
	}
}
