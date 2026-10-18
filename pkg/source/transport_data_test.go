package source

import (
	"errors"
	"testing"

	"github.com/home-operations/flate/pkg/manifest"
)

func TestResolveTransport_UnavailableMaterial(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name    string
		data    map[string]any
		cert    bool
		missing bool
		want    string
	}{
		{"wiped CA", map[string]any{"ca.crt": "..PLACEHOLDER_ca.crt.."}, true, true, "flux error: missing secret: OCIRepository ns/repo: secret ns/creds TLS material unavailable"},
		{"mixed cert pair", map[string]any{"tls.crt": "real", "tls.key": "..PLACEHOLDER_tls.key.."}, true, true, "flux error: missing secret: OCIRepository ns/repo: secret ns/creds TLS material unavailable"},
		{"empty TLS", nil, true, true, "flux error: missing secret: OCIRepository ns/repo: secret ns/creds TLS material unavailable"},
		{"real incomplete pair", map[string]any{"tls.crt": "real"}, true, false, "OCIRepository repo: certSecretRef ns/creds: must provide both tls.crt and tls.key together"},
		{"real invalid CA", map[string]any{"ca.crt": "invalid PEM"}, true, false, "OCIRepository repo: certSecretRef ns/creds: ca.crt did not parse as PEM"},
		{"real proxy credentials without address", map[string]any{"username": "u", "password": "p"}, false, false, "flux error: input error: proxy secret ns/creds missing required 'address' key"},
		{"empty proxy", nil, false, true, "flux error: missing secret: OCIRepository ns/repo: secret ns/creds missing required 'address' key"},
		{"wiped proxy", map[string]any{"address": "..PLACEHOLDER_address.."}, false, true, "flux error: missing secret: OCIRepository ns/repo: secret ns/creds proxy material unavailable"},
		{"wiped proxy password", map[string]any{"address": "http://proxy", "password": "..PLACEHOLDER_password.."}, false, true, "flux error: missing secret: OCIRepository ns/repo: secret ns/creds proxy material unavailable"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var lookup [2]string
			calls := 0
			getter := func(ns, name string) *manifest.Secret {
				lookup = [2]string{ns, name}
				calls++
				return &manifest.Secret{Name: "metadata-secret", Namespace: "elsewhere", StringData: tc.data}
			}
			owner := manifest.NamedResource{Group: manifest.SourceDomain, Kind: manifest.KindOCIRepository, Namespace: "ns", Name: "repo"}
			ref := &manifest.LocalObjectReference{Name: "creds"}
			var err error
			if tc.cert {
				_, err = ResolveCertSecret(getter, owner, ref)
			} else {
				_, err = ResolveProxy(getter, owner, ref)
			}
			if missing, ok := errors.AsType[*MissingSecretError](err); ok {
				if missing.Owner != owner || missing.Secret != (manifest.NamedResource{Kind: manifest.KindSecret, Namespace: "ns", Name: "creds"}) {
					t.Fatalf("credential identities = %+v", missing)
				}
			}
			if err == nil || errors.Is(err, manifest.ErrMissingSecret) != tc.missing || err.Error() != tc.want {
				t.Fatalf("missing=%t, error=%v", tc.missing, err)
			}
			if lookup != [2]string{"ns", "creds"} || calls != 1 {
				t.Fatalf("lookup = %v (%d calls)", lookup, calls)
			}
		})
	}
}
