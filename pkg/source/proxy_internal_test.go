package source

import (
	"errors"
	"net/url"
	"testing"

	"github.com/home-operations/flate/pkg/store"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/pkg/manifest"
)

func TestResolveProxy(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name, kind, wantError string
		secret                *manifest.Secret
		unwired               bool
	}{
		{
			name: "unwired Git", kind: manifest.KindGitRepository, unwired: true,
			wantError: "references proxySecretRef but no source.SecretGetter is wired",
		},
		{
			name: "unwired Helm", kind: manifest.KindHelmRepository, unwired: true,
			wantError: "references proxySecretRef but no source.SecretGetter is wired",
		},
		{
			name: "absent", kind: manifest.KindOCIRepository,
			wantError: "secret ns/px not found",
		},
		{
			name: "missing address", kind: manifest.KindBucket,
			secret:    &manifest.Secret{StringData: map[string]any{"username": "alice"}},
			wantError: "proxy secret ns/px missing required 'address' key",
		},
		{
			name: "malformed address empty password", kind: manifest.KindGitRepository,
			secret:    &manifest.Secret{StringData: map[string]any{"address": "http://proxy-credential-marker@proxy.invalid/%", "password": ""}},
			wantError: "proxySecretRef ns/px: invalid proxy address",
		},
		{
			name: "full secret", kind: manifest.KindGitRepository,
			secret: &manifest.Secret{StringData: map[string]any{"address": "http://proxy.example.com:8080", "username": "alice", "password": "hunter2"}},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			testResolveProxyCase1(t, tc)
		})
	}
}

func testResolveProxyCase1(t *testing.T, tc struct {
	name      string
	kind      string
	wantError string
	secret    *manifest.Secret
	unwired   bool
},
) {
	t.Helper()

	var lookup [2]string
	calls := 0
	var getter SecretGetter
	if !tc.unwired {
		getter = func(ns, name string) store.ReferenceFacts {
			lookup = [2]string{ns, name}
			calls++

			secretInput := tc.secret
			if secretInput == nil {
				return store.ReferenceFacts{}
			}
			return store.ReferenceFacts{Data: secretInput, Original: secretInput}
		}
	}
	owner := manifest.NamedResource{Group: manifest.SourceDomain, Kind: tc.kind, Namespace: "ns", Name: "r"}
	material, err := ResolveTransport(
		getter,
		manifest.DiagnosticFor(owner, nil),
		[]TransportInput{{Reference: &manifest.LocalObjectReference{Name: "px"}, Use: TransportProxy}},
	)
	if !tc.unwired {
		assert.Equal(t, lookup, [2]string{"ns", "px"})
		assert.Equal(t, calls, 1)
	}
	if tc.wantError != "" {
		assert.FatalIf(t, err == nil || err.Error() != tc.wantError,
			"ResolveProxy: %v, want %q", err, tc.wantError)

		if !tc.unwired && tc.secret == nil {
			missing, ok := errors.AsType[*MissingSecretError](err)
			assert.FatalIf(t, !ok || !errors.Is(err, manifest.ErrMissingSecret),
				"expected missing-secret error: %v", err)

			assert.Equal(t, missing.Owner, owner)
			assert.Equal(
				t,
				missing.Secret,
				manifest.NamedResource{Kind: manifest.KindSecret, Namespace: "ns", Name: "px"},
			)
		}
		return
	}
	assert.NoError(t, err)
	assert.FatalIf(t, material.proxy == nil, "explicit proxy material is absent")
	assert.Equal(
		t,
		[3]string{material.proxy.Address, material.proxy.Username, material.proxy.Password},
		[3]string{"http://proxy.example.com:8080", "alice", "hunter2"},
	)
	selected, err := material.ProxyURL(&url.URL{Scheme: "http", Host: "target.invalid"})
	assert.NoError(t, err)
	assert.Equal(t, selected.User.String(), "alice:hunter2")
}
