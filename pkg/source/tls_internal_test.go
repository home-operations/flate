package source

import (
	"crypto/tls"
	"errors"
	"strings"
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
)

func TestBuildTLSConfig(t *testing.T) {
	t.Parallel()
	migratedResult1 := testutil.SelfSignedClientCert(t)
	crt, key := migratedResult1.Cert, migratedResult1.Key
	ca := testutil.SelfSignedCA(t)
	for _, tc := range []struct {
		name, crt, key, ca, wantError string
		certificates                  int
	}{
		{name: "empty", wantError: "no TLS material"},
		{name: "cert without key", crt: crt, wantError: "must provide both"},
		{name: "key without cert", key: key, wantError: "must provide both"},
		{name: "CA only", ca: ca},
		{name: "full material", crt: crt, key: key, ca: ca, certificates: 1},
		{name: "pair without CA", crt: crt, key: key, certificates: 1},
		{name: "invalid CA", ca: "invalid", wantError: "ca.crt"},
		{name: "invalid pair", crt: "invalid", key: key, wantError: "tls.crt/tls.key"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			cfg, err := buildTLSConfig(tc.crt, tc.key, tc.ca)
			if tc.wantError != "" {
				assert.FatalIf(t, !errors.Is(err, manifest.ErrInput) || !strings.Contains(err.Error(), tc.wantError),
					"TLS error=%v, want %q", err, tc.wantError)

				return
			}
			assert.NoError(t, err)
			assert.Equal(t, cfg.MinVersion, uint16(tls.VersionTLS12))
			assert.Equal(t, len(cfg.Certificates), tc.certificates)
			assert.Equal(t, cfg.RootCAs != nil, tc.ca != "")
		})
	}
}
