package source

import (
	"crypto/tls"
	"strings"
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
)

func TestBuildTLSConfig(t *testing.T) {
	t.Parallel()
	crt, key := testutil.SelfSignedClientCert(t)
	ca := testutil.SelfSignedCA(t)
	for _, tc := range []struct {
		name, crt, key, ca, wantError string
		certificates                  int
	}{
		{name: "empty", wantError: "no TLS material"},
		{name: "cert without key", crt: "-cert-", wantError: "must provide both"},
		{name: "key without cert", key: "-key-", wantError: "must provide both"},
		{name: "CA only", ca: ca},
		{name: "full material", crt: crt, key: key, ca: ca, certificates: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			cfg, err := BuildTLSConfig(tc.crt, tc.key, tc.ca)
			if tc.wantError != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantError) {
					t.Fatalf("BuildTLSConfig: %v, want %q", err, tc.wantError)
				}
				return
			}
			assert.NoError(t, err)
			if cfg.RootCAs == nil {
				t.Fatal("expected RootCAs populated from ca.crt")
			}
			assert.Equal(t, len(cfg.Certificates), tc.certificates)
			assert.Equal(t, cfg.MinVersion, uint16(tls.VersionTLS12))
		})
	}
}
