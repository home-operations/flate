package source

import (
	"bytes"
	"crypto/ecdh"
	"crypto/ecdsa"
	"crypto/mldsa"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"strings"
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
)

func TestBuildTLSConfig(t *testing.T) {
	t.Parallel()
	crt, key := testutil.SelfSignedClientCert(t)
	_, otherKey := testutil.SelfSignedClientCert(t)
	ca := testutil.SelfSignedCA(t)
	encode := func(kind string, der []byte) string {
		return string(pem.EncodeToMemory(&pem.Block{Type: kind, Bytes: der}))
	}
	crtBlock, _ := pem.Decode([]byte(crt))
	keyBlock, _ := pem.Decode([]byte(key))
	ecKey, err := x509.ParsePKCS8PrivateKey(keyBlock.Bytes)
	assert.NoError(t, err)
	ecDER, err := x509.MarshalECPrivateKey(ecKey.(*ecdsa.PrivateKey))
	assert.NoError(t, err)
	rsaKey, err := rsa.GenerateKey(rand.Reader, 2048)
	assert.NoError(t, err)
	xKey, err := ecdh.X25519().GenerateKey(rand.Reader)
	assert.NoError(t, err)
	unsupportedKey, err := x509.MarshalPKCS8PrivateKey(xKey)
	assert.NoError(t, err)
	// An unknown public-key OID is valid certificate DER but cannot form a TLS pair.
	unsupportedCert := bytes.Replace(crtBlock.Bytes,
		[]byte{6, 7, 42, 134, 72, 206, 61, 2, 1}, []byte{6, 7, 42, 134, 72, 206, 61, 2, 2}, 1)
	leaf, err := x509.ParseCertificate(unsupportedCert)
	assert.NoError(t, err)
	assert.Equal(t, leaf.PublicKeyAlgorithm, x509.UnknownPublicKeyAlgorithm)
	mlKey, err := mldsa.GenerateKey(mldsa.MLDSA44())
	assert.NoError(t, err)
	mlDER, err := x509.MarshalPKCS8PrivateKey(mlKey)
	assert.NoError(t, err)
	leaf, err = x509.ParseCertificate(crtBlock.Bytes)
	assert.NoError(t, err)
	mlCert, err := x509.CreateCertificate(rand.Reader, leaf, leaf, mlKey.PublicKey(), ecKey)
	assert.NoError(t, err)
	wiped := "..PLACEHOLDER_material.."
	for _, tc := range []struct {
		name, crt, key, ca, wantError string
		certificates                  int
		missing                       bool
	}{
		{name: "empty", wantError: "no TLS material"},
		{name: "cert without key", crt: "-cert-", wantError: "must provide both"},
		{name: "key without cert", key: "-key-", wantError: "must provide both"},
		{name: "CA only", ca: ca},
		{name: "full material", crt: crt, key: key, ca: ca, certificates: 1},
		{name: "pair without CA", crt: crt, key: key, certificates: 1},
		{name: "invalid CA wiped client", crt: wiped, ca: "invalid-tls-marker", wantError: "ca.crt"},
		{name: "PEM label sanitized", crt: encode("credential-tls-marker", []byte("fixture")), key: key, ca: wiped, wantError: "tls.crt/tls.key"},
		{name: "mismatched pair wiped CA", crt: crt, key: otherKey, ca: wiped, wantError: "tls.crt/tls.key"},
		{name: "invalid cert wiped key", crt: "invalid-tls-marker", key: wiped, wantError: "PEM"},
		{name: "invalid key wiped cert", crt: wiped, key: "invalid-tls-marker", wantError: "PEM"},
		{name: "invalid cert DER wiped key", crt: encode("CERTIFICATE", []byte("invalid-tls-marker")), key: wiped, wantError: "tls.crt"},
		{name: "invalid key DER wiped cert", crt: wiped, key: encode("PRIVATE KEY", []byte("invalid-tls-marker")), wantError: "tls.key"},
		{name: "valid cert wiped key", crt: crt, key: wiped, missing: true},
		{name: "valid key wiped cert", crt: wiped, key: key, missing: true},
		{name: "valid CA wiped client", key: wiped, ca: ca, missing: true},
		{name: "wiped CA", ca: wiped, missing: true},
		{name: "wiped cert absent key", crt: wiped, missing: true},
		{name: "cert absent key wiped CA", crt: crt, ca: wiped, wantError: "must provide both"},
		{name: "key absent cert wiped CA", key: key, ca: wiped, wantError: "must provide both"},
		{name: "cert selection", crt: encode("OTHER", []byte("ignored")) + crt, key: wiped, missing: true},
		{name: "key selection", crt: wiped, key: crt + key, missing: true},
		{name: "cert leaf only", crt: crt + encode("CERTIFICATE", []byte("invalid-tls-marker")), key: wiped, missing: true},
		{name: "first selected cert invalid", crt: encode("CERTIFICATE", []byte("invalid-tls-marker")) + crt, key: wiped, wantError: "tls.crt"},
		{name: "first selected key invalid", crt: wiped, key: encode("PRIVATE KEY", []byte("invalid-tls-marker")) + key, wantError: "tls.key"},
		{name: "cert requires certificate block", crt: key, key: wiped, wantError: "PEM"},
		{name: "key requires private key block", crt: wiped, key: crt, wantError: "PEM"},
		{name: "unsupported public algorithm", crt: encode("CERTIFICATE", unsupportedCert), key: wiped, wantError: "unsupported public"},
		{name: "unsupported private algorithm", crt: wiped, key: encode("PRIVATE KEY", unsupportedKey), wantError: "unsupported private"},
		{name: "PKCS1 key wiped cert", crt: wiped, key: encode("RSA PRIVATE KEY", x509.MarshalPKCS1PrivateKey(rsaKey)), missing: true},
		{name: "SEC1 key wiped cert", crt: wiped, key: encode("EC PRIVATE KEY", ecDER), missing: true},
		{name: "MLDSA key wiped cert", crt: wiped, key: encode("PRIVATE KEY", mlDER), missing: true},
		{name: "MLDSA cert wiped key", crt: encode("CERTIFICATE", mlCert), key: wiped, missing: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			cfg, err := buildTLSConfig(tc.crt, tc.key, tc.ca)
			if tc.wantError != "" || tc.missing {
				assert.Equal(t, cfg == nil, true)
				assert.Equal(t, errors.Is(err, manifest.ErrFlux), true)
				assert.Equal(t, errors.Is(err, manifest.ErrInput), !tc.missing)
				assert.Equal(t, errors.Is(err, manifest.ErrMissingSecret), tc.missing)
				if err == nil || !strings.Contains(err.Error(), tc.wantError) {
					t.Fatalf("buildTLSConfig: %v, want %q", err, tc.wantError)
				}
				if strings.Contains(err.Error(), "invalid-tls-marker") || strings.Contains(err.Error(), "credential-tls-marker") {
					t.Fatal("TLS material leaked")
				}
				return
			}
			assert.NoError(t, err)
			assert.Equal(t, cfg.RootCAs != nil, tc.ca != "")
			assert.Equal(t, len(cfg.Certificates), tc.certificates)
			assert.Equal(t, cfg.MinVersion, uint16(tls.VersionTLS12))
		})
	}
}
