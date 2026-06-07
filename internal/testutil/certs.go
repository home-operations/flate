package testutil

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"testing"
	"time"
)

// certValidity is the lifetime stamped on every test cert. 24h
// generously covers any test run (including long-running integration
// tests against frozen clocks) while still bounding the practical
// window for a leaked fixture if someone pastes the cert somewhere.
const certValidity = 24 * time.Hour

// randomSerial returns a 128-bit random serial — different on every
// invocation so the CA + server + client cert templates produced by
// this package don't share serial numbers. Two certs with the same
// (Issuer, Serial) tuple can short-circuit a verify-cache hit and
// mask a real chain bug in the system under test.
func randomSerial(t *testing.T) *big.Int {
	t.Helper()
	limit := new(big.Int).Lsh(big.NewInt(1), 128)
	n, err := rand.Int(rand.Reader, limit)
	if err != nil {
		t.Fatalf("serial: %v", err)
	}
	return n
}

// ecKey generates an ephemeral ECDSA P-256 key. P-256 is accepted by
// tls.X509KeyPair and x509.NewCertPool; it is ~20x faster to generate
// than RSA-2048 at equivalent security, which matters in tests that
// call these helpers in tight loops.
func ecKey(t *testing.T) *ecdsa.PrivateKey {
	t.Helper()
	priv, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("ecdsa: %v", err)
	}
	return priv
}

// pemKey encodes an ECDSA private key as a PKCS#8 PEM block.
func pemKey(t *testing.T, priv *ecdsa.PrivateKey) string {
	t.Helper()
	der, err := x509.MarshalPKCS8PrivateKey(priv)
	if err != nil {
		t.Fatalf("MarshalPKCS8PrivateKey: %v", err)
	}
	return string(pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: der}))
}

// makeCertificate builds a self-signed cert (signed by its own
// ephemeral ECDSA key) with the given subject CN and usage flags,
// returning the cert and key as PEM. The shared boilerplate — serial,
// validity window, key generation, CreateCertificate, PEM encoding —
// lives here; the exported helpers pass only the fields that vary.
func makeCertificate(t *testing.T, cn string, keyUsage x509.KeyUsage, extKeyUsage []x509.ExtKeyUsage, isCA bool) (cert, key string) {
	t.Helper()
	priv := ecKey(t)
	tmpl := &x509.Certificate{
		SerialNumber: randomSerial(t),
		Subject:      pkix.Name{CommonName: cn},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(certValidity),
		KeyUsage:     keyUsage,
		ExtKeyUsage:  extKeyUsage,
		IsCA:         isCA,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &priv.PublicKey, priv)
	if err != nil {
		t.Fatalf("CreateCertificate: %v", err)
	}
	return string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})), pemKey(t, priv)
}

// SelfSignedCA returns a PEM-encoded CA certificate.
func SelfSignedCA(t *testing.T) string {
	t.Helper()
	cert, _ := makeCertificate(t, "flate-test-ca",
		x509.KeyUsageCertSign|x509.KeyUsageDigitalSignature, nil, true)
	return cert
}

// SelfSignedServerCert returns a PEM-encoded server+client cert and
// matching ECDSA private key — usable as both a TLS server cert and a
// CA bundle (IsCA=true).
func SelfSignedServerCert(t *testing.T) (cert, key string) {
	t.Helper()
	return makeCertificate(t, "flate-test",
		x509.KeyUsageDigitalSignature|x509.KeyUsageCertSign,
		[]x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth}, true)
}

// SelfSignedClientCert returns a PEM-encoded client cert and matching
// ECDSA private key — ExtKeyUsage is ClientAuth only.
func SelfSignedClientCert(t *testing.T) (cert, key string) {
	t.Helper()
	return makeCertificate(t, "flate-test-client",
		x509.KeyUsageDigitalSignature,
		[]x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}, false)
}
