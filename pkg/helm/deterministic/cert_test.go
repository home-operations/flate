package deterministic

import (
	"crypto/ed25519"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"testing"
	"time"
)

func mustParseCert(t *testing.T, pemStr string) *x509.Certificate {
	t.Helper()
	block, _ := pem.Decode([]byte(pemStr))
	if block == nil {
		t.Fatalf("no PEM data in certificate:\n%s", pemStr)
	}
	c, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		t.Fatalf("parsing certificate: %v", err)
	}
	return c
}

func mustParseEd25519Key(t *testing.T, pemStr string) ed25519.PrivateKey {
	t.Helper()
	block, _ := pem.Decode([]byte(pemStr))
	if block == nil {
		t.Fatalf("no PEM data in key:\n%s", pemStr)
	}
	k, err := x509.ParsePKCS8PrivateKey(block.Bytes)
	if err != nil {
		t.Fatalf("parsing PKCS#8 key: %v", err)
	}
	ed, ok := k.(ed25519.PrivateKey)
	if !ok {
		t.Fatalf("key is %T, want ed25519.PrivateKey", k)
	}
	return ed
}

func genCAFn(t *testing.T, fm map[string]any) func(string, int) (certificate, error) {
	t.Helper()
	fn, ok := fm["genCA"].(func(string, int) (certificate, error))
	if !ok {
		t.Fatalf("genCA override has unexpected type %T", fm["genCA"])
	}
	return fn
}

// TestCertFuncs_DeterministicAndValid pins the cert tier: a CA and a leaf signed
// by it render byte-identically for a given seed, are valid ed25519 x509 with the
// FixedTime window, and the leaf verifies against the CA — so caBundle/tls.crt
// stop churning while staying internally consistent.
func TestCertFuncs_DeterministicAndValid(t *testing.T) {
	fm := Funcs(SeedFor("rel", "ns"))
	ca, err := genCAFn(t, fm)("my-ca", 365)
	if err != nil {
		t.Fatalf("genCA: %v", err)
	}

	genSigned, ok := fm["genSignedCert"].(func(string, []any, []any, int, certificate) (certificate, error))
	if !ok {
		t.Fatalf("genSignedCert override has unexpected type %T", fm["genSignedCert"])
	}
	leaf, err := genSigned("leaf.example.com", []any{"10.0.0.1"}, []any{"leaf.example.com"}, 365, ca)
	if err != nil {
		t.Fatalf("genSignedCert: %v", err)
	}

	caCert := mustParseCert(t, ca.Cert)
	if !caCert.IsCA {
		t.Error("genCA cert is not marked IsCA")
	}
	if caCert.SignatureAlgorithm != x509.PureEd25519 {
		t.Errorf("CA SignatureAlgorithm = %v, want PureEd25519 (a CustomReader-tainted algorithm would be nondeterministic)", caCert.SignatureAlgorithm)
	}
	if !caCert.NotBefore.Equal(FixedTime) {
		t.Errorf("CA NotBefore = %v, want FixedTime %v", caCert.NotBefore, FixedTime)
	}
	if want := FixedTime.Add(365 * 24 * time.Hour); !caCert.NotAfter.Equal(want) {
		t.Errorf("CA NotAfter = %v, want %v", caCert.NotAfter, want)
	}

	// Chain consistency: the leaf must be signed by the CA in caBundle.
	leafCert := mustParseCert(t, leaf.Cert)
	if err := leafCert.CheckSignatureFrom(caCert); err != nil {
		t.Errorf("leaf is not signed by the generated CA: %v", err)
	}

	// Keys round-trip as ed25519.
	mustParseEd25519Key(t, ca.Key)
	mustParseEd25519Key(t, leaf.Key)

	// Same seed → byte-identical CA; different seed → different.
	again, err := genCAFn(t, Funcs(SeedFor("rel", "ns")))("my-ca", 365)
	if err != nil {
		t.Fatalf("genCA again: %v", err)
	}
	if again.Cert != ca.Cert || again.Key != ca.Key {
		t.Error("same seed produced a different CA (genCA not deterministic)")
	}
	other, err := genCAFn(t, Funcs(SeedFor("other", "ns")))("my-ca", 365)
	if err != nil {
		t.Fatalf("genCA other: %v", err)
	}
	if other.Cert == ca.Cert {
		t.Error("different seeds produced an identical CA")
	}
}

// TestGenSignedCert_RejectsNonEd25519CA pins the WithKey/parseCA boundary: a CA
// whose key is not ed25519 (everything this package generates is) is rejected
// with a clear error rather than producing a nondeterministic signature.
func TestGenSignedCert_RejectsNonEd25519CA(t *testing.T) {
	// A valid ed25519 CA cert, but paired with an RSA key.
	fm := Funcs(SeedFor("rel", "ns"))
	ca, err := genCAFn(t, fm)("my-ca", 365)
	if err != nil {
		t.Fatalf("genCA: %v", err)
	}
	rsaKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("rsa.GenerateKey: %v", err)
	}
	der, err := x509.MarshalPKCS8PrivateKey(rsaKey)
	if err != nil {
		t.Fatalf("MarshalPKCS8: %v", err)
	}
	ca.Key = string(pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: der}))

	genSigned := fm["genSignedCert"].(func(string, []any, []any, int, certificate) (certificate, error))
	if _, err := genSigned("leaf", nil, nil, 365, ca); err == nil {
		t.Error("genSignedCert accepted a non-ed25519 CA key; want an error")
	}
}

// TestGenPrivateKey_ParsesPerType pins that every supported key type renders a
// parseable deterministic ed25519 key. stdlib cannot make rsa/ecdsa keygen
// deterministic, so those collapse to ed25519; "dsa" keeps sprig's message.
func TestGenPrivateKey_ParsesPerType(t *testing.T) {
	fm := Funcs(SeedFor("rel", "ns"))
	gpk, ok := fm["genPrivateKey"].(func(string) string)
	if !ok {
		t.Fatalf("genPrivateKey override has unexpected type %T", fm["genPrivateKey"])
	}
	for _, typ := range []string{"", "rsa", "ecdsa", "ed25519"} {
		mustParseEd25519Key(t, gpk(typ))
	}
	if got := gpk("dsa"); got != "Unknown type dsa" {
		t.Errorf("genPrivateKey(\"dsa\") = %q, want unknown-type message", got)
	}

	a := Funcs(SeedFor("rel", "ns"))["genPrivateKey"].(func(string) string)("rsa")
	b := Funcs(SeedFor("rel", "ns"))["genPrivateKey"].(func(string) string)("rsa")
	if a != b {
		t.Error("same seed produced different keys")
	}
}
