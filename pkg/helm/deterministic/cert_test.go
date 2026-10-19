package deterministic

import (
	"crypto/ed25519"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"math/big"
	"testing"
	"time"

	"github.com/home-operations/flate/internal/assert"
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

func TestCertFuncs_DeterministicAndValid(t *testing.T) {
	t.Parallel()
	fm := Funcs(SeedFor("rel", "ns"))
	ca, err := genCAFn(t, fm)("my-ca", 365)
	if err != nil {
		t.Fatalf("genCA: %v", err)
	}

	caCert := mustParseCert(t, ca.Cert)
	if !caCert.IsCA {
		t.Error("genCA cert is not marked IsCA")
	}
	if caCert.SignatureAlgorithm != x509.PureEd25519 {
		t.Errorf("CA SignatureAlgorithm = %v, want PureEd25519 (a CustomReader-tainted algorithm would be nondeterministic)", caCert.SignatureAlgorithm)
	}
	if !caCert.NotBefore.Equal(fixedTime) {
		t.Errorf("CA NotBefore = %v, want fixedTime %v", caCert.NotBefore, fixedTime)
	}
	if want := fixedTime.Add(365 * 24 * time.Hour); !caCert.NotAfter.Equal(want) {
		t.Errorf("CA NotAfter = %v, want %v", caCert.NotAfter, want)
	}

	mustParseEd25519Key(t, ca.Key)

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

	for _, tt := range []struct {
		name string
		cfg  certConfig
	}{
		{
			name: "genSelfSignedCert",
			cfg: certConfig{
				cn: "self.example.com", ips: []any{"10.0.0.7", "2001:db8::7"},
				alternateDNS: []any{"self-alt.example.com", "self-other.example.com"}, days: 17,
			},
		},
		{
			name: "genSignedCert",
			cfg: certConfig{
				cn: "leaf.example.com", ips: []any{"192.0.2.9", "2001:db8::9"},
				alternateDNS: []any{"leaf-alt.example.com", "leaf-other.example.com"}, days: 29,
			},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			seed := SeedFor(tt.name, "ns")
			expectedStream := newStream(seed)
			pub, priv, err := ed25519.GenerateKey(expectedStream)
			assert.NoError(t, err)
			serial, err := rand.Int(expectedStream, new(big.Int).Lsh(big.NewInt(1), 128))
			assert.NoError(t, err)
			var gotNext, wantNext [32]byte
			_, err = expectedStream.Read(wantNext[:])
			assert.NoError(t, err)

			s := newStream(seed)
			fm := make(map[string]any)
			certFuncs(s, fm)
			cfg := tt.cfg
			var got, want certificate
			if tt.name == "genSignedCert" {
				got, err = fm[tt.name].(func(string, []any, []any, int, certificate) (certificate, error))(cfg.cn, cfg.ips, cfg.alternateDNS, cfg.days, ca)
				assert.NoError(t, err)
				want, err = genSignedCert(newStream(seed), cfg, ca)
			} else {
				got, err = fm[tt.name].(func(string, []any, []any, int) (certificate, error))(cfg.cn, cfg.ips, cfg.alternateDNS, cfg.days)
				assert.NoError(t, err)
				want, err = genSelfSignedCert(newStream(seed), cfg)
			}
			assert.NoError(t, err)
			assert.Equal(t, got, want)
			_, err = s.Read(gotNext[:])
			assert.NoError(t, err)
			assert.Equal(t, gotNext, wantNext)

			for _, key := range []string{"not a PEM key", ca.Key} {
				withKeyStream := newStream(seed)
				withKeyFuncs := make(map[string]any)
				certFuncs(withKeyStream, withKeyFuncs)
				withKey := withKeyFuncs[tt.name+"WithKey"]
				if tt.name == "genSignedCert" {
					got, err = withKey.(func(string, []any, []any, int, certificate, string) (certificate, error))(cfg.cn, cfg.ips, cfg.alternateDNS, cfg.days, ca, key)
				} else {
					got, err = withKey.(func(string, []any, []any, int, string) (certificate, error))(cfg.cn, cfg.ips, cfg.alternateDNS, cfg.days, key)
				}
				assert.NoError(t, err)
				assert.Equal(t, got, want)
				_, err = withKeyStream.Read(gotNext[:])
				assert.NoError(t, err)
				assert.Equal(t, gotNext, wantNext)
			}

			cert := mustParseCert(t, got.Cert)
			assert.Equal(t, cert.Subject.CommonName, cfg.cn)
			ips, dns := []any{}, []any{}
			for _, ip := range cert.IPAddresses {
				ips = append(ips, ip.String())
			}
			for _, name := range cert.DNSNames {
				dns = append(dns, name)
			}
			assert.Diff(t, ips, cfg.ips)
			assert.Diff(t, dns, cfg.alternateDNS)
			assert.Equal(t, cert.NotBefore, fixedTime)
			assert.Equal(t, cert.NotAfter, fixedTime.Add(24*time.Hour*time.Duration(cfg.days)))
			assert.Equal(t, cert.SignatureAlgorithm, x509.PureEd25519)
			if tt.name == "genSignedCert" {
				assert.NoError(t, cert.CheckSignatureFrom(caCert))
			} else {
				assert.NoError(t, cert.CheckSignature(cert.SignatureAlgorithm, cert.RawTBSCertificate, cert.Signature))
			}

			assert.Diff(t, mustParseEd25519Key(t, got.Key), priv)
			assert.Diff(t, cert.PublicKey, pub)
			assert.Equal(t, cert.SerialNumber.Cmp(serial), 0)
		})
	}
}

func TestGenSignedCert_RejectsNonEd25519CA(t *testing.T) {
	t.Parallel()
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
	for _, tt := range []struct {
		name string
		ca   certificate
		ips  []any
		want string
	}{
		{name: "non-ed25519 key", ca: ca, want: "CA key is *rsa.PrivateKey, want ed25519"},
		{name: "non-ed25519 key before invalid IP", ca: ca, ips: []any{"invalid ip"}, want: "CA key is *rsa.PrivateKey, want ed25519"},
		{name: "invalid certificate before invalid IP", ca: certificate{Cert: "not PEM"}, ips: []any{"invalid ip"}, want: "unable to decode CA certificate PEM"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			_, err := genSigned("leaf", tt.ips, nil, 365, tt.ca)
			if err == nil {
				t.Fatal("genSignedCert accepted an invalid CA")
			}
			assert.Equal(t, err.Error(), tt.want)
		})
	}
}

// TestGenPrivateKey_ParsesPerType pins that every supported key type renders a
// parseable deterministic ed25519 key. stdlib cannot make rsa/ecdsa keygen
// deterministic, so those collapse to ed25519; "dsa" also uses the offline ed25519 stand-in.
func TestGenPrivateKey_ParsesPerType(t *testing.T) {
	t.Parallel()
	fm := Funcs(SeedFor("rel", "ns"))
	gpk, ok := fm["genPrivateKey"].(func(string) string)
	if !ok {
		t.Fatalf("genPrivateKey override has unexpected type %T", fm["genPrivateKey"])
	}
	for _, typ := range []string{"", "rsa", "ecdsa", "ed25519", "dsa"} {
		mustParseEd25519Key(t, gpk(typ))
	}
	if got := gpk("unknown"); got != "Unknown type unknown" {
		t.Errorf("genPrivateKey(\"unknown\") = %q, want unknown-type message", got)
	}

	a := Funcs(SeedFor("rel", "ns"))["genPrivateKey"].(func(string) string)("rsa")
	b := Funcs(SeedFor("rel", "ns"))["genPrivateKey"].(func(string) string)("rsa")
	if a != b {
		t.Error("same seed produced different keys")
	}
}
