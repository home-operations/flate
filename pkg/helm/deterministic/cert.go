package deterministic

import (
	"crypto/ed25519"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"encoding/pem"
	"errors"
	"fmt"
	"math/big"
	"net"
	"text/template"
	"time"
)

// certFuncs returns deterministic overrides for sprig's certificate and
// private-key generators. All generated material is ed25519: ed25519.GenerateKey
// reads the seeded stream directly (unlike rsa/ecdsa.GenerateKey, which route
// through crypto/rand's CustomReader and discard a custom reader), and ed25519
// signatures are deterministic (RFC 8032). Combined with a stream-derived serial
// and FixedTime validity, every cert/key renders identically run to run — with
// no hand-rolled crypto and no seeded reader handed to x509.CreateCertificate.
//
// flate renders for offline review and never applies its output, so an ed25519
// caBundle/key is a valid, deterministic stand-in for the random RSA material the
// live controller mints at apply time (which a render could never match anyway).
//
// genPrivateKey returns a deterministic ed25519 key for every known type — stdlib
// cannot make rsa/ecdsa keygen deterministic. The *WithKey variants keep their
// signatures but ignore the supplied PEM and generate an ed25519 key, so output
// stays deterministic regardless of the caller's key type.
func certFuncs(s *stream) template.FuncMap {
	return template.FuncMap{
		"genPrivateKey":   func(typ string) string { return genPrivateKey(s, typ) },
		"buildCustomCert": buildCustomCert,
		"genCA":           func(cn string, days int) (certificate, error) { return genCA(s, cn, days) },
		"genCAWithKey":    func(cn string, days int, _ string) (certificate, error) { return genCA(s, cn, days) },
		"genSelfSignedCert": func(cn string, ips, alternateDNS []any, days int) (certificate, error) {
			return genSelfSignedCert(s, cn, ips, alternateDNS, days)
		},
		"genSelfSignedCertWithKey": func(cn string, ips, alternateDNS []any, days int, _ string) (certificate, error) {
			return genSelfSignedCert(s, cn, ips, alternateDNS, days)
		},
		"genSignedCert": func(cn string, ips, alternateDNS []any, days int, ca certificate) (certificate, error) {
			return genSignedCert(s, cn, ips, alternateDNS, days, ca)
		},
		"genSignedCertWithKey": func(cn string, ips, alternateDNS []any, days int, ca certificate, _ string) (certificate, error) {
			return genSignedCert(s, cn, ips, alternateDNS, days, ca)
		},
	}
}

// certificate mirrors sprig's type: an unexported type with exported fields, so
// templates read .Cert/.Key and a genCA value flows into genSignedCert's ca arg.
type certificate struct {
	Cert string
	Key  string
}

// issue generates a deterministic ed25519 key, finishes the template (serial from
// the stream, validity from FixedTime), signs it, and PEM-encodes both. A nil
// signerKey means self-signed (parent and signer are the freshly generated cert
// and key). x509.CreateCertificate is handed the stream, but never reads it: the
// serial is pre-set and ed25519 signing consumes no randomness.
func issue(s *stream, tmpl, parent *x509.Certificate, signerKey ed25519.PrivateKey, days int) (certificate, error) {
	pub, priv, err := ed25519.GenerateKey(s)
	if err != nil {
		return certificate{}, fmt.Errorf("generating ed25519 key: %w", err)
	}
	serial, err := rand.Int(s, new(big.Int).Lsh(big.NewInt(1), 128))
	if err != nil {
		return certificate{}, fmt.Errorf("generating serial: %w", err)
	}
	tmpl.SerialNumber = serial
	tmpl.NotBefore = FixedTime
	tmpl.NotAfter = FixedTime.Add(24 * time.Hour * time.Duration(days))
	if signerKey == nil {
		parent, signerKey = tmpl, priv
	}
	der, err := x509.CreateCertificate(s, tmpl, parent, pub, signerKey)
	if err != nil {
		return certificate{}, fmt.Errorf("creating certificate: %w", err)
	}
	keyDER, err := x509.MarshalPKCS8PrivateKey(priv)
	if err != nil {
		return certificate{}, fmt.Errorf("marshaling key: %w", err)
	}
	return certificate{
		Cert: string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})),
		Key:  string(pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER})),
	}, nil
}

func baseTemplate(cn string, ips, alternateDNS []any) (*x509.Certificate, error) {
	ipAddresses, err := netIPs(ips)
	if err != nil {
		return nil, err
	}
	dnsNames, err := dnsStrs(alternateDNS)
	if err != nil {
		return nil, err
	}
	return &x509.Certificate{
		Subject:               pkix.Name{CommonName: cn},
		IPAddresses:           ipAddresses,
		DNSNames:              dnsNames,
		KeyUsage:              x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		BasicConstraintsValid: true,
	}, nil
}

func genCA(s *stream, cn string, days int) (certificate, error) {
	tmpl, err := baseTemplate(cn, nil, nil)
	if err != nil {
		return certificate{}, err
	}
	tmpl.IsCA = true
	tmpl.KeyUsage |= x509.KeyUsageCertSign
	return issue(s, tmpl, nil, nil, days)
}

func genSelfSignedCert(s *stream, cn string, ips, alternateDNS []any, days int) (certificate, error) {
	tmpl, err := baseTemplate(cn, ips, alternateDNS)
	if err != nil {
		return certificate{}, err
	}
	return issue(s, tmpl, nil, nil, days)
}

func genSignedCert(s *stream, cn string, ips, alternateDNS []any, days int, ca certificate) (certificate, error) {
	parent, signerKey, err := parseCA(ca)
	if err != nil {
		return certificate{}, err
	}
	tmpl, err := baseTemplate(cn, ips, alternateDNS)
	if err != nil {
		return certificate{}, err
	}
	return issue(s, tmpl, parent, signerKey, days)
}

// parseCA decodes the CA certificate and key from a certificate produced by
// genCA (or buildCustomCert). The key must be ed25519, which everything in this
// package generates.
func parseCA(ca certificate) (*x509.Certificate, ed25519.PrivateKey, error) {
	certBlock, _ := pem.Decode([]byte(ca.Cert))
	if certBlock == nil {
		return nil, nil, errors.New("unable to decode CA certificate PEM")
	}
	parent, err := x509.ParseCertificate(certBlock.Bytes)
	if err != nil {
		return nil, nil, fmt.Errorf("parsing CA certificate: %w", err)
	}
	keyBlock, _ := pem.Decode([]byte(ca.Key))
	if keyBlock == nil {
		return nil, nil, errors.New("unable to decode CA key PEM")
	}
	key, err := x509.ParsePKCS8PrivateKey(keyBlock.Bytes)
	if err != nil {
		return nil, nil, fmt.Errorf("parsing CA key: %w", err)
	}
	edKey, ok := key.(ed25519.PrivateKey)
	if !ok {
		return nil, nil, fmt.Errorf("CA key is %T, want ed25519", key)
	}
	return parent, edKey, nil
}

func genPrivateKey(s *stream, typ string) string {
	switch typ {
	case "", "rsa", "ecdsa", "ed25519":
		_, priv, err := ed25519.GenerateKey(s)
		if err != nil {
			return fmt.Sprintf("failed to generate private key: %s", err)
		}
		der, err := x509.MarshalPKCS8PrivateKey(priv)
		if err != nil {
			return fmt.Sprintf("failed to marshal private key: %s", err)
		}
		return string(pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: der}))
	default:
		return "Unknown type " + typ
	}
}

// buildCustomCert decodes a base64 cert+key into a certificate. It is already
// deterministic; it is overridden so its result is this package's certificate
// type, letting a template feed it into genSignedCert without a type mismatch.
func buildCustomCert(b64cert, b64key string) (certificate, error) {
	cert, err := base64.StdEncoding.DecodeString(b64cert)
	if err != nil {
		return certificate{}, errors.New("unable to decode base64 certificate")
	}
	key, err := base64.StdEncoding.DecodeString(b64key)
	if err != nil {
		return certificate{}, errors.New("unable to decode base64 private key")
	}
	block, _ := pem.Decode(cert)
	if block == nil {
		return certificate{}, errors.New("unable to decode certificate")
	}
	if _, err := x509.ParseCertificate(block.Bytes); err != nil {
		return certificate{}, fmt.Errorf("parsing certificate: %w", err)
	}
	return certificate{Cert: string(cert), Key: string(key)}, nil
}

func netIPs(ips []any) ([]net.IP, error) {
	out := make([]net.IP, 0, len(ips))
	for _, ip := range ips {
		str, ok := ip.(string)
		if !ok {
			return nil, fmt.Errorf("ip %v is not a string", ip)
		}
		parsed := net.ParseIP(str)
		if parsed == nil {
			return nil, fmt.Errorf("invalid ip %q", str)
		}
		out = append(out, parsed)
	}
	return out, nil
}

func dnsStrs(names []any) ([]string, error) {
	out := make([]string, 0, len(names))
	for _, name := range names {
		str, ok := name.(string)
		if !ok {
			return nil, fmt.Errorf("dns name %v is not a string", name)
		}
		out = append(out, str)
	}
	return out, nil
}
