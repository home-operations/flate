package source

import (
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/mldsa"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"fmt"
	"strings"

	"github.com/home-operations/flate/pkg/manifest"
)

// ResolveCertSecret builds TLS from the configured Secret's client pair or CA.
// Missing or wiped material returns MissingSecretError; malformed real material
// remains a failure. A nil ref leaves TLS unconfigured.
func ResolveCertSecret(secrets SecretGetter, owner manifest.NamedResource, ref *manifest.LocalObjectReference) (*tls.Config, error) {
	if ref == nil {
		return nil, nil
	}
	sec, err := resolveSecretRef(secrets, owner, "certSecretRef", ref)
	if err != nil {
		return nil, err
	}

	var material [3]string
	for i, key := range []string{"tls.crt", "tls.key", "ca.crt"} {
		value, _, err := sec.Value(key)
		if err != nil {
			return nil, fmt.Errorf("%s %s: certSecretRef %s/%s: %w", owner.Kind, owner.Name, owner.Namespace, ref.Name, err)
		}
		material[i] = value
	}
	if material[0] == "" && material[1] == "" && material[2] == "" {
		return nil, MissingSecretErr(owner, ref.Name, "TLS material unavailable")
	}
	cfg, err := BuildTLSConfig(material[0], material[1], material[2])
	if errors.Is(err, manifest.ErrMissingSecret) {
		return nil, MissingSecretErr(owner, ref.Name, "TLS material unavailable")
	}
	if err != nil {
		return nil, fmt.Errorf("%s %s: certSecretRef %s/%s: %w",
			owner.Kind, owner.Name, owner.Namespace, ref.Name, err)
	}
	return cfg, nil
}

// BuildTLSConfig builds TLS from a client pair, a CA bundle, or both.
// Independently malformed real material returns manifest.ErrInput before placeholders
// return manifest.ErrMissingSecret; omitted client components must appear together.
func BuildTLSConfig(crt, key, ca string) (*tls.Config, error) {
	if crt == "" && key == "" && ca == "" {
		return nil, fmt.Errorf("%w: no TLS material (tls.crt, tls.key, ca.crt) provided", manifest.ErrInput)
	}
	if (crt == "" && key != "" && !manifest.HasValuePlaceholder(key)) ||
		(key == "" && crt != "" && !manifest.HasValuePlaceholder(crt)) {
		return nil, fmt.Errorf("%w: must provide both tls.crt and tls.key together", manifest.ErrInput)
	}
	cfg := &tls.Config{MinVersion: tls.VersionTLS12}
	if ca != "" && !manifest.HasValuePlaceholder(ca) {
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM([]byte(ca)) {
			return nil, fmt.Errorf("%w: ca.crt did not parse as PEM", manifest.ErrInput)
		}
		cfg.RootCAs = pool
	}
	if crt != "" && key != "" && !manifest.HasValuePlaceholder(crt) && !manifest.HasValuePlaceholder(key) {
		cert, err := tls.X509KeyPair([]byte(crt), []byte(key))
		if err != nil {
			return nil, fmt.Errorf("%w: parse tls.crt/tls.key: invalid client pair", manifest.ErrInput)
		}
		cfg.Certificates = []tls.Certificate{cert}
	} else {
		for i, material := range []string{crt, key} {
			if material == "" || manifest.HasValuePlaceholder(material) {
				continue
			}
			var block *pem.Block
			for remaining := []byte(material); len(remaining) > 0; {
				block, remaining = pem.Decode(remaining)
				if block == nil {
					break
				}
				if (i == 0 && block.Type == "CERTIFICATE") ||
					(i == 1 && (block.Type == "PRIVATE KEY" || strings.HasSuffix(block.Type, " PRIVATE KEY"))) {
					break
				}
				block = nil
			}
			if block == nil {
				return nil, fmt.Errorf("%w: parse tls.crt/tls.key: no matching PEM block", manifest.ErrInput)
			}
			if i == 0 {
				cert, err := x509.ParseCertificate(block.Bytes)
				if err != nil {
					return nil, fmt.Errorf("%w: parse tls.crt: invalid certificate", manifest.ErrInput)
				}
				switch cert.PublicKey.(type) {
				case *rsa.PublicKey, *ecdsa.PublicKey, ed25519.PublicKey, *mldsa.PublicKey:
				default:
					return nil, fmt.Errorf("%w: tls.crt: unsupported public key algorithm", manifest.ErrInput)
				}
			} else {
				// crypto/tls.parsePrivateKey is unexported; keep its DER forms and key types.
				privateKey, err := x509.ParsePKCS8PrivateKey(block.Bytes)
				if err != nil {
					privateKey, err = x509.ParsePKCS1PrivateKey(block.Bytes)
				}
				if err != nil {
					privateKey, err = x509.ParseECPrivateKey(block.Bytes)
				}
				if err != nil {
					return nil, fmt.Errorf("%w: parse tls.key: invalid private key", manifest.ErrInput)
				}
				switch privateKey.(type) {
				case *rsa.PrivateKey, *ecdsa.PrivateKey, ed25519.PrivateKey, *mldsa.PrivateKey:
				default:
					return nil, fmt.Errorf("%w: tls.key: unsupported private key type", manifest.ErrInput)
				}
			}
		}
	}
	if manifest.HasValuePlaceholder(crt) || manifest.HasValuePlaceholder(key) || manifest.HasValuePlaceholder(ca) {
		return nil, manifest.ErrMissingSecret
	}
	return cfg, nil
}
