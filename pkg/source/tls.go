package source

import (
	"cmp"
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

func resolveTLS(secrets SecretGetter, owner manifest.NamedResource, input TransportInput) (*tls.Config, [3]string, any, error) {
	ref := input.Reference
	sec, err := resolveSecretRef(secrets, owner, input.Use.label(), ref)
	if err != nil {
		if input.OptionalTLS && errors.Is(err, manifest.ErrMissingSecret) {
			return nil, [3]string{}, nil, nil
		}
		return nil, [3]string{}, nil, err
	}
	var material [3]string
	for i, key := range []string{"tls.crt", "tls.key", "ca.crt"} {
		value, _, err := sec.Value(key)
		if err != nil {
			return nil, [3]string{}, nil, fmt.Errorf("%s: secret %s/%s (%s): %w", QualifiedName(owner.Kind, owner.Namespace, owner.Name), owner.Namespace, ref.Name, input.Use.label(), err)
		}
		material[i] = value
	}
	if input.CAFallback {
		caFile, _, err := sec.Value("caFile")
		if err != nil {
			return nil, [3]string{}, nil, fmt.Errorf("%s: secret %s/%s (%s): %w", QualifiedName(owner.Kind, owner.Namespace, owner.Name), owner.Namespace, ref.Name, input.Use.label(), err)
		}
		material[2] = cmp.Or(material[2], caFile)
	}
	if material == [3]string{} {
		if input.OptionalTLS {
			return nil, material, nil, nil
		}
		return nil, material, nil, MissingSecretErr(owner, ref.Name, "TLS material unavailable")
	}
	cfg, err := buildTLSConfig(material[0], material[1], material[2])
	if errors.Is(err, manifest.ErrMissingSecret) {
		return nil, [3]string{}, nil, MissingSecretErr(owner, ref.Name, "TLS material unavailable")
	}
	if err != nil {
		return nil, [3]string{}, nil, fmt.Errorf("%s: secret %s/%s (%s): %w", QualifiedName(owner.Kind, owner.Namespace, owner.Name), owner.Namespace, ref.Name, input.Use.label(), err)
	}
	return cfg, material, [2]any{sec.Data, sec.StringData}, nil
}

// buildTLSConfig builds TLS from a client pair, a CA bundle, or both.
// Independently malformed real material returns manifest.ErrInput before placeholders
// return manifest.ErrMissingSecret; omitted client components must appear together.
func buildTLSConfig(crt, key, ca string) (*tls.Config, error) {
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
