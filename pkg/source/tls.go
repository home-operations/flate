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

func resolveTLS(
	secrets SecretGetter, ownerValue *manifest.DiagnosticIdentity,
	input TransportInput,
) (tlsMaterial, error) {
	owner := *ownerValue
	ref := input.Reference
	sec, err := resolveSecretRef(secrets, new(owner), input.Use.label(), ref)
	if err != nil {
		if input.OptionalTLS && errors.Is(err, manifest.ErrMissingSecret) {
			return tlsMaterial{}, nil
		}
		return tlsMaterial{}, err
	}
	var material [3]string
	for i, key := range []string{"tls.crt", "tls.key", "ca.crt"} {
		valueResult, valueErr := sec.Value(key)
		value := valueResult.Value
		if valueErr != nil {
			return tlsMaterial{}, fmt.Errorf(
				"secret %s/%s (%s): %w",
				owner.ID.Namespace,
				ref.Name,
				input.Use.label(),
				valueErr,
			)
		}
		material[i] = value
	}
	if input.CAFallback {
		caFile, valueErr2 := sec.Value("caFile")

		if valueErr2 != nil {
			return tlsMaterial{}, fmt.Errorf(
				"secret %s/%s (%s): %w",
				owner.ID.Namespace,
				ref.Name,
				input.Use.label(),
				valueErr2,
			)
		}
		material[2] = cmp.Or(material[2], caFile.Value)
	}
	if material == [3]string{} {
		if input.OptionalTLS {
			return tlsMaterial{material: material}, nil
		}
		return tlsMaterial{
			configuration: nil,
			material:      material,
			identity:      nil,
		}, MissingSecretErr(
			owner,
			ref.Name,
			"TLS material unavailable",
		)
	}
	cfg, err := buildTLSConfig(material[0], material[1], material[2])
	if errors.Is(err, manifest.ErrMissingSecret) {
		return tlsMaterial{
			configuration: nil,
			material:      [3]string{},
			identity:      nil,
		}, MissingSecretErr(
			owner,
			ref.Name,
			"TLS material unavailable",
		)
	}
	if err != nil {
		return tlsMaterial{}, fmt.Errorf(
			"secret %s/%s (%s): %w",
			owner.ID.Namespace,
			ref.Name,
			input.Use.label(),
			err,
		)
	}
	return tlsMaterial{configuration: cfg, material: material, identity: [2]any{sec.Data, sec.StringData}}, nil
}

// buildTLSConfig builds TLS from a client pair, a CA bundle, or both.
// Independently malformed real material returns manifest.ErrInput before placeholders
// return manifest.ErrMissingSecret; omitted client components must appear together.
func buildTLSConfig(crt, key, ca string) (*tls.Config, error) {
	if crt == "" && key == "" && ca == "" {
		return nil, manifest.NewError(manifest.ErrInput, nil, "no TLS material (tls.crt, tls.key, ca.crt) provided")
	}
	if (crt == "" && key != "" && !manifest.HasValuePlaceholder(key)) ||
		(key == "" && crt != "" && !manifest.HasValuePlaceholder(crt)) {
		return nil, manifest.NewError(manifest.ErrInput, nil, "must provide both tls.crt and tls.key together")
	}
	cfg := &tls.Config{MinVersion: tls.VersionTLS12}
	if ca != "" && !manifest.HasValuePlaceholder(ca) {
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM([]byte(ca)) {
			return nil, manifest.NewError(manifest.ErrInput, nil, "ca.crt did not parse as PEM")
		}
		cfg.RootCAs = pool
	}
	if err := configureTLSClient(cfg, crt, key); err != nil {
		return nil, err
	}
	if manifest.HasValuePlaceholder(crt) || manifest.HasValuePlaceholder(key) || manifest.HasValuePlaceholder(ca) {
		return nil, manifest.ErrMissingSecret
	}
	return cfg, nil
}

func matchingPEM(material string, certificate bool) *pem.Block {
	for remaining := []byte(material); len(remaining) > 0; {
		block, rest := pem.Decode(remaining)
		if block == nil {
			return nil
		}
		if certificate && block.Type == "CERTIFICATE" ||
			!certificate && (block.Type == "PRIVATE KEY" || strings.HasSuffix(block.Type, " PRIVATE KEY")) {
			return block
		}
		remaining = rest
	}
	return nil
}

func validateTLSComponent(material string, certificate bool) error {
	block := matchingPEM(material, certificate)
	if block == nil {
		return manifest.NewError(manifest.ErrInput, nil, "parse tls.crt/tls.key: no matching PEM block")
	}
	if !certificate {
		return validatePrivateKey(block.Bytes)
	}
	cert, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		return manifest.NewError(manifest.ErrInput, nil, "parse tls.crt: invalid certificate")
	}
	switch cert.PublicKey.(type) {
	case *rsa.PublicKey, *ecdsa.PublicKey, ed25519.PublicKey, *mldsa.PublicKey:
		return nil
	default:
		return manifest.NewError(manifest.ErrInput, nil, "tls.crt: unsupported public key algorithm")
	}
}

func validatePrivateKey(data []byte) error {
	// crypto/tls.parsePrivateKey is unexported; preserve its DER forms and key types.
	key, err := x509.ParsePKCS8PrivateKey(data)
	if err != nil {
		key, err = x509.ParsePKCS1PrivateKey(data)
	}
	if err != nil {
		key, err = x509.ParseECPrivateKey(data)
	}
	if err != nil {
		return manifest.NewError(manifest.ErrInput, nil, "parse tls.key: invalid private key")
	}
	switch key.(type) {
	case *rsa.PrivateKey, *ecdsa.PrivateKey, ed25519.PrivateKey, *mldsa.PrivateKey:
		return nil
	default:
		return manifest.NewError(manifest.ErrInput, nil, "tls.key: unsupported private key type")
	}
}

func configureTLSClient(cfg *tls.Config, crt, key string) error {
	if crt != "" && key != "" && !manifest.HasValuePlaceholder(crt) && !manifest.HasValuePlaceholder(key) {
		cert, err := tls.X509KeyPair([]byte(crt), []byte(key))
		if err != nil {
			return manifest.NewError(manifest.ErrInput, nil, "parse tls.crt/tls.key: invalid client pair")
		}
		cfg.Certificates = []tls.Certificate{cert}
	} else {
		for i, material := range []string{crt, key} {
			if material == "" || manifest.HasValuePlaceholder(material) {
				continue
			}
			if err := validateTLSComponent(material, i == 0); err != nil {
				return err
			}
		}
	}
	return nil
}

type tlsMaterial struct {
	configuration *tls.Config
	material      [3]string
	identity      any
}
