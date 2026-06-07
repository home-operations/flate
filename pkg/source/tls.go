package source

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"

	"github.com/home-operations/flate/pkg/manifest"
)

// ResolveCertSecret reads ref's Secret and builds a *tls.Config from
// its tls.crt + tls.key (client cert) and/or ca.crt (server CA) keys —
// mirroring source-controller's certSecretRef schema.
//
// Returns (nil, nil) when ref is nil. Loud errors when ref is set but
// SecretGetter is unwired or the Secret is missing/unparseable.
// ownerKind+ownerID are formatted into errors for diagnosability.
func ResolveCertSecret(secrets SecretGetter, ns, ownerKind, ownerID string, ref *manifest.LocalObjectReference) (*tls.Config, error) {
	if ref == nil {
		return nil, nil
	}
	sec, err := resolveSecretRef(secrets, ns, ownerKind, ownerID, "certSecretRef", ref)
	if err != nil {
		return nil, err
	}
	cfg, err := BuildTLSConfig(
		StringFromSecret(sec, "tls.crt"),
		StringFromSecret(sec, "tls.key"),
		StringFromSecret(sec, "ca.crt"),
	)
	if err != nil {
		return nil, fmt.Errorf("%s %s: certSecretRef %s/%s: %w",
			ownerKind, ownerID, ns, ref.Name, err)
	}
	return cfg, nil
}

// BuildTLSConfig assembles a *tls.Config from PEM-armored secret
// material. Any subset of (client cert + key) and (CA) is acceptable;
// crt/key must appear together if either is present. Returns an error
// when all inputs are empty so callers can distinguish "no TLS
// configured" from "malformed config".
//
// MinVersion is pinned to TLS 1.2 to match source-controller's
// defaults. Callers that need TLS 1.3-only behavior can adjust the
// returned config.
//
// Common Flux Secret keys feeding this helper:
//
//	tls.crt / tls.key  → client certificate (mTLS)
//	ca.crt             → trust roots for the server
func BuildTLSConfig(crt, key, ca string) (*tls.Config, error) {
	if crt == "" && key == "" && ca == "" {
		return nil, errors.New("no TLS material (tls.crt, tls.key, ca.crt) provided")
	}
	if (crt == "") != (key == "") {
		return nil, errors.New("must provide both tls.crt and tls.key together")
	}
	cfg := &tls.Config{MinVersion: tls.VersionTLS12}
	if crt != "" {
		cert, err := tls.X509KeyPair([]byte(crt), []byte(key))
		if err != nil {
			return nil, fmt.Errorf("parse tls.crt/tls.key: %w", err)
		}
		cfg.Certificates = []tls.Certificate{cert}
	}
	if ca != "" {
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM([]byte(ca)) {
			return nil, errors.New("ca.crt did not parse as PEM")
		}
		cfg.RootCAs = pool
	}
	return cfg, nil
}
