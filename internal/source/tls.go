package source

import (
	"cmp"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"

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

func buildTLSConfig(crt, key, ca string) (*tls.Config, error) {
	if crt == "" && key == "" && ca == "" {
		return nil, manifest.NewError(manifest.ErrInput, nil, "no TLS material (tls.crt, tls.key, ca.crt) provided")
	}
	if (crt == "" && key != "") ||
		(key == "" && crt != "") {
		return nil, manifest.NewError(manifest.ErrInput, nil, "must provide both tls.crt and tls.key together")
	}
	cfg := &tls.Config{MinVersion: tls.VersionTLS12}
	if ca != "" {
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM([]byte(ca)) {
			return nil, manifest.NewError(manifest.ErrInput, nil, "ca.crt did not parse as PEM")
		}
		cfg.RootCAs = pool
	}
	if crt != "" && key != "" {
		cert, err := tls.X509KeyPair([]byte(crt), []byte(key))
		if err != nil {
			return nil, manifest.NewError(manifest.ErrInput, nil, "parse tls.crt/tls.key: invalid client pair")
		}
		cfg.Certificates = []tls.Certificate{cert}
	}
	return cfg, nil
}

type tlsMaterial struct {
	configuration *tls.Config
	material      [3]string
	identity      any
}
