package source

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"

	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
	"github.com/home-operations/flate/pkg/values"
)

func resolveTLS(
	secrets SecretGetter, ownerValue *manifest.DiagnosticIdentity,
	input TransportInput,
) (tlsMaterial, error) {
	owner := *ownerValue
	ref := input.Reference
	sec, err := resolveSecretRef(secrets, new(owner), input.Use.label(), ref)
	if err != nil {
		return tlsMaterial{}, err
	}
	material, results, err := tlsFields(&sec, &owner, input)
	if err != nil {
		return tlsMaterial{}, err
	}
	resolved := tlsMaterial{material: material}
	unavailableErr := resolved.unavailable(&owner, input, &results)
	if unavailableErr != nil {
		return tlsMaterial{}, unavailableErr
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
	identity := sec.Data.Snapshot()
	delete(identity, "metadata")
	return tlsMaterial{configuration: cfg, material: material, identity: identity}, nil
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

func tlsFields(
	sec *store.ReferenceFacts,
	ownerValue *manifest.DiagnosticIdentity,
	input TransportInput,
) ([3]string, [3]values.Result, error) {
	owner := *ownerValue
	ref := input.Reference
	var material [3]string
	var results [3]values.Result
	for i, key := range []string{"tls.crt", "tls.key", "ca.crt"} {
		valueResult, valueErr := SecretValue(sec, new(owner), ref.Name, key)
		value := valueResult.Value()
		if valueErr != nil {
			return [3]string{}, [3]values.Result{}, fmt.Errorf(
				"secret %s/%s (%s): %w",
				owner.ID.Namespace,
				ref.Name,
				input.Use.label(),
				valueErr,
			)
		}
		material[i], results[i] = value, valueResult
	}
	if input.CAFallback {
		caFile, valueErr2 := SecretValue(sec, new(owner), ref.Name, "caFile")

		if valueErr2 != nil {
			return [3]string{}, [3]values.Result{}, fmt.Errorf(
				"secret %s/%s (%s): %w",
				owner.ID.Namespace,
				ref.Name,
				input.Use.label(),
				valueErr2,
			)
		}
		if material[2] == "" && (caFile.State() != values.MissingKey || results[2].State() == values.MissingKey) {
			results[2] = caFile
		}
		if material[2] == "" {
			material[2] = caFile.Value()
		}
	}
	return material, results, nil
}

func (m *tlsMaterial) unavailable(
	owner *manifest.DiagnosticIdentity,
	input TransportInput,
	results *[3]values.Result,
) error {
	for i := range results {
		result := &results[i]
		pairRequired := i == 0 && m.material[1] != "" || i == 1 && m.material[0] != ""
		if result.State() == values.Produced && !pairRequired && (input.OptionalTLS || m.material != [3]string{}) {
			continue
		}
		if result.State() == values.Readable || result.State() == values.MissingKey {
			continue
		}
		if input.OptionalTLS && result.State() == values.Absent {
			return nil
		}
		return UnavailableSecret(owner, input.Reference.Name, result)
	}
	return nil
}
