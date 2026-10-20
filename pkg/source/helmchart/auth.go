package helmchart

import (
	"fmt"
	"net/http"

	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source"
)

// httpRepository fixes auth and transport inputs for a chart and its nested index.
type httpRepository struct {
	*manifest.HelmRepository
	username, password string
	client             *http.Client
	material           any
}

func (f *Fetcher) prepareHTTP(r *manifest.HelmRepository) (*httpRepository, error) {
	secrets := source.SnapshotSecrets(f.secrets)
	prepared := &httpRepository{HelmRepository: r}
	if r.SecretRef != nil {
		if secrets == nil {
			return nil, fmt.Errorf("%w: %s references secretRef but no SecretGetter is wired", manifest.ErrMissingSecret, source.QualifiedName(manifest.KindHelmRepository, r.Namespace, r.Name))
		}
		sec := secrets(r.Namespace, r.SecretRef.Name)
		if sec == nil {
			return nil, source.MissingSecretErr(r.Named(), r.SecretRef.Name, "not found")
		}
		var err error
		prepared.username, prepared.password, err = source.BasicAuthFromSecret(sec, r.Named(), r.SecretRef.Name)
		if err != nil {
			return nil, err
		}
	}
	owner := r.Named()
	owner.Name = source.QualifiedName(manifest.KindHelmRepository, r.Namespace, r.Name)
	resolved, err := source.ResolveTransport(secrets, owner, []source.TransportInput{{Reference: r.CertSecretRef, Use: source.TransportCert}})
	if err != nil {
		return nil, err
	}
	var credentials any
	if prepared.username != "" || prepared.password != "" {
		credentials = [2]string{prepared.username, prepared.password}
	}
	prepared.material = resolved.Identity(credentials)
	tr, err := f.network.HTTPTransport(source.HTTPTransportConfig{Material: resolved, Credentials: credentials, DisableCompression: true})
	if err != nil {
		return nil, err
	}
	prepared.client = source.NewHTTPClient(tr, source.HTTPChart)
	return prepared, nil
}
