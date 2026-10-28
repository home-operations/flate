package helmchart

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"path/filepath"

	"helm.sh/helm/v4/pkg/helmpath"
	repo "helm.sh/helm/v4/pkg/repo/v1"

	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source"
)

type httpRepository struct {
	*manifest.HelmRepository
	username, password string
	client             *http.Client
	material           any
	ambient            bool
}

func (f *operation) prepareHTTP(r *manifest.HelmRepository) (*httpRepository, error) {
	secrets := f.secrets
	prepared := &httpRepository{HelmRepository: r}
	var authErr error
	if r.SecretRef != nil {
		if secrets == nil {
			return nil, fmt.Errorf("%w: %s references secretRef but no SecretGetter is wired", manifest.ErrMissingSecret, manifest.DiagnosticFor(r.Named(), r).Label())
		}
		sec := secrets(r.Namespace, r.SecretRef.Name)
		if sec == nil {
			authErr = source.MissingSecretErr(manifest.DiagnosticFor(r.Named(), r), r.SecretRef.Name, "not found")
		} else {
			prepared.username, prepared.password, authErr = source.BasicAuthFromSecret(sec, manifest.DiagnosticFor(r.Named(), r), r.SecretRef.Name)
		}
		if authErr != nil && !errors.Is(authErr, manifest.ErrMissingSecret) {
			return nil, authErr
		}
	}
	owner := manifest.DiagnosticFor(r.Named(), r)
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
	prepared.client = &http.Client{Transport: tr}
	prepared.ambient = authErr != nil
	return prepared, authErr
}

func (r *httpRepository) localCredentials(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	config, err := repo.LoadFile(cmp.Or(os.Getenv("HELM_REPOSITORY_CONFIG"), helmpath.ConfigPath("repositories.yaml")))
	if errors.Is(err, os.ErrNotExist) {
		return source.MissingSecretErr(manifest.DiagnosticFor(r.Named(), r.HelmRepository), "", "no local Helm repository login")
	}
	if err != nil {
		return fmt.Errorf("%w: load Helm repository login: %w", manifest.ErrInput, err)
	}
	for _, entry := range config.Repositories {
		if entry == nil || entry.Username == "" || entry.Password == "" || manifest.HasValuePlaceholder(entry.Username) || manifest.HasValuePlaceholder(entry.Password) {
			continue
		}
		if repositoryURLEqual(r.URL, entry.URL) {
			r.username, r.password = entry.Username, entry.Password
			return nil
		}
	}
	return source.MissingSecretErr(manifest.DiagnosticFor(r.Named(), r.HelmRepository), "", "no local Helm repository login")
}

// repositoryURLEqual follows Helm v4.3.1 internal/urlutil.Equal, target first.
func repositoryURLEqual(target, entry string) bool {
	a, err := url.Parse(target)
	if err != nil {
		return filepath.Clean(target) == filepath.Clean(entry)
	}
	b, err := url.Parse(entry)
	if err != nil {
		return false
	}
	for _, u := range []*url.URL{a, b} {
		if u.Path == "" {
			u.Path = "/"
		}
		u.Path = filepath.Clean(u.Path)
	}
	return a.String() == b.String()
}
