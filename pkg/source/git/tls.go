package git

import (
	"cmp"
	"errors"
	"fmt"

	"github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing/transport"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source"
)

// Git secretRef may contain only auth credentials, so TLS material is optional.
// Validate PEM before go-git appends the CA bundle to the system trust roots.
func (f *Fetcher) resolveTLS(repo *manifest.GitRepository, sec *manifest.Secret) (git.CloneOptions, error) {
	var opts git.CloneOptions
	if repo.SecretRef == nil {
		return opts, nil
	}
	endpoint, err := transport.NewEndpoint(repo.URL)
	if err != nil {
		return opts, fmt.Errorf("%w: %s: parse git endpoint: %w", manifest.ErrInput, gitID(repo), err)
	}
	if endpoint.Protocol == "ssh" {
		return opts, nil
	}
	if f.Secrets == nil || sec == nil {
		return opts, nil
	}
	crt, _, err := sec.Value("tls.crt")
	if err != nil {
		return opts, fmt.Errorf("%s: secret %s/%s: %w", gitID(repo), repo.Namespace, repo.SecretRef.Name, err)
	}
	key, _, err := sec.Value("tls.key")
	if err != nil {
		return opts, fmt.Errorf("%s: secret %s/%s: %w", gitID(repo), repo.Namespace, repo.SecretRef.Name, err)
	}
	ca, _, err := sec.Value("ca.crt")
	if err != nil {
		return opts, fmt.Errorf("%s: secret %s/%s: %w", gitID(repo), repo.Namespace, repo.SecretRef.Name, err)
	}
	caFile, _, err := sec.Value("caFile")
	if err != nil {
		return opts, fmt.Errorf("%s: secret %s/%s: %w", gitID(repo), repo.Namespace, repo.SecretRef.Name, err)
	}
	ca = cmp.Or(ca, caFile)
	if crt == "" && key == "" && ca == "" {
		return opts, nil
	}
	_, err = source.BuildTLSConfig(crt, key, ca)
	if errors.Is(err, manifest.ErrMissingSecret) {
		return opts, source.MissingSecretErr(repo.Named(), repo.SecretRef.Name, "TLS material unavailable")
	}
	if err != nil {
		return opts, fmt.Errorf("%s: secretRef %s/%s: %w",
			gitID(repo), repo.Namespace, repo.SecretRef.Name, err)
	}
	opts.CABundle, opts.ClientCert, opts.ClientKey = []byte(ca), []byte(crt), []byte(key)
	return opts, nil
}
