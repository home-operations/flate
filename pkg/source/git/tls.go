package git

import (
	"cmp"
	"fmt"

	"github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing/transport"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source"
)

// Git secretRef may contain only auth credentials, so TLS material is optional.
// Validate PEM before go-git appends the CA bundle to the system trust roots.
func (f *Fetcher) resolveTLS(repo *manifest.GitRepository) (git.CloneOptions, error) {
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
	if f.Secrets == nil {
		// resolveAuth already errored if SecretRef && !Secrets, but
		// guard anyway so this method is safe to call standalone.
		return opts, nil
	}
	sec := f.Secrets(repo.Namespace, repo.SecretRef.Name)
	if sec == nil {
		// resolveAuth reports the missing-secret error first.
		return opts, nil
	}
	crt, _, _ := sec.Value("tls.crt")
	key, _, _ := sec.Value("tls.key")
	ca, _, _ := sec.Value("ca.crt")
	caFile, _, _ := sec.Value("caFile")
	ca = cmp.Or(ca, caFile)
	if manifest.HasValuePlaceholder(crt) || manifest.HasValuePlaceholder(key) || manifest.HasValuePlaceholder(ca) {
		return opts, source.MissingSecretErr("GitRepository", repo.Namespace, repo.Name, repo.SecretRef.Name, "TLS material unavailable")
	}
	if crt == "" && key == "" && ca == "" {
		return opts, nil
	}
	_, err = source.BuildTLSConfig(crt, key, ca)
	if err != nil {
		return opts, fmt.Errorf("%s: secretRef %s/%s: %w",
			gitID(repo), repo.Namespace, repo.SecretRef.Name, err)
	}
	opts.CABundle, opts.ClientCert, opts.ClientKey = []byte(ca), []byte(crt), []byte(key)
	return opts, nil
}
