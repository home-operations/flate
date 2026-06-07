package git

import (
	"cmp"
	"crypto/tls"
	"fmt"

	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source"
)

// resolveTLS builds a *tls.Config from spec.secretRef for HTTPS
// GitRepositories. It reads both a custom CA ("ca.crt" preferred,
// "caFile" legacy alias) AND a client certificate ("tls.crt" +
// "tls.key") so HTTPS-with-mTLS repositories authenticate — matching
// the cert-resolution schema the OCI and Bucket fetchers already use.
// Returns nil when the secret carries no TLS material at all (the
// anonymous / system-CA / SSH-identity / basic-auth path).
//
// Unlike OCI/Bucket this does NOT route through source.ResolveCertSecret:
// a GitRepository's single secretRef carries auth credentials AND
// optional TLS material together, so a secret holding only an SSH
// identity or username/password must resolve to nil here rather than
// erroring "no TLS material". The SSH-URL guard and the no-material
// short-circuit below preserve that.
func (f *Fetcher) resolveTLS(repo *manifest.GitRepository) (*tls.Config, error) {
	if repo.SecretRef == nil {
		return nil, nil
	}
	if isSSHURL(repo.URL) {
		return nil, nil
	}
	if f.Secrets == nil {
		// resolveAuth already errored if SecretRef && !Secrets, but
		// guard anyway so this method is safe to call standalone.
		return nil, nil
	}
	sec := f.Secrets(repo.Namespace, repo.SecretRef.Name)
	if sec == nil {
		// resolveAuth reports the missing-secret error first.
		return nil, nil
	}
	crt := source.StringFromSecret(sec, "tls.crt")
	key := source.StringFromSecret(sec, "tls.key")
	ca := cmp.Or(source.StringFromSecret(sec, "ca.crt"), source.StringFromSecret(sec, "caFile"))
	if crt == "" && key == "" && ca == "" {
		// No TLS material — the secret carries only SSH identity or
		// basic-auth credentials. Anonymous / system-CA path.
		return nil, nil
	}
	cfg, err := source.BuildTLSConfig(crt, key, ca)
	if err != nil {
		return nil, fmt.Errorf("GitRepository %s/%s: secretRef %s/%s: %w",
			repo.Namespace, repo.Name, repo.Namespace, repo.SecretRef.Name, err)
	}
	return cfg, nil
}
