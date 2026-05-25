package git

import (
	"crypto/tls"
	"fmt"
	"sync"

	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source"
)

// httpsTransportMu serializes the brief window where a per-CR HTTPS
// client is installed as go-git's process-global transport. go-git v5
// has no per-CloneOptions TLS hook, so custom-CA fetches must hold
// this lock across InstallProtocol → clone → restore. Package-global
// because `client.InstallProtocol` is itself process-global — a
// per-Fetcher mutex would race when two Fetchers run concurrently
// and clobber each other's transport.
var httpsTransportMu sync.Mutex

// resolveTLS builds a *tls.Config from spec.secretRef for HTTPS
// GitRepositories using a custom CA. Returns nil when no CA material
// is present (anonymous / system-CA path). Matches Flux
// source-controller key conventions: "ca.crt" (preferred) or
// "caFile" (legacy alias).
//
// SSH repositories ignore this — TLS doesn't apply to that transport.
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
	ca := source.StringFromSecret(sec, "ca.crt")
	if ca == "" {
		ca = source.StringFromSecret(sec, "caFile")
	}
	if ca == "" {
		return nil, nil
	}
	cfg, err := source.BuildTLSConfig("", "", ca)
	if err != nil {
		return nil, fmt.Errorf("GitRepository %s/%s: secretRef %s/%s: %w",
			repo.Namespace, repo.Name, repo.Namespace, repo.SecretRef.Name, err)
	}
	return cfg, nil
}
