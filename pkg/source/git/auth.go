package git

import (
	"cmp"
	"fmt"

	"github.com/go-git/go-git/v5/plumbing/transport"
	githttp "github.com/go-git/go-git/v5/plumbing/transport/http"
	gitssh "github.com/go-git/go-git/v5/plumbing/transport/ssh"

	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source"
)

// resolveAuth returns anonymous auth when no secret is configured.
// For HTTPS URLs the secret may carry either
// (username, password) or (bearerToken); for SSH URLs the secret
// carries `identity` (PEM private key) and an optional `password`.
func (f *Fetcher) resolveAuth(repo *manifest.GitRepository) (transport.AuthMethod, error) {
	if repo.SecretRef == nil {
		return nil, nil
	}
	if f.Secrets == nil {
		return nil, fmt.Errorf("%s references secretRef but no SecretGetter is wired", gitID(repo))
	}
	sec := f.Secrets(repo.Namespace, repo.SecretRef.Name)
	if sec == nil {
		return nil, source.MissingSecretErr("GitRepository", repo.Namespace, repo.Name, repo.SecretRef.Name, "not found")
	}
	endpoint, err := transport.NewEndpoint(repo.URL)
	if err != nil {
		return nil, fmt.Errorf("%w: %s: parse git endpoint: %w", manifest.ErrInput, gitID(repo), err)
	}
	if endpoint.Protocol == "ssh" {
		identity, _, _ := sec.Value("identity")
		if identity == "" || manifest.HasValuePlaceholder(identity) {
			// Empty covers both missing-key and PLACEHOLDER-wiped values
			// (the ExternalSecret case). Same sentinel so
			// --allow-missing-secrets covers both shapes.
			return nil, source.MissingSecretErr("GitRepository", repo.Namespace, repo.Name, repo.SecretRef.Name, "missing 'identity' for SSH auth")
		}
		password, _, _ := sec.Value("password")
		auth, err := gitssh.NewPublicKeys(cmp.Or(endpoint.User, "git"), []byte(identity), password)
		if err != nil {
			return nil, fmt.Errorf("%s: parse SSH identity: %w", gitID(repo), err)
		}
		// Flate has no central known_hosts. If the secret carries one,
		// enforce strict host-key checking; otherwise skip (offline
		// renders against ephemeral worktrees are the norm). Users who
		// need strict checks provide `known_hosts` in the Secret.
		if kh, _, _ := sec.Value("known_hosts"); kh != "" && !manifest.HasValuePlaceholder(kh) {
			cb, herr := knownHostsCallback([]byte(kh))
			if herr != nil {
				return nil, fmt.Errorf("%s: parse known_hosts: %w", gitID(repo), herr)
			}
			auth.HostKeyCallback = cb
		} else {
			auth.HostKeyCallback = insecureIgnoreHostKey
		}
		return auth, nil
	}
	// HTTPS / HTTP: bearerToken takes precedence over basic auth, mirroring
	// source-controller's docs.
	if token, _, _ := sec.Value("bearerToken"); token != "" && !manifest.HasValuePlaceholder(token) {
		return &githttp.TokenAuth{Token: token}, nil
	}
	username, password, err := source.BasicAuthFromSecret(sec, "GitRepository", repo.Namespace, repo.Name, repo.SecretRef.Name)
	if err != nil {
		return nil, err
	}
	return &githttp.BasicAuth{Username: username, Password: password}, nil
}
