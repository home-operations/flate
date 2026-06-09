package git

import (
	"fmt"
	"strings"

	"github.com/go-git/go-git/v5/plumbing/transport"
	githttp "github.com/go-git/go-git/v5/plumbing/transport/http"
	gitssh "github.com/go-git/go-git/v5/plumbing/transport/ssh"

	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source"
)

// resolveAuth turns repo.SecretRef into a go-git AuthMethod. Returns
// nil auth (anonymous) when no secret is configured, matching the
// pre-auth behavior. For HTTPS URLs the secret may carry either
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
	if isSSHURL(repo.URL) {
		identity := source.StringFromSecret(sec, "identity")
		if identity == "" {
			// Empty covers both missing-key and PLACEHOLDER-wiped values
			// (the ExternalSecret case). Same sentinel so
			// --allow-missing-secrets covers both shapes.
			return nil, source.MissingSecretErr("GitRepository", repo.Namespace, repo.Name, repo.SecretRef.Name, "missing 'identity' for SSH auth")
		}
		password := source.StringFromSecret(sec, "password")
		user := sshUserFromURL(repo.URL)
		auth, err := gitssh.NewPublicKeys(user, []byte(identity), password)
		if err != nil {
			return nil, fmt.Errorf("%s: parse SSH identity: %w", gitID(repo), err)
		}
		// Flate has no central known_hosts. If the secret carries one,
		// enforce strict host-key checking; otherwise skip (offline
		// renders against ephemeral worktrees are the norm). Users who
		// need strict checks provide `known_hosts` in the Secret.
		if kh := source.StringFromSecret(sec, "known_hosts"); kh != "" {
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
	if token := source.StringFromSecret(sec, "bearerToken"); token != "" {
		return &githttp.TokenAuth{Token: token}, nil
	}
	username := source.StringFromSecret(sec, "username")
	password := source.StringFromSecret(sec, "password")
	if username == "" || password == "" {
		// Empty covers both missing-key and PLACEHOLDER-wiped values
		// (the ExternalSecret case). Same sentinel so
		// --allow-missing-secrets covers both shapes.
		return nil, source.MissingSecretErr("GitRepository", repo.Namespace, repo.Name, repo.SecretRef.Name, "missing username/password (or bearerToken) for HTTPS auth")
	}
	return &githttp.BasicAuth{Username: username, Password: password}, nil
}

// isSSHURL detects a git SSH URL. Covers both `ssh://user@host/repo`
// and the scp-like `user@host:repo` form Flux GitRepository specs
// commonly use; rejects http(s):// URLs that happen to contain `@`
// (e.g. embedded basic-auth credentials).
func isSSHURL(url string) bool {
	return strings.HasPrefix(url, "ssh://") ||
		(strings.Contains(url, "@") && !strings.HasPrefix(url, "http://") && !strings.HasPrefix(url, "https://"))
}

// sshUserFromURL extracts the SSH user from "user@host:repo" or
// "ssh://user@host/repo" forms. Defaults to "git" when absent.
func sshUserFromURL(url string) string {
	u := strings.TrimPrefix(url, "ssh://")
	if at := strings.Index(u, "@"); at > 0 {
		return u[:at]
	}
	return "git"
}
