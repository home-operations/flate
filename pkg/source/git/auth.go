package git

import (
	"cmp"
	"errors"
	"fmt"

	"github.com/go-git/go-git/v5/plumbing/transport"
	githttp "github.com/go-git/go-git/v5/plumbing/transport/http"
	gitssh "github.com/go-git/go-git/v5/plumbing/transport/ssh"
	"golang.org/x/crypto/ssh"

	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source"
)

func (f *operation) resolveAuth(repo *manifest.GitRepository, sec *manifest.Secret, endpoint *transport.Endpoint) (transport.AuthMethod, error) {
	if repo.SecretRef == nil {
		if endpoint.Protocol == "ssh" {
			return nil, source.MissingSecretErr(manifest.DiagnosticFor(repo.Named(), repo), "", "SSH credentials unavailable")
		}
		return nil, nil
	}
	if f.secrets == nil {
		return nil, errors.New("secretRef requires a SecretGetter")
	}
	if sec == nil {
		return nil, source.MissingSecretErr(manifest.DiagnosticFor(repo.Named(), repo), repo.SecretRef.Name, "not found")
	}
	var err error
	if endpoint.Protocol == "ssh" {
		identity, _, err := sec.Value("identity")
		if err != nil {
			return nil, fmt.Errorf("secret %s/%s: %w", repo.Namespace, repo.SecretRef.Name, err)
		}
		password, _, err := sec.Value("password")
		if err != nil {
			return nil, fmt.Errorf("secret %s/%s: %w", repo.Namespace, repo.SecretRef.Name, err)
		}
		kh, _, err := sec.Value("known_hosts")
		if err != nil {
			return nil, fmt.Errorf("secret %s/%s: %w", repo.Namespace, repo.SecretRef.Name, err)
		}
		var auth *gitssh.PublicKeys
		if identity != "" && !manifest.HasValuePlaceholder(identity) {
			user := cmp.Or(endpoint.User, "git")
			if manifest.HasValuePlaceholder(password) {
				signer, parseErr := ssh.ParsePrivateKey([]byte(identity))
				if _, needsPassword := errors.AsType[*ssh.PassphraseMissingError](parseErr); needsPassword {
					parseErr = nil
				}
				auth, err = &gitssh.PublicKeys{User: user, Signer: signer}, parseErr
			} else {
				auth, err = gitssh.NewPublicKeys(user, []byte(identity), password)
			}
			if err != nil {
				return nil, manifest.NewError(manifest.ErrInput, nil, fmt.Sprintf("secret %s/%s: parse SSH identity: invalid private key", repo.Namespace, repo.SecretRef.Name))
			}
		}
		var cb ssh.HostKeyCallback = insecureIgnoreHostKey
		if kh != "" && !manifest.HasValuePlaceholder(kh) {
			cb, err = knownHostsCallback([]byte(kh))
			if err != nil {
				return nil, manifest.NewError(manifest.ErrInput, nil, fmt.Sprintf("secret %s/%s: parse known_hosts: invalid host data", repo.Namespace, repo.SecretRef.Name))
			}
		}
		if identity == "" || manifest.HasValuePlaceholder(identity) {
			return nil, source.MissingSecretErr(manifest.DiagnosticFor(repo.Named(), repo), repo.SecretRef.Name, "missing 'identity' for SSH auth")
		}
		if manifest.HasValuePlaceholder(password) {
			return nil, source.MissingSecretErr(manifest.DiagnosticFor(repo.Named(), repo), repo.SecretRef.Name, "SSH password unavailable")
		}
		if manifest.HasValuePlaceholder(kh) {
			return nil, source.MissingSecretErr(manifest.DiagnosticFor(repo.Named(), repo), repo.SecretRef.Name, "known_hosts unavailable")
		}
		auth.HostKeyCallback = cb
		return auth, nil
	}
	token, _, err := sec.Value("bearerToken")
	if err != nil {
		return nil, fmt.Errorf("secret %s/%s: %w", repo.Namespace, repo.SecretRef.Name, err)
	}
	if manifest.HasValuePlaceholder(token) {
		return nil, source.MissingSecretErr(manifest.DiagnosticFor(repo.Named(), repo), repo.SecretRef.Name, "bearerToken unavailable")
	}
	if token != "" {
		return &githttp.TokenAuth{Token: token}, nil
	}
	username, password, err := source.BasicAuthFromSecret(sec, manifest.DiagnosticFor(repo.Named(), repo), repo.SecretRef.Name)
	if err != nil {
		return nil, err
	}
	return &githttp.BasicAuth{Username: username, Password: password}, nil
}
