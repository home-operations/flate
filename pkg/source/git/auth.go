package git

import (
	"cmp"
	"errors"
	"fmt"
	"net/url"

	githttp "github.com/go-git/go-git/v6/plumbing/transport/http"
	gitssh "github.com/go-git/go-git/v6/plumbing/transport/ssh"
	"golang.org/x/crypto/ssh"

	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source"
	"github.com/home-operations/flate/pkg/store"
	"github.com/home-operations/flate/pkg/values"
)

func (f *operation) resolveAuth(
	repo *manifest.GitRepository,
	sec *store.ReferenceFacts,
	endpoint *url.URL,
) (any, error) {
	owner := manifest.DiagnosticFor(repo.Named(), repo)
	if repo.SecretRef == nil {
		if endpoint.Scheme == sshScheme {
			return nil, source.MissingSecretErr(
				owner,
				"",
				"SSH credentials unavailable",
			)
		}
		return nil, nil //nolint:nilnil // Without SecretRef on a non-SSH endpoint, newClient omits credentials for anonymous acquisition.
	}
	if endpoint.Scheme == sshScheme {
		auth, err := f.resolveSSHAuth(repo, sec, endpoint)
		if err != nil {
			return nil, err
		}
		return auth, nil
	}

	valueResult, err := source.SecretValue(sec, &owner, repo.SecretRef.Name, "bearerToken")
	token := valueResult.Value()
	if err != nil {
		return nil, fmt.Errorf("secret %s/%s: %w", repo.Namespace, repo.SecretRef.Name, err)
	}
	if token != "" {
		return &githttp.TokenAuth{Token: token}, nil
	}
	username, password, err := source.BasicAuthFromSecret(
		*sec,
		owner,
		repo.SecretRef.Name,
	)
	if err != nil {
		unavailableToken := valueResult.Effect() != values.Used &&
			(valueResult.KnownKey() || valueResult.Present() && !valueResult.KeysComplete())
		if errors.Is(err, manifest.ErrMissingSecret) && unavailableToken {
			return nil, source.UnavailableSecret(&owner, sec, &valueResult)
		}
		return nil, err
	}
	return &githttp.BasicAuth{Username: username, Password: password}, nil
}

func (f *operation) resolveSSHAuth(
	repo *manifest.GitRepository,
	sec *store.ReferenceFacts,
	endpoint *url.URL,
) (*gitssh.PublicKeys, error) {
	owner := manifest.DiagnosticFor(repo.Named(), repo)
	identity, valueErr := source.SecretValue(sec, &owner, repo.SecretRef.Name, "identity")

	if valueErr != nil {
		return nil, fmt.Errorf("secret %s/%s: %w", repo.Namespace, repo.SecretRef.Name, valueErr)
	}
	password, valueErr := source.SecretValue(sec, &owner, repo.SecretRef.Name, "password")

	if valueErr != nil {
		return nil, fmt.Errorf("secret %s/%s: %w", repo.Namespace, repo.SecretRef.Name, valueErr)
	}
	knownHosts, valueErr := source.SecretValue(sec, &owner, repo.SecretRef.Name, "known_hosts")

	if valueErr != nil {
		return nil, fmt.Errorf("secret %s/%s: %w", repo.Namespace, repo.SecretRef.Name, valueErr)
	}
	var auth *gitssh.PublicKeys
	if identity.Value() != "" {
		auth, valueErr = gitssh.NewPublicKeys(
			cmp.Or(endpoint.User.Username(), "git"),
			[]byte(identity.Value()),
			password.Value(),
		)
		if valueErr != nil {
			if _, missing := errors.AsType[*ssh.PassphraseMissingError](
				valueErr,
			); missing &&
				password.Effect() != values.Used {
				return nil, source.UnavailableSecret(&owner, sec, &password)
			}
			return nil, manifest.NewError(
				manifest.ErrInput,
				nil,
				fmt.Sprintf(
					"secret %s/%s: parse SSH identity: invalid private key",
					repo.Namespace,
					repo.SecretRef.Name,
				),
			)
		}
	}
	var cb ssh.HostKeyCallback
	if knownHosts.Value() != "" {
		cb, valueErr = knownHostsCallback([]byte(knownHosts.Value()))
		if valueErr != nil {
			return nil, manifest.NewError(
				manifest.ErrInput,
				nil,
				fmt.Sprintf(
					"secret %s/%s: parse known_hosts: invalid host data",
					repo.Namespace,
					repo.SecretRef.Name,
				),
			)
		}
	}
	for _, result := range []*values.Result{&identity, &knownHosts} {
		if result.Effect() != values.Used {
			return nil, source.UnavailableSecret(&owner, sec, result)
		}
	}
	if identity.Value() == "" {
		missing := source.UnavailableSecret(&owner, sec, &identity)
		missing.Detail = "missing 'identity' for SSH auth"
		return nil, missing
	}
	if knownHosts.Value() == "" {
		missing := source.UnavailableSecret(&owner, sec, &knownHosts)
		missing.Detail = "missing 'known_hosts' for SSH auth"
		return nil, missing
	}
	auth.HostKeyCallback = cb
	return auth, nil
}
