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
		return nil, nil //nolint:nilnil // A nil value and nil error represent an optional absence or a reusable-cache miss.
	}
	if f.secrets == nil {
		return nil, errors.New("secretRef requires a SecretGetter")
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
		if tokenResult := valueResult; errors.Is(err, manifest.ErrMissingSecret) &&
			(tokenResult.State() == values.Encrypted || tokenResult.State() == values.Produced) {
			return nil, source.UnavailableSecret(&owner, repo.SecretRef.Name, &tokenResult)
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
				password.State() != values.Readable {
				return nil, source.UnavailableSecret(&owner, repo.SecretRef.Name, &password)
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
		if result.State() != values.Readable {
			return nil, source.UnavailableSecret(&owner, repo.SecretRef.Name, result)
		}
	}
	if identity.Value() == "" {
		return nil, source.MissingSecretErr(
			owner,
			repo.SecretRef.Name,
			"missing 'identity' for SSH auth",
		)
	}
	if knownHosts.Value() == "" {
		return nil, source.MissingSecretErr(
			owner,
			repo.SecretRef.Name,
			"missing 'known_hosts' for SSH auth",
		)
	}
	auth.HostKeyCallback = cb
	return auth, nil
}
