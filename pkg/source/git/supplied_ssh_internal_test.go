package git

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source"
	sourcecache "github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/store"
)

//nolint:paralleltest // SSH fixtures change process-global state.
func TestFetcher_SSHAcquisitionsOnBothSides(t *testing.T) {
	for _, mode := range []string{"supplied", "repository", "agent", "absent"} {
		t.Run(mode, func(t *testing.T) { testSSHAcquisitions(t, mode) })
	}
}

func testSSHAcquisitions(t *testing.T, mode string) {
	t.Helper()
	identity, signer := testSSHIdentity(t)
	root := t.TempDir()
	mustInitRepo(t, root)
	url, authenticated := localGitSSH(t, root, signer.PublicKey())
	knownHosts, err := os.ReadFile(filepath.Join(os.Getenv("HOME"), ".ssh", "known_hosts"))
	assert.NoError(t, err)
	t.Setenv("SSH_AUTH_SOCK", filepath.Join(t.TempDir(), "absent"))
	if mode == "agent" {
		acquisitionAgent(t, identity)
	}
	for _, side := range []string{"orig", "current"} {
		repo := gitRepoWithSecret(side, url, "credentials")
		secret := &manifest.Secret{
			Name: "credentials", Namespace: repo.Namespace,
			StringData: map[string]any{"identity": identity, "known_hosts": string(knownHosts)},
		}
		testSSHAcquisition(t, repo, secret, mode)
	}
	if mode == "absent" {
		assert.Equal(t, authenticated.Load(), int32(0))
	} else {
		assert.FatalIf(
			t,
			authenticated.Load() < 2,
			"authenticated acquisitions=%d, want at least two",
			authenticated.Load(),
		)
	}
}

func testSSHAcquisition(t *testing.T, repo *manifest.GitRepository, secret *manifest.Secret, mode string) {
	t.Helper()
	st := store.New()
	st.AddObject(repo)
	if mode == "supplied" || mode == "repository" {
		origin := manifest.SuppliedDeclaration
		if mode == "repository" {
			origin = manifest.RepositoryDeclaration
		}
		st.AddDeclaration(store.Declaration{Obj: secret, Origin: origin})
	}
	var reads store.ReadSet
	reads.Init(st, repo.Named(), nil)
	defer reads.Close()
	fetcher := &Fetcher{Cache: cachetest.New(t, "", sourcecache.New)}
	artifact, err := fetcher.Fetch(t.Context(), repo, &reads, &sourcecache.Policy{})
	if mode == "agent" || mode == "absent" {
		missing, ok := errors.AsType[*source.MissingSecretError](err)
		assert.FatalIf(t, !ok || missing.RetryAmbient == nil, "missing ambient continuation: %v", err)
		artifact, err = missing.RetryAmbient(t.Context())
	}
	if mode == "absent" {
		assert.FatalIf(t, artifact != nil || !errors.Is(err, manifest.ErrMissingSecret),
			"absent agent: artifact=%v error=%v", artifact, err)
		return
	}
	assert.NoError(t, err)
	assert.FatalIf(t, artifact == nil, "no source artifact on %s side", repo.Name)
}
