package git

import (
	"errors"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"

	"github.com/home-operations/flate/pkg/store"

	"github.com/home-operations/flate/internal/testutil/fetchtest"
	"github.com/home-operations/flate/internal/testutil/networktest"

	"golang.org/x/crypto/ssh"
	"golang.org/x/crypto/ssh/knownhosts"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source"
	sourcecache "github.com/home-operations/flate/pkg/source/cache"
)

func localGitSSH(t *testing.T, root string, allowed ssh.PublicKey) (string, *atomic.Int32) {
	t.Helper()
	_, host := testSSHIdentity(t)
	fixture := &testutil.GitSSH{Host: host, Allowed: allowed, Roots: []string{root}}
	address := testutil.LocalGitSSH(t, fixture)
	homeDir := t.TempDir()
	t.Setenv("HOME", homeDir)
	testutil.WriteFile(t, homeDir, ".ssh/known_hosts", knownhosts.Line([]string{address}, host.PublicKey())+"\n")
	return "ssh://git@" + address + root, &fixture.Authenticated
}

//nolint:paralleltest // Credential fixture helpers change the process environment.
//nolint:paralleltest // Credential and DNS fixtures change process environment or resolver state.
func TestFetcher_LocalAgentSSH(t *testing.T) {
	for _, shape := range []string{"accepted", "rejected", "absent", "explicit rejected"} {
		t.Run(
			shape,
			func(t *testing.T) { testFetcherLocalAgentSSHCase(t, &shape) },
		)
	}
}

func testFetcherLocalAgentSSHCase(t *testing.T, shape *string,
) {
	t.Helper()
	identity, key := testSSHIdentity(t)
	allowed := key.PublicKey()
	if (*shape) == "rejected" || (*shape) == "explicit rejected" {
		_, other := testSSHIdentity(t)
		allowed = other.PublicKey()
	}
	root := t.TempDir()
	mustInitRepo(t, root)
	url, authenticated := localGitSSH(t, root, allowed)
	var closed <-chan struct{}
	var opened *atomic.Int32
	if (*shape) == "absent" {
		t.Setenv("SSH_AUTH_SOCK", filepath.Join(t.TempDir(), "absent"))
	} else {
		closed, opened, _ = acquisitionAgent(t, identity)
	}
	repo := gitRepoWithSecret("agent", url, "auth")
	inputs := testInputs{}
	if (*shape) == "explicit rejected" {
		knownHosts, readErr := os.ReadFile(filepath.Join(os.Getenv("HOME"), ".ssh", "known_hosts"))
		assert.NoError(t, readErr)
		inputs.secrets = func(_, _ string) store.ReferenceFacts {
			secretInput := &manifest.Secret{StringData: map[string]any{
				"identity": identity, "known_hosts": string(knownHosts),
			}}

			return store.ReferenceFacts{Data: secretInput, Original: secretInput}
		}
	}
	reads := inputs.readSet(repo)
	defer reads.Close()
	f := &Fetcher{Network: networktest.New(t), Cache: cachetest.New(t, "", sourcecache.New)}
	artifact, err := fetchtest.Fetch(t.Context(), f.Fetch, repo, reads, &sourcecache.Policy{}, nil)
	if (*shape) == "explicit rejected" {
		if err == nil || errors.Is(err, manifest.ErrMissingSecret) != ((*shape) == "absent") ||
			artifact != nil ||
			opened.Load() != 0 {
			t.Fatalf(
				"explicit rejection used ambient: artifact=%v error=%v opened=%d",
				artifact,
				err,
				opened.Load(),
			)
		}
		return
	}
	missing, ok := errors.AsType[*source.MissingSecretError](err)
	assert.FatalIf(t, !ok || missing.RetryAmbient == nil, "prepare = %v", err)
	artifact, err = fetchtest.Retry(t.Context(), missing.RetryAmbient)
	if closed != nil {
		<-closed
	}
	if (*shape) == "accepted" {
		assert.NoError(t, err)
		if artifact == nil || authenticated.Load() == 0 {
			t.Fatalf("native acquisition = %+v; authenticated=%d", artifact, authenticated.Load())
		}
	} else if artifact != nil || err == nil || errors.Is(err, manifest.ErrMissingSecret) != ((*shape) == "absent") || !errors.Is(err, manifest.ErrFlux) || authenticated.Load() != 0 {
		t.Fatalf("failed native acquisition = %+v, %v; authenticated=%d", artifact, err, authenticated.Load())
	}
}
