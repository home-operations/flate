package git

import (
	"errors"
	"testing"
	"time"

	"golang.org/x/crypto/ssh/knownhosts"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/source"
	sourcecache "github.com/home-operations/flate/internal/source/cache"
	"github.com/home-operations/flate/internal/testutil"
)

func TestFetcher_DiscardedPreparationClosesAgent(t *testing.T) {
	identity, _ := testSSHIdentity(t)
	_, host := testSSHIdentity(t)
	homeDir := t.TempDir()
	t.Setenv("HOME", homeDir)
	testutil.WriteFile(t, homeDir, ".ssh/known_hosts", knownhosts.Line([]string{"fixture"}, host.PublicKey())+"\n")
	closed, opened, _ := acquisitionAgent(t, identity)
	repo := gitRepoWithSecret("source", "ssh://git@fixture/repo", "auth")
	reads := (testInputs{}).readSet(repo)
	defer reads.Close()
	_, err := (&Fetcher{}).Fetch(t.Context(), repo, reads, &sourcecache.Policy{})
	missing, ok := errors.AsType[*source.MissingSecretError](err)
	assert.FatalIf(t, !ok, "credential preparation: %v", err)
	prepared, err := missing.RetryMachine(t.Context(), reads)
	assert.NoError(t, err)
	assert.Equal(t, opened.Load(), 1)
	assert.FatalIf(t, prepared.Close == nil, "prepared acquisition has no cleanup")
	prepared.Close()
	select {
	case <-closed:
	case <-time.After(5 * time.Second):
		t.Fatal("discarded preparation retained the SSH agent connection")
	}
	assert.NoError(t, t.Context().Err())
}
