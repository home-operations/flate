package git

import (
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	gitlib "github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/filemode"
	"github.com/go-git/go-git/v5/plumbing/format/index"
	"github.com/go-git/go-git/v5/plumbing/object"
	"github.com/go-git/go-git/v5/plumbing/transport/client"
	"github.com/go-git/go-git/v5/plumbing/transport/server"
	"golang.org/x/crypto/ssh/knownhosts"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/pkg/source"
	sourcecache "github.com/home-operations/flate/pkg/source/cache"
)

func TestFetcher_AmbientPostAuthenticationFailure(t *testing.T) {
	for _, phase := range []string{"view", "publication"} {
		t.Run(phase, func(t *testing.T) {
			identity, key := testSSHIdentity(t)
			_, host := testSSHIdentity(t)
			closed, opened := acquisitionAgent(t, identity)
			homeDir := t.TempDir()
			t.Setenv("HOME", homeDir)
			testutil.WriteFile(t, homeDir, ".ssh/known_hosts", knownhosts.Line([]string{"fixture"}, host.PublicKey())+"\n")
			root := t.TempDir()
			mustInitRepo(t, root)
			if phase == "view" {
				repository, err := gitlib.PlainOpen(root)
				assert.NoError(t, err)
				head, err := repository.Head()
				assert.NoError(t, err)
				commit, err := repository.CommitObject(head.Hash())
				assert.NoError(t, err)
				tree, err := commit.Tree()
				assert.NoError(t, err)
				tree.Entries[0].Name = ".."
				encoded := repository.Storer.NewEncodedObject()
				assert.NoError(t, tree.Encode(encoded))
				commit.TreeHash, err = repository.Storer.SetEncodedObject(encoded)
				assert.NoError(t, err)
				encoded = repository.Storer.NewEncodedObject()
				assert.NoError(t, commit.Encode(encoded))
				hash, err := repository.Storer.SetEncodedObject(encoded)
				assert.NoError(t, err)
				assert.NoError(t, repository.Storer.SetReference(plumbing.NewHashReference(head.Name(), hash)))
			}
			tr := &agentAuthentication{Transport: server.DefaultServer, host: host.PublicKey(), allowed: key.PublicKey()}
			previous := client.Protocols["ssh"]
			client.InstallProtocol("ssh", tr)
			t.Cleanup(func() { client.InstallProtocol("ssh", previous) })
			cacheDir := t.TempDir()
			f := &Fetcher{Cache: cachetest.New(t, cacheDir, sourcecache.New)}
			if phase == "publication" {
				blobDir := filepath.Join(cacheDir, "oci", "blobs", "sha256")
				assert.NoError(t, os.MkdirAll(blobDir, 0o750))
				tr.afterAuth = func() error {
					if err := os.Rename(blobDir, blobDir+".held"); err != nil {
						return err
					}
					return os.WriteFile(blobDir, []byte("publication blocked"), 0o600)
				}
				defer func() { _ = os.Remove(blobDir); _ = os.Rename(blobDir+".held", blobDir) }()
			}
			repo := gitRepoWithSecret("failure", "ssh://git@fixture"+root, "auth")
			reads := (testInputs{}).readSet(repo)
			defer reads.Close()
			_, err := f.Fetch(t.Context(), repo, reads, sourcecache.Policy{})
			missing, ok := errors.AsType[*source.MissingSecretError](err)
			if !ok || missing.RetryAmbient == nil {
				t.Fatalf("prepare = %v", err)
			}
			artifact, err := missing.RetryAmbient(t.Context())
			if artifact != nil || err == nil || !strings.Contains(err.Error(), map[string]string{"view": "validate git view", "publication": "blobs"}[phase]) {
				t.Fatalf("ambient %s failure = %v, %v", phase, artifact, err)
			}
			<-closed
			if opened.Load() != 1 || tr.authenticated.Load() == 0 {
				t.Fatalf("post-auth failure: opened=%d authenticated=%d", opened.Load(), tr.authenticated.Load())
			}
		})
	}
}

func TestFetcher_AmbientSubmodulesShareAttempt(t *testing.T) {
	identity, key := testSSHIdentity(t)
	_, host := testSSHIdentity(t)
	closed, opened := acquisitionAgent(t, identity)
	homeDir := t.TempDir()
	t.Setenv("HOME", homeDir)
	testutil.WriteFile(t, homeDir, ".ssh/known_hosts", knownhosts.Line([]string{"fixture"}, host.PublicKey())+"\n")
	root, leaf := t.TempDir(), t.TempDir()
	mustInitRepo(t, leaf)
	mustInitRepoWithFiles(t, root, map[string]string{"hello.txt": "parent", ".gitmodules": "[submodule \"lib\"]\n\tpath = lib\n\turl = ssh://git@fixture" + leaf + "\n"})
	repository, err := gitlib.PlainOpen(root)
	assert.NoError(t, err)
	entries, err := repository.Storer.Index()
	assert.NoError(t, err)
	entries.Entries = append(entries.Entries, &index.Entry{Name: "lib", Mode: filemode.Submodule, Hash: plumbing.NewHash(mustHead(t, leaf))})
	assert.NoError(t, repository.Storer.SetIndex(entries))
	worktree, err := repository.Worktree()
	assert.NoError(t, err)
	_, err = worktree.Commit("fixture submodule", &gitlib.CommitOptions{Author: &object.Signature{Name: "fixture", Email: "fixture@example.test", When: time.Unix(1, 0)}})
	assert.NoError(t, err)
	tr := &agentAuthentication{Transport: server.DefaultServer, host: host.PublicKey(), allowed: key.PublicKey()}
	previous := client.Protocols["ssh"]
	client.InstallProtocol("ssh", tr)
	t.Cleanup(func() { client.InstallProtocol("ssh", previous) })
	repo := gitRepoWithSecret("modules", "ssh://git@fixture"+root, "auth")
	repo.RecurseSubmodules = true
	reads := (testInputs{}).readSet(repo)
	defer reads.Close()
	f := &Fetcher{Cache: cachetest.New(t, "", sourcecache.New)}
	_, err = f.Fetch(t.Context(), repo, reads, sourcecache.Policy{})
	missing, ok := errors.AsType[*source.MissingSecretError](err)
	if !ok || missing.RetryAmbient == nil {
		t.Fatalf("prepare = %v", err)
	}
	artifact, err := missing.RetryAmbient(t.Context())
	assert.NoError(t, err)
	<-closed
	if opened.Load() != 1 || tr.authenticated.Load() < 2 {
		t.Fatalf("attempt sharing: opened=%d authenticated=%d", opened.Load(), tr.authenticated.Load())
	}
	data, err := fs.ReadFile(artifact.FS, "lib/hello.txt")
	assert.NoError(t, err)
	assert.Equal(t, string(data), "hi")
}
