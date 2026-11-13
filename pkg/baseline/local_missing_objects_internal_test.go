package baseline

import (
	"bytes"
	"context"
	"errors"
	"io"
	"io/fs"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/go-git/go-git/v6"
	"github.com/go-git/go-git/v6/plumbing"
	"github.com/go-git/go-git/v6/plumbing/format/packfile"
	"github.com/go-git/go-git/v6/plumbing/protocol"
	"github.com/go-git/go-git/v6/plumbing/protocol/capability"
	"github.com/go-git/go-git/v6/plumbing/transport"
	"github.com/go-git/go-git/v6/plumbing/transport/file"
	"github.com/go-git/go-git/v6/storage/memory"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/internal/testutil/networktest"
	"github.com/home-operations/flate/pkg/manifest"
	sourcecache "github.com/home-operations/flate/pkg/source/cache"
	sourcegit "github.com/home-operations/flate/pkg/source/git"
	"github.com/home-operations/flate/pkg/source/gitrepo"
	"github.com/home-operations/flate/pkg/source/gittree"
)

func TestAutoResolve_LocalBaseMissingObjects(t *testing.T) {
	t.Parallel()
	for _, missing := range []string{"tree", "clean_candidate_blob", "policy_membership_missing_blob"} {
		t.Run(missing, func(t *testing.T) {
			t.Parallel()
			testAutoResolveLocalBaseMissingObjectsCase(t, missing)
		})
	}
}

func testAutoResolveLocalBaseMissingObjectsCase(t *testing.T, missing string) {
	t.Helper()

	root, origin := t.TempDir(), t.TempDir()
	pin := initRepoWithFile(t, root, "apps/file.yaml", "base")
	headPath, headBody, currentBody := "apps/file.yaml", "head", "head"
	if missing == "policy_membership_missing_blob" {
		pin = writeAndCommit(t, root, "apps/.sourceignore", ".sourceignore\nfile.yaml\n")
		headPath, headBody, currentBody = "apps/.sourceignore", ".sourceignore\n", "base"
	}
	repo := openHelper(t, root)
	setRef(t, repo, plumbing.NewBranchReferenceName("selected"), pin)
	writeAndCommit(t, root, headPath, headBody)
	testutil.CopyTree(t, root, origin)
	advanced := writeAndCommit(t, origin, "apps/file.yaml", "moving remote")
	remote := openHelper(t, origin)
	setRef(t, remote, plumbing.NewBranchReferenceName("selected"), advanced)
	setRef(t, remote, plumbing.NewBranchReferenceName("pinned"), pin)
	url := filepath.Join(origin, ".git")
	addOrigin(t, root, url)
	commit, err := repo.CommitObject(pin)
	assert.NoError(t, err)
	tree, err := commit.Tree()
	assert.NoError(t, err)
	absent := tree.Hash
	if missing != "tree" {
		entry, localErr := tree.FindEntry("apps/file.yaml")
		assert.NoError(t, localErr)
		absent = entry.Hash
	}
	oid := absent.String()
	objectPath := filepath.Join(root, ".git", "objects", oid[:2], oid[2:])
	assert.NoError(t, os.Remove(objectPath))
	repo = openHelper(t, root)
	_, err = repo.CommitObject(pin)
	assert.NoError(t, err)
	wt, err := repo.Worktree()
	assert.NoError(t, err)
	status, err := wt.Status()
	assert.NoError(t, err)
	assert.Equal(t, status.IsClean(), true)
	if missing == "policy_membership_missing_blob" {
		location, localErr := gitrepo.Locate(t.Context(), root, "")
		assert.NoError(t, localErr)
		directory, localErr := admitLocation(t.Context(), location, manifest.WorkScope{})
		assert.NoError(t, localErr)
		t.Cleanup(func() { _ = directory.Close() })
		input := directory.Artifact()
		entries, comparison, localErr := treeEntries(t.Context(), tree)
		assert.NoError(t, localErr)
		s := selection{
			repo:     repo,
			worktree: comparisonWorktree(t, input.FS.(*gittree.FS)),
			current:  input.FS.(*gittree.FS),
		}
		candidates, warnings, localErr := s.candidates(t.Context(), entries, comparison)
		assert.NoError(t, localErr)
		assert.Equal(t, len(warnings), 0)
		assert.Diff(t, slices.Sorted(maps.Keys(candidates)), []string{"apps/.sourceignore"})
	}
	owner := cachetest.New(t, "", sourcecache.New)
	var identity string
	network := networktest.New(t)
	var calls atomic.Int32
	server := file.NewTransport(file.Options{})
	connect := func(ctx context.Context, request sourcegit.TransportRequest) (sourcegit.Connection, error) {
		calls.Add(1)
		assert.Equal(t, request.URL.Path, url)
		assert.Equal(t, request.Command, transport.UploadPackService)
		return server.Connect(
			ctx,
			&transport.Request{
				URL:      request.URL,
				Command:  request.Command,
				Protocol: protocol.Version(request.Protocol),
			},
		)
	}
	for round := range 2 {
		routes := sourcegit.Transports{"file": connect}
		initialCalls := calls.Load()
		location, localErr := gitrepo.Locate(t.Context(), root, "")
		assert.NoError(t, localErr)
		initial, localErr := (&selection{repo: repo, owner: owner, network: network, transports: routes}).resolve(
			t.Context(),
			location,
			"selected",
		)
		assert.NoError(t, localErr)
		assert.Equal(t, initial.Hash, pin)
		assert.Equal(t, calls.Load(), initialCalls)
		res, err := AutoResolve(
			t.Context(),
			Config{
				Path:          filepath.Join(root, "apps"),
				Base:          "selected",
				Cache:         owner,
				Network:       network,
				GitTransports: routes,
			},
		)
		routes["file"] = func(context.Context, sourcegit.TransportRequest) (sourcegit.Connection, error) {
			return nil, errors.New("caller changed transport after admission")
		}
		resolvedCalls := calls.Load()
		assert.NoError(t, err)
		assert.Equal(t, res.BaseInput.Revision, pin.String())
		assert.Diff(t, res.Source.Metadata, map[string]string{"baseline-selection": "explicit --base=selected"})
		assert.Diff(t, res.Changes.Paths(), []string{"apps/file.yaml"})
		label := filepath.Join(root, ".flate-base", pin.String())
		assert.Equal(t, res.Source.LocalPath, root)
		assert.Equal(t, res.Source.LocalRoot, root)
		assert.Equal(t, res.Source.Key, "")
		assert.Equal(t, res.BaseInput.LocalPath, label)
		assert.Equal(t, res.BaseInput.LocalRoot, root)
		assert.Equal(t, res.ScanPath, filepath.Join(root, "apps"))
		assert.Equal(t, res.BaseScanPath, filepath.Join(label, "apps"))
		assert.Diff(t, res.SelfURLs, []string{url})
		assert.FatalIf(t, res.BaseInput.Key == "", "%v", "baseline identity is empty")
		if round == 0 {
			identity = res.BaseInput.Key
		}
		assert.Equal(t, res.BaseInput.Key, identity)
		assert.Equal(t, res.BaseInput.Content, res.BaseInput.Key)
		assert.Equal(t, res.BaseInput.Kind, manifest.KindGitRepository)
		before, err := fs.ReadFile(res.BaseInput.FS, "apps/file.yaml")
		assert.NoError(t, err)
		assert.Equal(t, string(before), "base")
		assert.Equal(t, calls.Load(), resolvedCalls)
		assert.Equal(t, calls.Load() > initialCalls, true)
		after, err := fs.ReadFile(res.Source.FS, "apps/file.yaml")
		assert.NoError(t, err)
		assert.Equal(t, string(after), currentBody)
		if missing == "policy_membership_missing_blob" {
			for _, mode := range []gittree.ViewMode{gittree.Direct, gittree.Source} {
				derived, derivedIdentity, localErr := gittree.New(t.Context(), nil, plumbing.ZeroHash,
					gittree.ViewOptions{Mode: mode, Current: res.BaseInput.FS})
				assert.NoError(t, localErr)
				data, readErr := fs.ReadFile(derived, "apps/file.yaml")
				if mode == gittree.Direct {
					assert.NoError(t, readErr)
					assert.Equal(t, string(data), "base")
					assert.Equal(t, derivedIdentity, identity)
					data, readErr = fs.ReadFile(derived, "apps/.sourceignore")
					assert.NoError(t, readErr)
					assert.Equal(t, string(data), ".sourceignore\nfile.yaml\n")
				} else {
					assert.Equal(t, errors.Is(readErr, fs.ErrNotExist), true)
				}
			}
		}
		assert.Equal(t, errors.Is(repo.Storer.HasEncodedObject(absent), plumbing.ErrObjectNotFound), true)
		_, err = os.Stat(objectPath)
		assert.Equal(t, errors.Is(err, fs.ErrNotExist), true)
	}
}

func TestSelection_AcquireBodiesCacheKeys(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	initRepoWithFile(t, root, "one.yaml", "one")
	pin := writeAndCommit(t, root, "two.yaml", "two")
	origin := openHelper(t, root)
	commit, err := origin.CommitObject(pin)
	assert.NoError(t, err)
	tree, err := commit.Tree()
	assert.NoError(t, err)
	entries, _, err := treeEntries(t.Context(), tree)
	assert.NoError(t, err)
	partial := memory.NewStorage()
	assert.NoError(t, copyMetadata(t.Context(), origin.Storer, partial, pin))
	caps := new(capability.List)
	caps.Set(capability.AllowReachableSHA1InWant)
	client := &fetchPolicyTransport{capabilities: caps}
	client.upload = func() {
		var packed bytes.Buffer
		_, encodeErr := packfile.NewEncoder(&packed, origin.Storer, false).
			Encode(client.requests[len(client.requests)-1].Wants, 0)
		assert.NoError(t, encodeErr)
		client.pack = packed.Bytes()
	}
	repo := &git.Repository{Storer: partial}
	s := selection{
		repo: repo,
		base: &resolution{
			Hash: pin, Repo: repo,
			remote: &baseRemote{client: client, url: "file:///fixture", pin: pin},
		},
		owner: cachetest.New(t, "", sourcecache.New),
	}
	for _, name := range []string{"one.yaml", "two.yaml", "one.yaml", "two.yaml"} {
		assert.NoError(t, s.acquireBodies(t.Context(), entries, map[string]bool{name: true}))
		blob, readErr := s.base.Repo.BlobObject(entries[name].Hash)
		assert.NoError(t, readErr)
		reader, readErr := blob.Reader()
		assert.NoError(t, readErr)
		body, readErr := io.ReadAll(reader)
		assert.NoError(t, reader.Close())
		assert.NoError(t, readErr)
		assert.Equal(t, string(body), strings.TrimSuffix(name, ".yaml"))
	}
	assert.Equal(t, len(client.requests), 2)
}
