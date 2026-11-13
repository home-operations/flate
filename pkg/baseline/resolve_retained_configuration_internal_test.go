package baseline

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/go-git/go-git/v6"
	"github.com/go-git/go-git/v6/config"
	"github.com/go-git/go-git/v6/plumbing"
	"github.com/go-git/go-git/v6/storage"
	"github.com/go-git/go-git/v6/storage/memory"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/internal/testutil/networktest"
	"github.com/home-operations/flate/pkg/manifest"
	sourcecache "github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/source/gitrepo"
	"github.com/home-operations/flate/pkg/source/gittree"
)

func TestResolve_RetainedConfiguration(t *testing.T) {
	t.Parallel()
	for _, route := range []string{"upstream", "origin"} {
		t.Run(route, func(t *testing.T) {
			t.Parallel()
			root := t.TempDir()
			base := initRepoWithFile(t, root, "a.yaml", "base")
			repo := openHelper(t, root)
			head, err := repo.Head()
			assert.NoError(t, err)
			var explicit string
			if route == "upstream" {
				setRef(t, repo, plumbing.NewRemoteReferenceName("upstream", "trunk"), base)
				configureBranch(
					t,
					repo,
					&config.Branch{
						Name:   head.Name().Short(),
						Remote: "upstream",
						Merge:  plumbing.NewBranchReferenceName("trunk"),
					},
				)
				writeAndCommit(t, root, "a.yaml", "head")
			} else {
				remote := t.TempDir()
				base = initRepoWithFile(t, remote, "a.yaml", "remote")
				setRef(t, openHelper(t, remote), plumbing.NewBranchReferenceName("trunk"), base)
				addOrigin(t, root, filepath.Join(remote, ".git"))
				explicit = "trunk"
			}
			input, _, err := AdmitDirectory(t.Context(), root, "", manifest.WorkScope{})
			assert.NoError(t, err)
			location := input.FS.(*gittree.FS).Location()
			assert.NoError(t, os.Remove(filepath.Join(root, ".git", "config")))
			resolved, err := (&selection{repo: location.Repository, owner: cachetest.New(t, "", sourcecache.New), network: networktest.New(t)}).resolve(
				t.Context(),
				location,
				explicit,
			)
			assert.NoError(t, err)
			assert.Equal(t, resolved.Hash, base)
			if route == "upstream" {
				assert.Equal(t, resolved.Source, "merge-base with @{u}")
			} else if !strings.Contains(resolved.Source, "fetched from origin") {
				t.Fatalf("origin selection = %q", resolved.Source)
			}
		})
	}
}

func TestAdmitDirectory_ExplicitRootRetainsRepository(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	initRepoWithFile(t, root, "cluster/a.yaml", "body")
	const url = "https://example.invalid/cluster.git"
	addOrigin(t, root, url)
	content := filepath.Join(root, "cluster")
	input, scan, err := AdmitDirectory(t.Context(), content, content, manifest.WorkScope{})
	assert.NoError(t, err)
	location := input.FS.(*gittree.FS).Location()
	assert.Equal(t, scan, content)
	assert.Equal(t, input.LocalRoot, content)
	assert.Equal(t, location.ContentRoot, content)
	assert.Equal(t, location.RepositoryRoot, root)
	assert.NoError(t, os.Remove(filepath.Join(root, ".git", "config")))
	got, ok := originURL(location.Config)
	assert.Equal(t, ok, true)
	assert.Equal(t, got, url)
	body, err := fs.ReadFile(input.FS, "a.yaml")
	assert.NoError(t, err)
	assert.Equal(t, string(body), "body")
	_, _, err = AdmitDirectory(t.Context(), root, content, manifest.WorkScope{})
	if err == nil || !strings.Contains(err.Error(), "outside the source root") {
		t.Fatalf("escaped scan was admitted: %v", err)
	}
}

func TestResolvePaths_ReusesRoot(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	initRepoWithFile(t, root, "current/a.yaml", "current")
	testutil.WriteFile(t, root, "original/a.yaml", "original")
	for _, sameScan := range []bool{false, true} {
		t.Run(fmt.Sprintf("same_scan_%t", sameScan), func(t *testing.T) {
			t.Parallel()
			current, original := filepath.Join(root, "current"), filepath.Join(root, "original")
			if sameScan {
				original = current
			}
			result, err := ResolvePaths(t.Context(), current, original)
			assert.NoError(t, err)
			assert.Equal(t, result.Source, result.BaseInput)
			assert.Equal(t, result.ScanPath, current)
			assert.Equal(t, result.BaseScanPath, original)
			missing := filepath.Join(root, "missing")
			_, err = ResolvePaths(t.Context(), missing, original)

			assert.FatalIf(t, !errors.Is(err, fs.ErrNotExist), "missing same-root scan was admitted: %v", err)

			assert.Equal(t, errors.Is(err, manifest.ErrInput), true)
			assert.Equal(t, errors.Is(err, manifest.ErrFlux), true)
			assert.Equal(t, strings.Contains(err.Error(), missing), true)
		})
	}
}

func TestResolvePaths_IndependentGitRules(t *testing.T) {
	t.Parallel()
	current, original := t.TempDir(), t.TempDir()
	for _, root := range []string{current, original} {
		initRepoWithFile(t, root, "entry/tracked.yaml", "tracked")
		testutil.WriteFile(t, root, "entry/current-only.yaml", "current")
		testutil.WriteFile(t, root, "entry/original-only.yaml", "original")
	}
	testutil.WriteFile(t, current, ".gitignore", "current-only.yaml\n")
	testutil.WriteFile(t, original, ".gitignore", "original-only.yaml\n")
	result, err := ResolvePaths(t.Context(), filepath.Join(current, "entry"), filepath.Join(original, "entry"))
	assert.NoError(t, err)
	for _, side := range []struct {
		input        fs.FS
		hidden, kept string
	}{
		{result.Source.FS, "entry/current-only.yaml", "entry/original-only.yaml"},
		{result.BaseInput.FS, "entry/original-only.yaml", "entry/current-only.yaml"},
	} {
		_, readErr := fs.ReadFile(side.input, side.hidden)
		assert.Equal(t, errors.Is(readErr, fs.ErrNotExist), true)
		_, readErr = fs.ReadFile(side.input, side.kept)
		assert.NoError(t, readErr)
		data, readErr := fs.ReadFile(side.input, "entry/tracked.yaml")
		assert.NoError(t, readErr)
		assert.Equal(t, string(data), "tracked")
	}
}

func TestResolve_InputErrors(t *testing.T) {
	t.Parallel()
	headCause := errors.New("read HEAD")
	for _, tc := range []struct {
		name  string
		cause error
	}{
		{name: "HEAD lookup", cause: headCause},
		{name: "missing HEAD commit", cause: plumbing.ErrObjectNotFound},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var storer storage.Storer = memory.NewStorage()
			if tc.name == "HEAD lookup" {
				storer = &headErrorStorage{Storer: storer, err: tc.cause}
			} else {
				assert.NoError(
					t,
					storer.SetReference(
						plumbing.NewHashReference(plumbing.HEAD, plumbing.NewHash(strings.Repeat("a", 40))),
					),
				)
			}
			_, err := (&selection{repo: &git.Repository{Storer: storer}}).resolve(t.Context(), &gitrepo.Location{}, "")
			assert.Equal(t, errors.Is(err, manifest.ErrInput), true)
			assert.Equal(t, errors.Is(err, manifest.ErrFlux), true)
			assert.Equal(t, errors.Is(err, tc.cause), true)
		})
	}
}

type headErrorStorage struct {
	storage.Storer

	err error
}

func (s *headErrorStorage) Reference(plumbing.ReferenceName) (*plumbing.Reference, error) {
	return nil, s.err
}
