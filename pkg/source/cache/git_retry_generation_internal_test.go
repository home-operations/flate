package cache

import (
	"context"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/go-git/go-billy/v5"
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/storage"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil/cachetest"
)

func testGitRetryDiscardsGenerationCase(t *testing.T, disk *bool,
) {
	t.Helper()

	root := ""
	if *disk {
		root = t.TempDir()
	}
	c := cachetest.New(t, root, New)
	calls := 0
	g, _, err := c.Git(
		t.Context(),
		GitKey{Content: "partial-git", RecurseSubmodules: true},
		&Policy{MaxAge: Forever, Retry: fastRetry(2)},
		func(callbackArg1 context.Context, callbackArg2 storage.Storer, worktree billy.Filesystem) (plumbing.Hash, error) {
			return testGitRetryDiscardsGenerationCaseCallback(
				callbackArg1,
				callbackArg2,
				worktree,
				testGitRetryDiscardsGenerationCaseCallbackInputs{t: &t, disk: &disk, c: &c, calls: &calls},
			)
		},
	)
	assert.FatalIf(t, err != nil || g == nil || calls != 2, "generation=%v, calls=%d, error=%v", g, calls, err)
	held, _, err := c.Git(
		t.Context(),
		GitKey{Content: "partial-git", RecurseSubmodules: true},
		&Policy{MaxAge: Forever},
		func(context.Context, storage.Storer, billy.Filesystem) (plumbing.Hash, error) {
			calls++
			return plumbing.ZeroHash, nil
		},
	)
	assert.FatalIf(
		t,
		err != nil || held != g || calls != 2,
		"warm generation=%v, calls=%d, error=%v",
		held,
		calls,
		err,
	)
}

type testGitRetryDiscardsGenerationCaseCallbackInputs struct {
	t     **testing.T
	disk  **bool
	c     **Cache
	calls *int
}

func testGitRetryDiscardsGenerationCaseCallback(
	_ context.Context,
	_ storage.Storer,
	worktree billy.Filesystem,
	testCaseInput testGitRetryDiscardsGenerationCaseCallbackInputs,
) (plumbing.Hash, error) {
	(*testCaseInput.calls)++
	{
		_, err := worktree.Stat("partial")
		assert.FatalIf((*testCaseInput.t), !errors.Is(err, fs.ErrNotExist),
			"attempt inherited partial worktree: %v",
			err,
		)
	}
	if (*testCaseInput.calls) == 1 {
		f, err := worktree.Create("partial")
		if err != nil {
			return plumbing.ZeroHash, err
		}
		if err := f.Close(); err != nil {
			return plumbing.ZeroHash, err
		}
		return plumbing.ZeroHash, connReset()
	}
	if *(*testCaseInput.disk) {
		entries, err := os.ReadDir(filepath.Dir(filepath.Join((*testCaseInput.c).root.Name(), gitObjectsPath(""))))
		assert.FatalIf((*testCaseInput.t), err != nil || len(entries) != 1,
			"failed generation remains: %v, %v",
			entries,
			err,
		)
	}
	return plumbing.NewHash(strings.Repeat("1", 40)), nil
}
