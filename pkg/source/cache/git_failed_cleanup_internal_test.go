package cache

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/go-git/go-billy/v6"
	"github.com/go-git/go-git/v6/plumbing"
	"github.com/go-git/go-git/v6/storage"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/pkg/manifest"
)

func TestGit_FailedAttemptRemovesDirectories(t *testing.T) {
	t.Parallel()
	for _, scenario := range []string{"producer", "cancelled", "publication"} {
		t.Run(scenario, func(t *testing.T) {
			t.Parallel()
			testGitFailedAttemptRemovesDirectoriesCase(
				t,
				&scenario,
			)
		},
		)
	}
}

func testGitFailedAttemptRemovesDirectoriesCase(
	t *testing.T, scenario *string,
) {
	t.Helper()

	c := cachetest.New(t, t.TempDir(), New)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	failure := errors.New("incomplete clone")
	g, _, err := c.Git(
		ctx,
		GitKey{Content: "source-git-failed", RecurseSubmodules: true},
		&Policy{MaxAge: 0},
		func(callbackArg1 context.Context, callbackArg2 storage.Storer, worktree billy.Filesystem) (plumbing.Hash, error) {
			return testGitFailedAttemptRemovesDirectoriesCaseCallback(
				callbackArg1,
				callbackArg2,
				worktree,
				testGitFailedAttemptRemovesDirectoriesCaseCallbackInputs{
					scenario: &scenario,
					c:        &c,
					cancel:   &cancel,
					failure:  &failure,
				},
			)
		},
	)
	assert.FatalIf(
		t,
		g != nil || !errors.Is(err, manifest.ErrFlux),
		"failed Git acquisition: generation=%v, error=%v",
		g,
		err,
	)
	want := failure
	if (*scenario) == "cancelled" {
		want = context.Canceled
	}
	if (*scenario) == "publication" {
		{
			_, ok := errors.AsType[*CacheError](err)
			assert.FatalIf(t, !ok, "publication error = %v", err)
		}
	} else {
		assert.FatalIf(t, !errors.Is(err, want), "Git error = %v, want %v", err, want)
	}
	entries, err := os.ReadDir(filepath.Dir(filepath.Join(c.root.Name(), gitObjectsPath(""))))
	assert.FatalIf(t, err != nil || len(entries) != 0, "failed generation directories = %v, %v", entries, err)
	c.entries.Range(func(_, v any) bool {
		if !v.(*entry).resolved.IsZero() {
			t.Error("failed Git acquisition installed a successful entry")
		}
		return true
	})
	if (*scenario) == "publication" {
		index := filepath.Join(c.root.Name(), indexPath())
		{
			err := os.Remove(index)
			assert.NoError(t, err)
		}
		{
			err := os.Rename(index+"-saved", index)
			assert.NoError(t, err)
		}
	}
	{
		err := c.Close()
		assert.NoError(t, err)
	}

	reopened := cachetest.New(t, c.root.Name(), New)
	{
		err := reopened.store.Tags(t.Context(), "", func(tags []string) error {
			assert.FatalIf(t, len(tags) != 0, "failed generation published durable tags: %v", tags)
			return nil
		})
		assert.NoError(t, err)
	}
}

type testGitFailedAttemptRemovesDirectoriesCaseCallbackInputs struct {
	scenario **string
	c        **Cache
	cancel   *context.CancelFunc
	failure  *error
}

func testGitFailedAttemptRemovesDirectoriesCaseCallback(
	_ context.Context,
	_ storage.Storer,
	worktree billy.Filesystem,
	testCaseInput testGitFailedAttemptRemovesDirectoriesCaseCallbackInputs,
) (plumbing.Hash, error) {
	file, err := worktree.Create("partial")
	if err != nil {
		return plumbing.ZeroHash, err
	}
	if err := file.Close(); err != nil {
		return plumbing.ZeroHash, err
	}
	if (*(*testCaseInput.scenario)) == "publication" {
		index := filepath.Join((*testCaseInput.c).root.Name(), indexPath())
		if err := os.Rename(index, index+"-saved"); err != nil {
			return plumbing.ZeroHash, err
		}
		return plumbing.ZeroHash, os.Mkdir(index, 0o750)
	}
	if (*(*testCaseInput.scenario)) == "cancelled" {
		(*testCaseInput.cancel)()
		return plumbing.ZeroHash, nil
	}
	return plumbing.ZeroHash, (*testCaseInput.failure)
}
