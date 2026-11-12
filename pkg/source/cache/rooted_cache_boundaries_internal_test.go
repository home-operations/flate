package cache

import (
	"context"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/go-git/go-billy/v6"
	"github.com/go-git/go-git/v6/plumbing"
	"github.com/go-git/go-git/v6/storage"
	"github.com/google/go-cmp/cmp"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/pkg/manifest"
)

func TestCache_RootedWritesRejectEscape(t *testing.T) {
	t.Parallel()
	for _, sink := range []string{"staging", "last-use", "last-use-parent", "git-generation", "staging-cleanup-parent", "staging-cleanup-final"} {
		t.Run(sink, func(t *testing.T) {
			t.Parallel()
			testCacheRootedWritesRejectEscapeCase(t, &sink)
		},
		)
	}
}

func TestGit_FailedCleanupRejectsEscapingParent(t *testing.T) {
	t.Parallel()
	for _, phase := range []string{"producer", "publication"} {
		t.Run(phase, func(t *testing.T) {
			t.Parallel()
			testGitFailedCleanupRejectsEscapingParentCase(
				t,
				&phase,
			)
		},
		)
	}
}

func testCacheRootedWritesRejectEscapeCase(t *testing.T, sink *string,
) {
	t.Helper()

	c := cachetest.New(t, t.TempDir(), New)
	outside := t.TempDir()
	testutil.WriteFile(t, outside, "marker", "untouched")
	key := BlobKey{Content: "rooted-write"}
	if (*sink) == "git-generation" {
		key.Content = "source-git-rooted"
	}
	link, target := filepath.Join(c.root.Name(), ociDir), outside
	hit := strings.HasPrefix((*sink), "last-use")
	cleanup := strings.HasPrefix((*sink), "staging-cleanup")
	if hit {
		_, _, id, err := c.Blob(
			t.Context(),
			key,
			&Policy{MaxAge: Forever},
			producer([]byte("content"), &ocispec.Descriptor{}),
		)
		assert.NoError(t, err)

		e, _ := c.entries.Load(id)
		payload := blobPath(e.(*entry).stored.Digest)
		link = filepath.Join(c.root.Name(), payload)
		target = filepath.Join(outside, "marker")
		if (*sink) == "last-use-parent" {
			link = filepath.Dir(link)
			target = outside
			testutil.WriteFile(t, outside, filepath.Base(payload), "outside payload")
		}
	} else if (*sink) == "git-generation" {
		link = filepath.Join(filepath.Join(c.root.Name(), ociDir), "git")
		{
			err := os.Mkdir(link, 0o750)
			assert.NoError(t, err)
		}
	}
	if !cleanup {
		{
			err := os.Rename(link, link+"-saved")
			assert.NoError(t, err)
		}
		{
			err := os.Symlink(target, link)
			assert.NoError(t, err)
		}
	}
	before := sweepSnapshot(t, outside)
	failure := errors.New("producer failed after staging replacement")
	called := false
	var err error
	if (*sink) == "git-generation" {
		var g *GitGeneration
		g, _, err = c.Git(t.Context(), GitKey{Content: key.Content}, &Policy{MaxAge: 0}, func(context.Context,

			storage.Storer, billy.Filesystem,
		) (plumbing.Hash, error) {
			called = true
			return plumbing.ZeroHash, nil
		})
		assert.FatalIf(t, g != nil, "%s", "escape returned a Git generation")
	} else {
		var r fs.FS
		r, _, _, err = c.Blob(
			t.Context(),
			key,
			&Policy{MaxAge: Forever},
			func(callbackArg1 context.Context, callbackArg2 ProducerInput) (ProducerResult, error) {
				return testCacheRootedWritesRejectEscapeCaseCallback(
					callbackArg1,
					callbackArg2, &testCacheRootedWritesRejectEscapeCaseCallbackInputs{
						t:       &t,
						sink:    &sink,
						c:       &c,
						outside: &outside,
						link:    &link,
						target:  &target,
						cleanup: &cleanup,
						before:  &before,
						failure: &failure,
						called:  &called,
					},
				)
			},
		)
		assert.FatalIf(t, r != nil, "%s", "escape returned a blob reader")
	}
	if cleanup {
		assert.FatalIf(
			t,
			!called || !errors.Is(err, failure) || !errors.Is(err, manifest.ErrFlux),
			"staging cleanup: producer=%v, error=%v",
			called,
			err,
		)
	} else {
		_, ok := errors.AsType[*CacheError](err)
		assert.FatalIf(
			t,
			!ok || called || !errors.Is(err, manifest.ErrFlux),
			"escape: producer=%v, error=%v",
			called,
			err,
		)
	}
	{
		diff := cmp.Diff(before, sweepSnapshot(t, outside))
		assert.FatalIf(t, diff != "", "outside changed (-want +got):\n%s", diff)
	}
	c.entries.Range(func(_, v any) bool {
		if !v.(*entry).resolved.IsZero() {
			t.Error("failed acquisition installed a successful entry")
		}
		return true
	})
	if (*sink) == "staging-cleanup-final" {
		{
			_, err := os.Lstat(link)
			assert.FatalIf(t, !errors.Is(err, fs.ErrNotExist), "staging cleanup retained final link: %v", err)
		}
	} else {
		err := os.Remove(link)
		assert.NoError(t, err)
	}
	{
		err := os.Rename(link+"-saved", link)
		assert.NoError(t, err)
	}
	{
		err := c.store.Tags(t.Context(), "", func(tags []string) error {
			assert.FatalIf(t, len(tags) > 1 || hit != (len(tags) == 1), "publication after failure: %v", tags)
			return nil
		})
		assert.NoError(t, err)
	}
}

type testCacheRootedWritesRejectEscapeCaseCallbackInputs struct {
	t       **testing.T
	sink    **string
	c       **Cache
	outside *string
	link    *string
	target  *string
	cleanup *bool
	before  *map[string]string
	failure *error
	called  *bool
}

func testCacheRootedWritesRejectEscapeCaseCallback(
	_ context.Context,
	_ ProducerInput, testCaseInputValue *testCacheRootedWritesRejectEscapeCaseCallbackInputs,
) (ProducerResult, error) {
	testCaseInput := *testCaseInputValue
	(*testCaseInput.called) = true
	if !(*testCaseInput.cleanup) {
		return ProducerResult{}, nil
	}
	entries, readDirErr := fs.ReadDir((*testCaseInput.c).root.FS(), "oci")
	assert.NoError((*testCaseInput.t), readDirErr)

	var staging []string
	for _, entry := range entries {
		if strings.HasPrefix(entry.Name(), "acquire-") {
			staging = append(staging, entry.Name())
		}
	}
	assert.FatalIf((*testCaseInput.t), len(staging) != 1, "exclusive staging files = %v", staging)
	name := filepath.Join("oci", staging[0])
	info, readDirErr := (*testCaseInput.c).root.Stat(name)
	assert.FatalIf((*testCaseInput.t), readDirErr != nil || info.Mode().Perm() != 0o600,
		"staging permissions: %v, %v",
		info,
		readDirErr,
	)
	testutil.WriteFile((*testCaseInput.t), (*testCaseInput.outside), staging[0], "outside staging")
	if (*(*testCaseInput.sink)) == "staging-cleanup-final" {
		(*testCaseInput.link) = filepath.Join((*testCaseInput.c).root.Name(), name)
		(*testCaseInput.target) = filepath.Join((*testCaseInput.outside), staging[0])
	}
	{
		err := os.Rename((*testCaseInput.link), (*testCaseInput.link)+"-saved")
		assert.NoError((*testCaseInput.t), err)
	}
	{
		err := os.Symlink((*testCaseInput.target), (*testCaseInput.link))
		assert.NoError((*testCaseInput.t), err)
	}
	(*testCaseInput.before) = sweepSnapshot((*testCaseInput.t), (*testCaseInput.outside))
	return ProducerResult{}, (*testCaseInput.failure)
}

func testGitFailedCleanupRejectsEscapingParentCase(
	t *testing.T, phase *string,
) {
	t.Helper()

	c := cachetest.New(t, t.TempDir(), New)
	outside := t.TempDir()
	key := GitKey{Content: "source-git-escaping-cleanup", RecurseSubmodules: true}
	parent := filepath.Dir(filepath.Join(c.root.Name(), gitObjectsPath("")))
	index := filepath.Join(c.root.Name(), indexPath())
	failure := errors.New("producer failed after parent replacement")
	var before map[string]string
	g, _, err := c.Git(
		t.Context(),
		key,
		&Policy{MaxAge: 0},
		func(callbackArg1 context.Context, callbackArg2 storage.Storer, worktree billy.Filesystem) (plumbing.Hash, error) {
			return testGitFailedCleanupRejectsEscapingParentCaseCallback(
				callbackArg1,
				callbackArg2,
				worktree,
				testGitFailedCleanupRejectsEscapingParentCaseCallbackInputs{
					t:       &t,
					phase:   &phase,
					outside: &outside,
					parent:  &parent,
					index:   &index,
					failure: &failure,
					before:  &before,
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
	if (*phase) == "publication" {
		{
			_, ok := errors.AsType[*CacheError](err)
			assert.FatalIf(t, !ok, "publication error = %v", err)
		}
	} else {
		assert.FatalIf(t, !errors.Is(err, failure), "producer error = %v", err)
	}
	{
		diff := cmp.Diff(before, sweepSnapshot(t, outside))
		assert.FatalIf(t, diff != "", "outside changed (-want +got):\n%s", diff)
	}
	c.entries.Range(func(_, v any) bool {
		if !v.(*entry).resolved.IsZero() {
			t.Error("failed Git acquisition installed a successful entry")
		}
		return true
	})
	{
		err := os.Remove(parent)
		assert.NoError(t, err)
	}
	{
		err := os.Rename(parent+"-saved", parent)
		assert.NoError(t, err)
	}

	if (*phase) == "publication" {
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

type testGitFailedCleanupRejectsEscapingParentCaseCallbackInputs struct {
	t       **testing.T
	phase   **string
	outside *string
	parent  *string
	index   *string
	failure *error
	before  *map[string]string
}

func testGitFailedCleanupRejectsEscapingParentCaseCallback(
	_ context.Context,
	_ storage.Storer,
	worktree billy.Filesystem,
	testCaseInput testGitFailedCleanupRejectsEscapingParentCaseCallbackInputs,
) (plumbing.Hash, error) {
	id := filepath.Base(filepath.Dir(worktree.Root()))
	testutil.WriteFile((*testCaseInput.t), (*testCaseInput.outside), filepath.Join(id, "marker"), "untouched")
	(*testCaseInput.before) = sweepSnapshot((*testCaseInput.t), (*testCaseInput.outside))
	if err := os.Rename((*testCaseInput.parent), (*testCaseInput.parent)+"-saved"); err != nil {
		return plumbing.ZeroHash, err
	}
	if err := os.Symlink((*testCaseInput.outside), (*testCaseInput.parent)); err != nil {
		return plumbing.ZeroHash, err
	}
	if (*(*testCaseInput.phase)) == "publication" {
		if err := os.Rename((*testCaseInput.index), (*testCaseInput.index)+"-saved"); err != nil {
			return plumbing.ZeroHash, err
		}
		return plumbing.ZeroHash, os.Mkdir((*testCaseInput.index), 0o750)
	}
	return plumbing.ZeroHash, (*testCaseInput.failure)
}
