package cache

import (
	"bytes"
	"context"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/go-git/go-billy/v5"
	"github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/object"
	"github.com/go-git/go-git/v5/storage"
	"github.com/google/go-cmp/cmp"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source/gittree"
)

func testGitRestartAndInvalidationCase(t *testing.T, scenario *string,
) {
	t.Helper()

	src, root := t.TempDir(), t.TempDir()
	repo := testutil.GitInit(t, src)
	testutil.WriteFileAt(t, filepath.Join(src, "file"), "committed")
	testutil.GitCommitAll(
		t,
		repo,
		"seed",
		&git.CommitOptions{Author: &object.Signature{Name: "t", Email: "t@example.invalid"}},
	)
	calls := 0
	produce := func(_ context.Context, storer storage.Storer, worktree billy.Filesystem) (plumbing.Hash, error) {
		calls++
		r, err := git.CloneContext(
			t.Context(),
			storer,
			worktree,
			&git.CloneOptions{URL: "file://" + src, NoCheckout: true},
		)
		if err != nil {
			return plumbing.ZeroHash, err
		}
		head, err := r.Head()
		if err != nil {
			return plumbing.ZeroHash, err
		}
		return head.Hash(), nil
	}
	c := cachetest.New(t, root, New)
	key := GitKey{Content: "source-git-restart", Ref: "HEAD"}
	first, id, err := c.Git(t.Context(), key, Policy{MaxAge: Forever}, produce)
	assert.NoError(t, err)

	archiveKey := ArchiveKey{Content: key.Content}
	var payloadTar bytes.Buffer
	testutil.WriteTar(t, &payloadTar, map[string]string{"file": "archive"}, 0o644)
	_, _, archiveID, err := c.Acquire(
		t.Context(),
		archiveKey,
		Policy{MaxAge: Forever},
		producer(payloadTar.Bytes(), &ocispec.Descriptor{}),
	)
	assert.NoError(t, err)

	blobKey := BlobKey{Content: key.Content}
	_, _, blobID, err := c.Blob(
		t.Context(),
		blobKey,
		Policy{MaxAge: Forever},
		producer([]byte("blob"), &ocispec.Descriptor{}),
	)
	assert.NoError(t, err)

	assert.FatalIf(
		t,
		id == "" || archiveID == id || blobID == id || archiveID == blobID,
		"product identities: git=%q archive=%q blob=%q",
		id,
		archiveID,
		blobID,
	)
	v, _ := c.entries.Load(id)
	e := v.(*entry)
	blob := filepath.Join(c.root.Name(), blobPath(e.stored.Digest))
	stale := time.Now().Add(-time.Hour)
	{
		chtimesErr3 := os.Chtimes(blob, stale, stale)
		assert.NoError(t, chtimesErr3)
	}

	if (*scenario) == "outcome" {
		o := newOutcome(outcomeFailure(connReset()), time.Now().UTC(), 1, &e.stored)
		for n := uint32(1); n <= 4; n++ {
			o.record.Count = n
			desc, publishOutcomeErr := c.publishOutcome(t.Context(), id, o, connReset())
			assert.NoError(t, publishOutcomeErr)

			o, publishOutcomeErr = c.readOutcome(t.Context(), &desc)
			assert.FatalIf(t, publishOutcomeErr != nil || o == nil || o.lastGood.Digest != e.stored.Digest ||
				o.lastGood.Annotations[prefix+"git-generation"] != first.id, "direct last-good anchor: %+v, %v", o, publishOutcomeErr)
		}
	}
	if (*scenario) == "missing-objects" {
		{
			removeAllErr := os.RemoveAll(
				filepath.Join(filepath.Join(c.root.Name(), gitObjectsPath(first.id)), "objects"),
			)
			assert.NoError(t, removeAllErr)
		}
	}
	{
		closeErr5 := c.Close()
		assert.NoError(t, closeErr5)
	}

	if (*scenario) == "ref-mismatch" {
		key.Ref = "tag:other"
	}
	shouldReturn := testGitRestartAndInvalidationCaseCompletion(
		t,
		&testGitRestartAndInvalidationCaseCompletionInputs{
			src:        &src,
			c:          &c,
			root:       &root,
			archiveKey: &archiveKey,
			archiveID:  &archiveID,
			blobKey:    &blobKey,
			blobID:     &blobID,
			scenario:   &scenario,
			id:         &id,
			e:          &e,
			calls:      &calls,
			first:      &first,
			key:        &key,
			produce:    &produce,
			v:          &v,
			blob:       &blob,
			stale:      &stale,
		},
	)
	if shouldReturn {
		return
	}
}

func testGitRestartAndInvalidationCaseCompletion(
	t *testing.T,
	captures *testGitRestartAndInvalidationCaseCompletionInputs,
) bool {
	t.Helper()
	{
		removeAllErr2 := os.RemoveAll((*captures.src))
		assert.NoError(t, removeAllErr2)
	}
	(*captures.c) = cachetest.New(t, (*captures.root), New)
	unexpected := func(context.Context, ProducerInput) (ProducerResult, error) {
		t.Error("restored product called producer")
		return ProducerResult{}, errors.New("unexpected producer")
	}
	input, _, restoredID, err := (*captures.c).Acquire(
		t.Context(),
		(*captures.archiveKey),
		Policy{MaxAge: Forever},
		unexpected,
	)
	assert.FatalIf(t, err != nil || restoredID != (*captures.archiveID), "restored archive: id=%q, %v", restoredID, err)
	{
		data, readFileErr5 := fs.ReadFile(input, "file")
		assert.FatalIf(
			t,
			readFileErr5 != nil || string(data) != "archive",
			"restored archive payload: %q, %v",
			data,
			readFileErr5,
		)
	}
	r, _, restoredID, err := (*captures.c).Blob(t.Context(), (*captures.blobKey), Policy{MaxAge: Forever}, nil)
	assert.FatalIf(t, err != nil || restoredID != (*captures.blobID), "restored blob: id=%q, %v", restoredID, err)
	_, _ = fs.ReadFile(r, "content")

	if (*(*captures.scenario)) == "outcome" {
		desc, resolveErr := (*captures.c).store.Resolve(t.Context(), (*captures.id))
		assert.NoError(t, resolveErr)

		o, resolveErr := (*captures.c).readOutcome(t.Context(), &desc)
		assert.FatalIf(
			t,
			resolveErr != nil || o == nil || o.record.Count != 4 || o.lastGood.Digest != (*captures.e).stored.Digest ||
				(*captures.calls) != 1,
			"restored outcome: %+v, %v",
			o,
			resolveErr,
		)
		input, _, resolveErr := gittree.New(
			t.Context(),
			(*captures.first).Backing,
			(*captures.first).Commit,
			gittree.ViewOptions{},
		)
		assert.NoError(t, resolveErr)

		{
			data, readFileErr := fs.ReadFile(input, "file")
			assert.FatalIf(
				t,
				readFileErr != nil || string(data) != "committed",
				"held Git content: %q, %v",
				data,
				readFileErr,
			)
		}
		return true
	}
	second, _, err := (*captures.c).Git(t.Context(), (*captures.key), Policy{MaxAge: Forever}, (*captures.produce))
	if (*(*captures.scenario)) == "ref-mismatch" || (*(*captures.scenario)) == "missing-objects" {
		assert.FatalIf(t, err == nil || (*captures.calls) != 2, "invalid restore: calls=%d, %v", (*captures.calls), err)
		return true
	}
	assert.FatalIf(t, err != nil || (*captures.calls) != 1, "restart hit: calls=%d, %v", (*captures.calls), err)
	input, _, err = gittree.New(t.Context(), second.Backing, second.Commit, gittree.ViewOptions{})
	assert.NoError(t, err)

	{
		data, readFileErr6 := fs.ReadFile(input, "file")
		assert.FatalIf(
			t,
			readFileErr6 != nil || string(data) != "committed",
			"restored content: %q, %v",
			data,
			readFileErr6,
		)
	}
	(*captures.v), _ = (*captures.c).entries.Load((*captures.id))
	assert.FatalIf(
		t,
		!(*captures.e).resolved.Equal((*captures.v).(*entry).resolved),
		"%s",
		"restart hit extended freshness",
	)

	info, err := os.Stat((*captures.blob))
	assert.FatalIf(t, err != nil || !info.ModTime().After((*captures.stale)), "anchor touch: %v, %v", info, err)
	if (*(*captures.scenario)) == "touch-failure" {
		{
			err := os.Remove((*captures.blob))
			assert.NoError(t, err)
		}
		(*captures.v).(*entry).touched.Store(captures.stale.UnixNano())
		{
			_, _, err := (*captures.c).Git(
				t.Context(), (*captures.key), Policy{MaxAge: Forever}, (*captures.produce),
			)
			assert.FatalIf(t, !errors.Is(
				err,
				fs.ErrNotExist,
			), "touch failure: %v", err)
		}
		{
			err := os.WriteFile((*captures.blob), []byte((*captures.first).id), 0o600)
			assert.NoError(t, err)
		}

		{
			_, _, err := (*captures.c).Git(t.Context(), (*captures.key), Policy{MaxAge: Forever}, (*captures.produce))
			assert.FatalIf(
				t,
				err == nil || (*captures.calls) != 2,
				"poisoned hit reused: calls=%d, %v",
				(*captures.calls),
				err,
			)
		}
	}
	return false
}

func testGitRestartAndInvalidationCase2(t *testing.T, disk *bool,
) {
	t.Helper()

	src := t.TempDir()
	repo := testutil.GitInit(t, src)
	testutil.WriteFile(t, src, "nested/file", "first")
	testutil.GitCommitAll(
		t,
		repo,
		"first",
		&git.CommitOptions{Author: &object.Signature{Name: "t", Email: "t@e"}},
	)
	root := ""
	if *disk {
		root = t.TempDir()
	}
	c := cachetest.New(t, root, New)
	key := GitKey{Content: "recursive-held", RecurseSubmodules: true}
	produce := func(ctx context.Context, storer storage.Storer, worktree billy.Filesystem) (plumbing.Hash, error) {
		cloneContextRepo, err := git.CloneContext(ctx, storer, worktree, &git.CloneOptions{URL: "file://" + src})
		if err != nil {
			return plumbing.ZeroHash, err
		}
		head, err := cloneContextRepo.Head()
		if err != nil {
			return plumbing.ZeroHash, err
		}
		return head.Hash(), nil
	}
	admit := func(generation *GitGeneration) (*gittree.FS, string) {
		t.Helper()
		input, pin, err := gittree.New(
			t.Context(),
			generation.Backing,
			generation.Commit,
			gittree.ViewOptions{Worktree: generation.Worktree, Content: key.Content},
		)
		assert.NoError(t, err)

		return input, pin
	}
	first, _, err := c.Git(t.Context(), key, Policy{MaxAge: 0}, produce)
	assert.NoError(t, err)

	held, pin := admit(first)
	latestPin := pin
	for _, changed := range []bool{false, true} {
		if changed {
			testutil.WriteFile(t, src, "nested/file", "second")
			testutil.GitCommitAll(
				t,
				repo,
				"second",
				&git.CommitOptions{Author: &object.Signature{Name: "t", Email: "t@e"}},
			)
		}
		next, _, err := c.Git(t.Context(), key, Policy{MaxAge: 0}, produce)
		assert.NoError(t, err)

		_, nextPin := admit(next)
		latestPin = nextPin
		assert.FatalIf(t, (nextPin != pin) != changed, "%s", "Git identity differs from acquired key/commit")

		{
			data, readFileErr7 := fs.ReadFile(held, "nested/file")
			assert.FatalIf(
				t,
				readFileErr7 != nil || string(data) != "first",
				"held recursive generation changed: %q, %v",
				data,
				readFileErr7,
			)
		}
		_, retained, err := gittree.New(
			t.Context(),
			nil,
			plumbing.ZeroHash,
			gittree.ViewOptions{Mode: gittree.Direct, Current: held},
		)
		assert.FatalIf(
			t,
			err != nil || retained != pin,
			"held recursive identity changed: %q, %v",
			retained,
			err,
		)
	}
	if *disk {
		{
			err := c.Close()
			assert.NoError(t, err)
		}

		c = cachetest.New(t, root, New)
		restored, _, err := c.Git(t.Context(), key, Policy{MaxAge: Forever}, nil)
		assert.NoError(t, err)

		input, pin := admit(restored)
		assert.FatalIf(t, pin == "" || pin != latestPin, "%s", "recursive restored identity lost")

		{
			data, err := fs.ReadFile(input, "nested/file")
			assert.FatalIf(
				t,
				err != nil || string(data) != "second",
				"recursive restoration: %q, %v",
				data,
				err,
			)
		}
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
		Policy{MaxAge: 0},
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
			Policy{MaxAge: Forever},
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
		g, _, err = c.Git(t.Context(), GitKey{Content: key.Content}, Policy{MaxAge: 0}, func(context.Context,

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
			Policy{MaxAge: Forever},
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

type testGitRestartAndInvalidationCaseCompletionInputs struct {
	src        *string
	c          **Cache
	root       *string
	archiveKey *ArchiveKey
	archiveID  *string
	blobKey    *BlobKey
	blobID     *string
	scenario   **string
	id         *string
	e          **entry
	calls      *int
	first      **GitGeneration
	key        *GitKey
	produce    *func(_ context.Context, storer storage.Storer, worktree billy.Filesystem) (plumbing.Hash, error)
	v          *any
	blob       *string
	stale      *time.Time
}
