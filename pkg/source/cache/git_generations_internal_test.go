package cache

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/go-git/go-billy/v6"
	"github.com/go-git/go-git/v6"
	"github.com/go-git/go-git/v6/plumbing"
	"github.com/go-git/go-git/v6/plumbing/object"
	"github.com/go-git/go-git/v6/storage"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/pkg/source/gittree"
)

func TestGit_RestartAndInvalidation(t *testing.T) {
	t.Parallel()
	for _, scenario := range []string{"hit", "ref-mismatch", "missing-objects", "touch-failure", "outcome"} {
		t.Run(scenario, func(t *testing.T) {
			t.Parallel()
			testGitRestartAndInvalidationCase(t, &scenario)
		},
		)
	}

	for _, disk := range []bool{false, true} {
		t.Run(fmt.Sprintf("recursive-held-%t", disk), func(t *testing.T) {
			t.Parallel()
			testGitRestartAndInvalidationCase2(t, &disk)
		},
		)
	}
}

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
	first, id, err := c.Git(t.Context(), key, &Policy{MaxAge: Forever}, produce)
	assert.NoError(t, err)

	archiveKey := ArchiveKey{Content: key.Content}
	var payloadTar bytes.Buffer
	testutil.WriteTar(t, &payloadTar, map[string]string{"file": "archive"}, 0o644)
	_, _, archiveID, err := c.Acquire(
		t.Context(),
		archiveKey,
		&Policy{MaxAge: Forever},
		producer(payloadTar.Bytes(), &ocispec.Descriptor{}),
	)
	assert.NoError(t, err)

	blobKey := BlobKey{Content: key.Content}
	_, _, blobID, err := c.Blob(
		t.Context(),
		blobKey,
		&Policy{MaxAge: Forever},
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
		&Policy{MaxAge: Forever},
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
	r, _, restoredID, err := (*captures.c).Blob(t.Context(), (*captures.blobKey), &Policy{MaxAge: Forever}, nil)
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
	second, _, err := (*captures.c).Git(t.Context(), (*captures.key), &Policy{MaxAge: Forever}, (*captures.produce))
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
				t.Context(), (*captures.key), &Policy{MaxAge: Forever}, (*captures.produce),
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
			_, _, err := (*captures.c).Git(t.Context(), (*captures.key), &Policy{MaxAge: Forever}, (*captures.produce))
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
	first, _, err := c.Git(t.Context(), key, &Policy{MaxAge: 0}, produce)
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
		next, _, err := c.Git(t.Context(), key, &Policy{MaxAge: 0}, produce)
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
		restored, _, err := c.Git(t.Context(), key, &Policy{MaxAge: Forever}, nil)
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
