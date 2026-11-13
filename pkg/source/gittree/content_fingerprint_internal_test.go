package gittree

import (
	"context"
	"errors"
	"io/fs"
	"runtime"
	"strings"
	"sync"
	"testing"
	"testing/fstest"

	"github.com/go-git/go-git/v6/plumbing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
)

func testFSContentFingerprintCase(t *testing.T, mode *string,
) {
	t.Helper()

	var keys [2]string
	for i := range keys {
		testFSContentFingerprintIteration(t, mode, i, &keys)
	}
	testFSContentFingerprintCaseCompletion(t, &testFSContentFingerprintCaseCompletionInputs{mode: &mode, keys: &keys})
}

func testFSContentFingerprintCaseCompletion(t *testing.T, captures *testFSContentFingerprintCaseCompletionInputs) {
	t.Helper()
	if (*(*captures.mode)) != "error" && (*(*captures.mode)) != "read_error" &&
		(*(*captures.mode)) != "walk_error" {
		assert.Equal(
			t,
			captures.keys[0] == captures.keys[1],
			(*(*captures.mode)) != "payload" && (*(*captures.mode)) != "name",
		)
	}
}

func testFSContentFingerprintNativeMembership(t *testing.T) {
	t.Helper()

	files := fstest.MapFS{
		".sourceignore": {Data: []byte("ignored\n")},
		"file":          {Data: []byte("selected")},
		"nested/other":  {Data: []byte("included")},
		"ignored":       {Data: []byte("ignored")},
	}
	reader := &contentReadFS{FS: files}
	input, _, err := New(t.Context(), nil, plumbing.ZeroHash, ViewOptions{Mode: Source, Current: reader})
	assert.NoError(t, err)
	reader.fail = "."
	key, err := ContentFingerprint(t.Context(), input)
	assert.NoError(t, err)
	assert.Equal(t, key != "", true)
	assert.Equal(t, len(input.FileDigests()), 3)
	for _, name := range []string{".sourceignore", "file", "nested/other"} {
		assert.Equal(
			t,
			input.FileDigests()[name],
			manifest.Fingerprint(testutil.GitBlobHash(t, files[name].Data)),
		)
	}
}

func testFSContentFingerprintFreshAdmission(t *testing.T) {
	t.Helper()

	root := t.TempDir()
	testutil.WriteFile(t, root, "file", "before")
	first, err := admitLocal(t, root)
	assert.NoError(t, err)
	before, err := first.ContentFingerprint(t.Context(), first.FS)
	assert.NoError(t, err)
	testutil.WriteFile(t, root, "file", "after")
	second, err := admitLocal(t, root)
	assert.NoError(t, err)
	operation := first.FS.(*FS).OperationView()
	assert.Equal(t, operation.FileDigests() == nil, true)
	operationKey, err := ContentFingerprint(t.Context(), operation)
	assert.NoError(t, err)
	assert.Equal(t, operationKey != before, true)

	assert.Equal(t, second.FS.(*FS).FileDigests() == nil, true)
	after, err := second.ContentFingerprint(t.Context(), second.FS)
	assert.NoError(t, err)
	assert.Equal(t, first.Key, "")
	assert.Equal(t, second.Key, "")
	assert.Equal(t, before != after, true)
	assert.Equal(
		t,
		second.FS.(*FS).FileDigests()["file"],
		manifest.Fingerprint(testutil.GitBlobHash(t, []byte("after"))),
	)
	retained, err := first.ContentFingerprint(t.Context(), first.FS)
	assert.NoError(t, err)
	assert.Equal(t, retained, before)
}

func testFSContentFingerprintExcludedPolicyFile(t *testing.T) {
	t.Helper()

	input, _, err := New(t.Context(), nil, plumbing.ZeroHash, ViewOptions{Mode: Source, Current: fstest.MapFS{
		".sourceignore":     {Data: []byte("*.png\nsub/.sourceignore\n")},
		"sub/.sourceignore": {Data: []byte("!*.png\n")},
		"sub/keep.png":      {Data: []byte("selected")},
		"file":              {Data: []byte("retained")},
	}})
	assert.NoError(t, err)
	_, err = fs.Stat(input, "sub/keep.png")
	assert.NoError(t, err)
	_, err = ContentFingerprint(t.Context(), input)
	assert.NoError(t, err)
	_, included := input.FileDigests()["sub/keep.png"]
	assert.Equal(t, included, false)
	assert.Equal(t, input.FileDigests()["file"] != "", true)
}

func testFSContentFingerprintContentCanceled(t *testing.T) {
	t.Helper()

	reader := &contentReadFS{FS: fstest.MapFS{"file": {Data: []byte("first")}, "later": {Data: []byte("later")}}}
	input, _, err := New(t.Context(), nil, plumbing.ZeroHash, ViewOptions{Mode: Direct, Current: reader})
	assert.NoError(t, err)
	_, err = input.SourcePolicy(t.Context())
	assert.NoError(t, err)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	reader.readCancel = cancel
	_, err = ContentFingerprint(ctx, input)
	assert.Equal(t, errors.Is(err, context.Canceled), true)
	assert.Equal(t, input.FileDigests() == nil, true)
	reader.readCancel = nil
	failedReads := reader.reads.Load()
	var keys [2]string
	var wg sync.WaitGroup
	for i := range keys {
		wg.Go(func() {
			key, contentFingerprintErr4 := ContentFingerprint(t.Context(), input)
			assert.NoError(t, contentFingerprintErr4)
			assert.Equal(t, key != "", true)
			keys[i] = key
		})
	}
	wg.Wait()
	assert.Equal(t, keys[0], keys[1])
	assert.Equal(t, reader.reads.Load(), failedReads+1)
	for name, data := range map[string]string{"file": "first", "later": "later"} {
		assert.Equal(
			t,
			input.FileDigests()[name],
			manifest.Fingerprint(testutil.GitBlobHash(t, []byte(data))),
		)
	}
	reads := reader.reads.Load()
	key, err := ContentFingerprint(t.Context(), input)
	assert.NoError(t, err)
	assert.Equal(t, key, keys[0])
	assert.Equal(t, reader.reads.Load(), reads)
}

type testFSContentFingerprintCaseCompletionInputs struct {
	mode **string
	keys *[2]string
}

func testFSContentFingerprintIteration(t *testing.T, mode *string, i int, keys *[2]string) {
	t.Helper()
	files := fstest.MapFS{
		"file":          {Data: []byte("selected")},
		".sourceignore": {Data: []byte("ignored\n")},
		"ignored":       {Data: []byte("ignored")},
	}
	if i == 1 {
		switch *mode {
		case "payload":
			files["file"].Data = []byte("different")
		case "name":
			files["other"] = files["file"]
			delete(files, "file")
		case "ignored":
			files["ignored"].Data = []byte("different")
		}
	}
	reader := &contentReadFS{FS: files}
	input, key, err := New(t.Context(), nil, plumbing.ZeroHash, ViewOptions{Mode: Direct, Current: reader})
	assert.NoError(t, err)
	assert.Equal(t, key, "")
	assert.Equal(t, reader.reads.Load(), int64(0))
	assert.Equal(t, input.FileDigests() == nil, true)
	ctx, cancel := context.WithCancel(t.Context())
	if testFSContentFingerprintErrors(
		ctx,
		t,
		mode,
		&fingerprintErrorCase{input: input, reader: reader, cancel: cancel},
	) {
		return
	}
	cancel()
	if (*mode) == "error" {
		input.view = &directView{FS: admissionErrorFS{err: fs.ErrPermission}}
		_, contentFingerprintErr2 := ContentFingerprint(t.Context(), input)
		assert.Equal(t, input.FileDigests() == nil, true)
		assert.FatalIf(
			t,
			!errors.Is(contentFingerprintErr2, fs.ErrPermission),
			"fingerprint error = %v",
			contentFingerprintErr2,
		)
		return
	}
	var wg sync.WaitGroup
	for range 2 {
		wg.Go(func() {
			fingerprint, contentFingerprintErr3 := ContentFingerprint(t.Context(), input)
			assert.NoError(t, contentFingerprintErr3)
			if fingerprint == "" {
				t.Error("mutable fingerprint is unknown")
			}
		})
	}
	wg.Wait()
	reads := reader.reads.Load()
	if (*mode) != "name" || i == 0 {
		assert.Equal(t, reads, int64(1))
	}
	for name, file := range files {
		if name != "ignored" {
			assert.Equal(
				t,
				input.FileDigests()[name],
				manifest.Fingerprint(testutil.GitBlobHash(t, file.Data)),
			)
		}
	}
	_, ignored := input.FileDigests()["ignored"]
	assert.Equal(t, ignored, false)
	keys[i], err = ContentFingerprint(t.Context(), input)
	assert.NoError(t, err)
	assert.Equal(t, reader.reads.Load(), reads)
	if (*mode) == "canceled" {
		_, err := ContentFingerprint(ctx, input)
		assert.FatalIf(t, !errors.Is(err, context.Canceled), "cached fingerprint error = %v", err)
	}
}

type fingerprintErrorCase struct {
	input  *FS
	reader *contentReadFS
	cancel context.CancelFunc
}

func testFSContentFingerprintErrors(
	ctx context.Context,
	t *testing.T,
	mode *string,
	sample *fingerprintErrorCase,
) bool {
	t.Helper()
	input, reader, cancel := sample.input, sample.reader, sample.cancel
	if (*mode) == "read_error" || (*mode) == "walk_error" ||
		(*mode) == "scan_canceled" ||
		(*mode) == "scan_deadline" {
		_, sourcePolicyErr := input.SourcePolicy(t.Context())
		assert.NoError(t, sourcePolicyErr)
		want := fs.ErrPermission
		reader.fail = "file"
		if (*mode) == "walk_error" {
			reader.fail = "."
		}
		if (*mode) == "scan_canceled" {
			reader.fail, reader.cancel, want = "", cancel, context.Canceled
		}
		if (*mode) == "scan_deadline" {
			reader.fail, reader.err, want = ".", context.DeadlineExceeded, context.DeadlineExceeded
		}
		_, sourcePolicyErr = ContentFingerprint(ctx, input)
		cancel()
		assert.Equal(t, input.FileDigests() == nil, true)
		assert.FatalIf(
			t,
			!errors.Is(sourcePolicyErr, want),
			"fingerprint error = %v, want %v",
			sourcePolicyErr,
			want,
		)
		reader.fail, reader.cancel, reader.err = "", nil, nil
		if (*mode) == "read_error" || (*mode) == "walk_error" {
			_, sourcePolicyErr = ContentFingerprint(t.Context(), input)
			assert.Equal(t, errors.Is(sourcePolicyErr, want), true)
			assert.Equal(t, input.FileDigests() == nil, true)
			return true
		}
	}
	if (*mode) == "canceled" {
		cancel()
		_, contentFingerprintErr := ContentFingerprint(ctx, input)
		assert.FatalIf(
			t,
			!errors.Is(contentFingerprintErr, context.Canceled),
			"fingerprint error = %v",
			contentFingerprintErr,
		)
	}
	return false
}

func testFSContentFingerprintCase2(t *testing.T, name *string,
) {
	t.Helper()

	reader := &contentReadFS{
		FS:      fstest.MapFS{"file": {Data: []byte("first")}, "later": {Data: []byte("later")}},
		entered: make(chan struct{}),
		release: make(chan struct{}),
	}
	input, _, err := New(t.Context(), nil, plumbing.ZeroHash, ViewOptions{Mode: Direct, Current: reader})
	assert.NoError(t, err)
	_, err = input.SourcePolicy(t.Context())
	assert.NoError(t, err)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	joinerCtx, cancelJoiner := context.WithCancel(t.Context())
	defer cancelJoiner()
	joiner := &fingerprintJoinerContext{Context: joinerCtx, input: input}
	first := make(chan error, 1)
	var firstKey string
	go func() {
		var innerErr error
		firstKey, innerErr = ContentFingerprint(ctx, input)
		first <- innerErr
	}()
	<-reader.entered
	assert.Equal(t, input.FileDigests() == nil, true)
	second := make(chan struct {
		key string
		err error
	}, 1)
	waiterID := make(chan string, 1)
	go func() {
		buf := make([]byte, 64)
		n := runtime.Stack(buf, false)
		_, header, _ := strings.Cut(string(buf[:n]), " ")
		id, _, _ := strings.Cut(header, " ")
		waiterID <- id
		key, contentFingerprintErr5 := ContentFingerprint(joiner, input)
		second <- struct {
			key string
			err error
		}{key, contentFingerprintErr5}
	}()
	waitForOnceWaiter(t, <-waiterID)
	if (*name) != "canceled_joiner_live_executor" {
		cancel()
	}
	if (*name) != "live_joiner_retries" {
		cancelJoiner()
	}
	close(reader.release)
	if (*name) == "canceled_joiner_live_executor" {
		assert.NoError(t, <-first)
		assert.Equal(t, firstKey != "", true)
		result := <-second
		assert.Equal(t, errors.Is(result.err, context.Canceled), true)
		assert.Equal(t, result.key, "")
		assert.Equal(t, len(input.FileDigests()), 2)
		reads := reader.reads.Load()
		key, contentFingerprintErr6 := ContentFingerprint(t.Context(), input)
		assert.NoError(t, contentFingerprintErr6)
		assert.Equal(t, key, firstKey)
		assert.Equal(t, reader.reads.Load(), reads)
		return
	}
	assert.Equal(t, errors.Is(<-first, context.Canceled), true)
	result := <-second
	assert.Equal(t, joiner.recomputed.Load(), false)
	if (*name) == "canceled_joiner" {
		assert.Equal(t, errors.Is(result.err, context.Canceled), true)
		assert.Equal(t, result.key, "")
		assert.Equal(t, input.FileDigests() == nil, true)
		assert.Equal(t, reader.reads.Load(), int64(1))
		return
	}
	assert.NoError(t, result.err)
	assert.Equal(t, result.key != "", true)
	assert.Equal(t, len(input.FileDigests()), 2)
	for name, data := range map[string]string{"file": "first", "later": "later"} {
		assert.Equal(
			t,
			input.FileDigests()[name],
			manifest.Fingerprint(testutil.GitBlobHash(t, []byte(data))),
		)
	}
	assert.Equal(t, reader.reads.Load(), int64(2))
	key, err := ContentFingerprint(t.Context(), input)
	assert.NoError(t, err)
	assert.Equal(t, key, result.key)
	assert.Equal(t, reader.reads.Load(), int64(2))
}
