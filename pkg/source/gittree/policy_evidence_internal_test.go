package gittree

import (
	"context"
	"errors"
	"io/fs"
	"sync"
	"testing"

	"github.com/go-git/go-git/v5/plumbing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
)

func testNewEntryPolicyPolicyReadOnceAndExtras(t *testing.T) {
	t.Helper()

	root := t.TempDir()
	testutil.WriteFile(t, root, ".sourceignore", "discard\n")
	testutil.WriteFile(t, root, "nested/.sourceignore", "hidden\n")
	for _, name := range []string{"discard", "kept", "image.png", "nested/hidden"} {
		testutil.WriteFile(t, root, name, name)
	}
	artifact, err := admitLocal(t, root)
	assert.NoError(t, err)
	input := artifact.FS.(*FS)
	counted := &policyReadFS{FS: input}
	policy, err := input.snapshot.load(counted)
	assert.NoError(t, err)
	reads := counted.reads.Load()
	assert.Equal(t, reads, int64(2))
	assert.Equal(t, policy.Match("discard", false), true)
	assert.Equal(t, policy.Match("nested/hidden", false), true)
	want := []struct{ Path, Data string }{{".sourceignore", "discard\n"}, {"nested/.sourceignore", "hidden\n"}}
	assert.Diff(t, policy.Evidence(), want)
	content := []struct {
		Path string
		Data []byte
	}{{".sourceignore", []byte("discard\n")}, {"nested/.sourceignore", []byte("hidden\n")}}
	testutil.WriteFile(t, root, ".sourceignore", "kept\n")
	shaped, pin, err := New(
		t.Context(),
		nil,
		plumbing.ZeroHash,
		ViewOptions{Mode: Source, Current: input, Ignore: "kept\n"},
	)
	assert.NoError(t, err)
	assert.Equal(t, pin, "")
	assert.Diff(t, shaped.PolicyEvidence(), any(content))
	_, err = fs.Stat(shaped, "image.png")
	assert.NoError(t, err)
	for _, name := range []string{"discard", "kept", "nested/hidden"} {
		_, err = fs.Stat(shaped, name)
		assert.Equal(t, errors.Is(err, fs.ErrNotExist), true)
	}
	testutil.WriteFile(t, root, ".sourceignore", "image.png\n")
	second, pin, err := New(
		t.Context(),
		nil,
		plumbing.ZeroHash,
		ViewOptions{Mode: Source, Current: input, Ignore: "image.png\n"},
	)
	assert.NoError(t, err)
	assert.Equal(t, pin, "")
	assert.Diff(t, second.PolicyEvidence(), any(content))
	assert.Diff(t, shaped.PolicyEvidence(), any(content))
	assert.Diff(t, policy.Evidence(), want)
	_, err = fs.Stat(second, "kept")
	assert.NoError(t, err)
	for _, name := range []string{"image.png", "discard", "nested/hidden"} {
		_, err = fs.Stat(second, name)
		assert.Equal(t, errors.Is(err, fs.ErrNotExist), true)
	}
	assert.Equal(t, policy.Match("kept", false), false)
	assert.Equal(t, policy.Match("image.png", false), false)
	_, err = fs.Stat(shaped, "kept")
	assert.Equal(t, errors.Is(err, fs.ErrNotExist), true)
	_, err = fs.Stat(shaped, "image.png")
	assert.NoError(t, err)
	retained, pin, err := operation(t, shaped, Source)
	assert.NoError(t, err)
	assert.Equal(t, pin, "")
	assert.Diff(t, retained.PolicyEvidence(), any(content))
	_, err = fs.Stat(retained, "kept")
	assert.Equal(t, errors.Is(err, fs.ErrNotExist), true)
	_, err = input.snapshot.load(counted)
	assert.NoError(t, err)
	assert.Equal(t, counted.reads.Load(), reads)
}

func testNewEntryPolicyPolicyEvidencePreservesBytes(t *testing.T) {
	t.Helper()

	root := t.TempDir()
	keys := make([]string, 0, 2)
	for _, data := range []string{"#\xff\n", "#\xfe\n"} {
		testutil.WriteFile(t, root, ".sourceignore", data)
		artifact, err := admitLocal(t, root)
		assert.NoError(t, err)
		input, pin, err := operation(t, artifact.FS, Source)
		assert.NoError(t, err)
		assert.Equal(t, pin, "")
		assert.Diff(t, input.PolicyEvidence(), any([]struct {
			Path string
			Data []byte
		}{{".sourceignore", []byte(data)}}))
		keys = append(keys, manifest.Fingerprint(input.PolicyEvidence()))
	}
	assert.Equal(t, keys[0] != keys[1], true)
}

func testNewEntryPolicyConcurrentFirstPolicyAndCancellation(t *testing.T) {
	t.Helper()

	root := t.TempDir()
	testutil.WriteFile(t, root, ".sourceignore", "discard\n")
	testutil.WriteFile(t, root, "discard", "discard")
	testutil.WriteFile(t, root, "kept", "kept")
	artifact, err := admitLocal(t, root)
	assert.NoError(t, err)
	start := make(chan struct{})
	var wg sync.WaitGroup
	for range 2 {
		wg.Go(func() {
			<-start
			input, pin, operationErr2 := operation(t, artifact.FS, Source)
			if operationErr2 != nil {
				t.Error(operationErr2)
				return
			}
			assert.Equal(t, pin, "")
			assert.Diff(t, input.PolicyEvidence(), any([]struct {
				Path string
				Data []byte
			}{{".sourceignore", []byte("discard\n")}}))
			_, operationErr2 = fs.Stat(input, "discard")
			assert.Equal(t, errors.Is(operationErr2, fs.ErrNotExist), true)
			data, operationErr2 := fs.ReadFile(input, "kept")
			assert.NoError(t, operationErr2)
			assert.Equal(t, string(data), "kept")
		})
	}
	close(start)
	wg.Wait()
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	_, err = AdmitDirectory(ctx, artifact.FS.(*FS).Location())
	assert.Equal(t, err, context.Canceled)
	for _, mode := range []ViewMode{Direct, Source} {
		_, _, err = New(ctx, nil, plumbing.ZeroHash, ViewOptions{Mode: mode, Current: artifact.FS})
		assert.Equal(t, err, context.Canceled)
	}
}
