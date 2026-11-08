package gittree

import (
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
)

func testNewEntryPolicyConcurrentFirstListing(t *testing.T) {
	t.Helper()

	root := t.TempDir()
	testutil.WriteFile(t, root, "dir/a", "a")
	testutil.WriteFile(t, root, "dir/child", "content")
	testutil.WriteFile(t, root, "dir/nested/file", "nested")
	assert.NoError(t, os.Symlink("../../absent", filepath.Join(root, "dir", "link")))
	want, err := os.Lstat(filepath.Join(root, "dir", "child"))
	assert.NoError(t, err)
	artifact, err := admitLocal(t, root)
	assert.NoError(t, err)
	view, _, err := operation(t, artifact.FS, Direct)
	assert.NoError(t, err)
	start := make(chan struct{})
	var ready, wg sync.WaitGroup
	ready.Add(8)
	for range 8 {
		wg.Go(func() {
			testNewEntryPolicyConcurrentFirstListingCallback(
				testNewEntryPolicyConcurrentFirstListingCallbackInputs{
					t:     &t,
					want:  &want,
					view:  &view,
					start: &start,
					ready: &ready,
				},
			)
		},
		)
	}
	ready.Wait()
	close(start)
	wg.Wait()
}

func testNewEntryPolicyRetainedDirectoryListing(t *testing.T) {
	t.Helper()

	root := t.TempDir()
	testutil.WriteFile(t, root, "dir/kept", "kept")
	artifact, err := admitLocal(t, root)
	assert.NoError(t, err)
	observed, _, err := operation(t, artifact.FS, Direct)
	assert.NoError(t, err)
	_, err = fs.ReadDir(observed, "dir")
	assert.NoError(t, err)
	testutil.WriteFile(t, root, "dir/arrived", "arrived")
	_, err = fs.Stat(observed, "dir/arrived")
	assert.Equal(t, errors.Is(err, fs.ErrNotExist), true)
	fresh, pin, err := operation(t, observed, Direct)
	assert.NoError(t, err)
	assert.Equal(t, pin, "")
	data, err := fs.ReadFile(fresh, "dir/arrived")
	assert.NoError(t, err)
	assert.Equal(t, string(data), "arrived")
}

type testNewEntryPolicyConcurrentFirstListingCallbackInputs struct {
	t     **testing.T
	want  *os.FileInfo
	view  **FS
	start *chan struct{}
	ready *sync.WaitGroup
}

func testNewEntryPolicyConcurrentFirstListingCallback(
	testCaseInput testNewEntryPolicyConcurrentFirstListingCallbackInputs,
) {
	testCaseInput.ready.
		Done()
	<-(*testCaseInput.start)
	entries, err := (*testCaseInput.view).ReadDir("dir")
	if err != nil {
		(*testCaseInput.t).
			Error(err)
		return
	}
	if len(entries) != 3 {
		(*testCaseInput.t).
			Errorf("listing length = %d, want 3", len(entries))
		return
	}
	entries[1] = entries[0]
	entries, err = (*testCaseInput.view).ReadDir("dir")
	if err != nil {
		(*testCaseInput.t).
			Error(err)
		return
	}
	var names []string
	for _, entry := range entries {
		names = append(names, entry.Name())
	}
	assert.Diff((*testCaseInput.t), names, []string{"a", "child", "nested"})
	info, err := fs.Stat((*testCaseInput.view), "dir/child")
	if err != nil {
		(*testCaseInput.t).
			Error(err)
		return
	}
	assert.Equal((*testCaseInput.t), info.Name(), (*testCaseInput.want).Name())
	assert.Equal((*testCaseInput.t), info.Mode(), (*testCaseInput.want).Mode())
	assert.Equal((*testCaseInput.t), info.Size(), (*testCaseInput.want).Size())
	assert.Equal((*testCaseInput.t), info.ModTime().Equal((*testCaseInput.want).ModTime()), true)
	_, err = fs.ReadFile((*testCaseInput.view), "dir/link/child")
	assert.Equal((*testCaseInput.t), errors.Is(err, fs.ErrPermission), true)
}
