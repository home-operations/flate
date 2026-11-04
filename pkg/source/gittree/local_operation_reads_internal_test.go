package gittree

import (
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
)

type testNewEntryPolicyLocalReadsCaseInputs struct {
	reader *struct {
		name string
		fs   fs.FS
	}
}

func testNewEntryPolicyLocalReadsCase(t *testing.T, testCaseInput testNewEntryPolicyLocalReadsCaseInputs) {
	t.Helper()

	entries, err := fs.ReadDir(testCaseInput.reader.fs, ".")
	assert.FatalIf(t, err != nil || len(entries) != 2 || entries[0].Name() != "nested" ||
		entries[1].Name() != "regular.txt", "admitted entries = %v, %v", entries, err)
	for _, name := range []string{"regular.txt", "file-link", "dir-link/regular.txt", "dangling", "escape", "escape/child", "broken-escape", "broken-escape/child", "socket", "absent"} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			var want error
			content := "content"
			if name != "regular.txt" {
				want, content = fs.ErrPermission, ""
				if name == "socket" || name == "absent" {
					want = fs.ErrNotExist
				}
			}
			data, err := fs.ReadFile(testCaseInput.reader.fs, name)
			assert.FatalIf(
				t,
				!errors.Is(err, want) || string(data) != content,
				"read = %q, %v; want %q, %v",
				data,
				err,
				content,
				want,
			)
			{
				_, err := fs.Stat(testCaseInput.reader.fs, name)
				assert.FatalIf(t, !errors.Is(err, want), "stat = %v; want %v", err, want)
			}
		})
	}
}

func testNewEntryPolicyLocalOperationObservationsIgnoreSnapshot(t *testing.T) {
	t.Helper()

	root := t.TempDir()
	testutil.WriteFile(t, root, ".sourceignore", "root\n")
	testutil.WriteFile(t, root, "nested/.sourceignore", "first\n")
	testutil.WriteFile(t, root, ".git/.sourceignore", "excluded\n")
	artifact, err := admitLocal(t, root)
	assert.NoError(t, err)
	input := artifact.FS
	shaped, pin, err := operation(t, input, Source)
	assert.NoError(t, err)
	assert.Equal(t, pin, "")
	content := shaped.PolicyEvidence()
	want := []struct {
		Path string
		Data []byte
	}{{".sourceignore", []byte("root\n")}, {"nested/.sourceignore", []byte("first\n")}}
	assert.Diff(t, content, any(want))
	testutil.WriteFile(t, root, "nested/.sourceignore", "other\n")
	shaped, pin, err = operation(t, input, Source)
	assert.Equal(t, pin, "")
	retained := shaped.PolicyEvidence()
	assert.NoError(t, err)
	assert.Diff(t, retained, content)
	artifact, err = admitLocal(t, root)
	assert.NoError(t, err)
	input = artifact.FS
	shaped, pin, err = operation(t, input, Source)
	assert.Equal(t, pin, "")
	refreshed := shaped.PolicyEvidence()
	assert.NoError(t, err)
	want[1].Data = []byte("other\n")
	assert.Diff(t, refreshed, any(want))
	assert.NoError(t, os.Rename(filepath.Join(root, "nested"), filepath.Join(root, "renamed")))
	artifact, err = admitLocal(t, root)
	assert.NoError(t, err)
	input = artifact.FS
	shaped, pin, err = operation(t, input, Source)
	assert.Equal(t, pin, "")
	moved := shaped.PolicyEvidence()
	assert.NoError(t, err)
	want[1].Path = "renamed/.sourceignore"
	assert.Diff(t, moved, any(want))
	unfiltered, pin, err := operation(t, input, Direct)
	assert.Equal(t, pin, "")
	assert.NoError(t, err)
	assert.FatalIf(t, unfiltered.PolicyEvidence() != nil, "unfiltered policy = %v", unfiltered)
}

func testNewEntryPolicyLocalOperationObservationsPointLookupWithoutListing(t *testing.T) {
	t.Helper()

	root := t.TempDir()
	dir := filepath.Join(root, "siblings")
	for _, name := range []string{"a", "b"} {
		testutil.WriteFile(t, dir, name, name)
	}
	assert.Equal(t, os.Chmod(dir, 0o111), nil)
	t.Cleanup(func() { assert.Equal(t, os.Chmod(dir, 0o755), nil) })
	if _, err := os.ReadDir(dir); err == nil {
		t.Skip("filesystem permits listing a search-only directory")
	} else if !errors.Is(err, fs.ErrPermission) {
		t.Fatal(err)
	}
	artifact, err := admitLocal(t, root)
	assert.Equal(t, err, nil)
	input := artifact.FS
	for _, name := range []string{"a", "b"} {
		file := "siblings/" + name
		data, readErr := fs.ReadFile(input, file)
		_, statErr := fs.Stat(input, file)
		assert.FatalIf(
			t,
			readErr != nil || statErr != nil || string(data) != name,
			"point lookup %s = %q, %v, %v",
			file,
			data,
			readErr,
			statErr,
		)
	}
}

type testNewEntryPolicyLocalOperationObservationsCaseInputs struct {
	tc *struct {
		name string
		path string

		content string
		view    fs.FS
		want    error
	}
}

func testNewEntryPolicyLocalOperationObservationsCase(
	t *testing.T,
	testCaseInput testNewEntryPolicyLocalOperationObservationsCaseInputs,
) {
	t.Helper()
	data, err := fs.ReadFile(testCaseInput.tc.view, testCaseInput.tc.path)
	if !errors.Is(err, testCaseInput.tc.want) || string(data) != testCaseInput.tc.content {
		t.Fatalf("view read = %q, %v; want %q, %v", data, err, testCaseInput.tc.content, testCaseInput.tc.want)
	}
}
