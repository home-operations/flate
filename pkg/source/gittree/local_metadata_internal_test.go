package gittree

import (
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
)

func testNewEntryPolicyLocalMetadata(t *testing.T) {
	t.Helper()

	for _, directory := range []bool{false, true} {
		t.Run(strconv.FormatBool(directory), func(t *testing.T) {
			t.Parallel()
			testNewEntryPolicyLocalMetadataCase(t, testNewEntryPolicyLocalMetadataCaseInputs{directory: &directory})
		},
		)
	}
}

type testNewEntryPolicyLocalMetadataCaseInputs struct {
	directory *bool
}

func testNewEntryPolicyLocalMetadataCase(t *testing.T, testCaseInput testNewEntryPolicyLocalMetadataCaseInputs) {
	t.Helper()

	root := t.TempDir()
	testutil.WriteFile(t, root, "file", "content")
	artifact, err := admitLocal(t, root)
	assert.NoError(t, err)
	for _, prefix := range []string{"", "nested/"} {
		name := prefix + ".git"
		if *testCaseInput.directory {
			name += "/config"
		}
		testutil.WriteFile(t, root, name, "metadata")
	}
	input := artifact.FS.(*FS)
	location := input.Location()
	assert.Equal(t, location.ContentRoot, root)
	for _, mode := range []ViewMode{Direct, Source, RemoteBase} {
		view, pin, operationErr := operation(t, input, mode)
		assert.NoError(t, operationErr)
		assert.Equal(t, pin, "")
		assert.Equal(t, view.Location() == location, true)
		for _, reader := range []fs.FS{input, view} {
			for _, name := range []string{".git", ".git/config", "nested/.git", "nested/.git/config"} {
				_, statErr2 := fs.Stat(reader, name)
				assert.Equal(t, errors.Is(statErr2, fs.ErrNotExist), true)
				_, statErr2 = fs.ReadFile(reader, name)
				assert.Equal(t, errors.Is(statErr2, fs.ErrNotExist), true)
				_, statErr2 = reader.Open(name)
				assert.Equal(t, errors.Is(statErr2, fs.ErrNotExist), true)
				_, statErr2 = fs.ReadDir(reader, name)
				assert.Equal(t, errors.Is(statErr2, fs.ErrNotExist), true)
			}
			assert.NoError(
				t,
				fs.WalkDir(reader, ".", func(name string, entry fs.DirEntry, err error) error {
					assert.NoError(t, err)
					assert.ErrorIf(t, entry.Name() == ".git", "metadata survived traversal: %s", name)
					return nil
				}),
			)
		}
	}
	assert.NoError(t, os.RemoveAll(filepath.Join(root, ".git")))
	testutil.WriteFile(t, root, ".git", "changed metadata")
	fresh, _, err := operation(t, input, Direct)
	assert.NoError(t, err)
	assert.Equal(t, fresh.Location() == location, true)
}
