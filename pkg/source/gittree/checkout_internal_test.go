package gittree

import (
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"testing"

	"github.com/go-git/go-git/v6/plumbing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
)

func TestCheckout_AdmittedMembership(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	testutil.WriteFile(t, root, filepath.Join("kept", "file"), "hidden")
	testutil.WriteFile(t, root, filepath.Join("other", "file"), "outside sparse")
	testutil.WriteFile(t, root, ".sourceignore", "kept/file\n")
	testutil.WriteFile(t, root, filepath.Join(".git", "excluded"), "metadata")
	assert.NoError(t, os.Mkdir(filepath.Join(root, "kept", "empty"), 0o750))
	assert.NoError(t, os.Chmod(filepath.Join(root, "kept", "file"), 0o750))
	assert.NoError(t, os.Symlink("empty", filepath.Join(root, "kept", "link")))
	admitted, err := admitLocal(t, root)
	assert.NoError(t, err)
	checkout, _, err := New(
		t.Context(),
		nil,
		plumbing.ZeroHash,
		ViewOptions{Mode: Checkout, Current: admitted.FS, Sparse: []string{"kept"}, Ignore: "kept/\n"},
	)
	assert.NoError(t, err)
	data, err := fs.ReadFile(checkout, "kept/file")
	assert.NoError(t, err)
	assert.Equal(t, string(data), "hidden")
	info, err := fs.Stat(checkout, "kept/file")
	assert.NoError(t, err)
	assert.Equal(t, info.Mode().Perm(), fs.FileMode(0o750))
	info, err = fs.Stat(checkout, "kept/empty")
	assert.NoError(t, err)
	assert.Equal(t, info.IsDir(), true)
	assert.Equal(t, info.Mode().Perm(), fs.FileMode(0o750))
	target, err := fs.ReadLink(checkout, "kept/link")
	assert.NoError(t, err)
	assert.Equal(t, target, "empty")
	_, err = fs.Stat(checkout, "other/file")
	assert.Equal(t, errors.Is(err, fs.ErrNotExist), true)
	_, err = fs.Stat(checkout, ".git/excluded")
	assert.Equal(t, errors.Is(err, fs.ErrNotExist), true)
	source, _, err := New(t.Context(), nil, plumbing.ZeroHash, ViewOptions{Mode: Source, Current: admitted.FS})
	assert.NoError(t, err)
	_, err = fs.Stat(source, "kept/empty")
	assert.NoError(t, err)
	_, err = fs.Stat(source, "kept/file")
	assert.Equal(t, errors.Is(err, fs.ErrNotExist), true)
	_, err = fs.Lstat(source, "kept/link")
	assert.Equal(t, errors.Is(err, fs.ErrPermission), true)
}
