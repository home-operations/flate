package loader

import (
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/assert"
)

type filesystemResultFS struct {
	fs.FS

	info   fs.FileInfo
	target string
	err    error
}

func (f filesystemResultFS) Lstat(string) (fs.FileInfo, error) { return f.info, f.err }

func (f filesystemResultFS) ReadLink(string) (string, error) { return f.target, f.err }

func TestDocumentReader_FilesystemErrorContext(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	testutil.WriteFile(t, root, "file", "content")
	assert.NoError(t, os.Symlink("file", filepath.Join(root, "link")))
	info, err := os.Lstat(filepath.Join(root, "link"))
	assert.NoError(t, err)
	for _, method := range []string{"lstat", "readlink"} {
		for _, failure := range []struct {
			name string
			err  error
		}{
			{name: "success"},
			{name: "missing", err: fs.ErrNotExist},
			{name: "permission", err: fs.ErrPermission},
		} {
			t.Run(method+"/"+failure.name, func(t *testing.T) {
				t.Parallel()
				var original *fs.PathError
				input := filesystemResultFS{FS: os.DirFS(root), info: info, target: "file"}
				if failure.err != nil {
					original = &fs.PathError{Op: method, Path: "link", Err: failure.err}
					input.err = original
				}
				reader := documentReader{views: map[string]fs.FS{root: input}}
				var gotErr error
				if method == "lstat" {
					got, err := reader.Lstat(filepath.Join(root, "link"))
					gotErr = err
					assert.Equal(t, got, info)
				} else {
					got, err := reader.Readlink(filepath.Join(root, "link"))
					gotErr = err
					assert.Equal(t, got, "file")
				}
				if original == nil {
					assert.NoError(t, gotErr)
					return
				}
				assert.FatalIf(t, gotErr == nil, "%s", "filesystem failure was lost")
				assert.Equal(t, strings.HasPrefix(gotErr.Error(), "document "+method+": "), true)
				assert.Equal(t, errors.Is(gotErr, failure.err), true)
				pathErr, ok := errors.AsType[*fs.PathError](gotErr)
				assert.Equal(t, ok, true)
				assert.Equal(t, pathErr, original)
			})
		}
	}
}
