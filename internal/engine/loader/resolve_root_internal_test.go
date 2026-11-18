package loader

import (
	"context"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/home-operations/flate/internal/engine/store"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/assert"
	"github.com/home-operations/flate/pkg/manifest"
)

func TestResolveRoot_ConfinedCapturedSource(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name, path, want string
		available        bool
	}{
		{name: "directory", path: "inputs", want: "inputs", available: true},
		{name: "source root", path: ".", want: ".", available: true},
		{name: "confined symlink", path: "link", want: "inputs", available: true},
		{name: "escaped root", path: "../outside"},
		{name: "escaped symlink", path: "escape"},
		{name: "absolute root", path: "/inputs"},
		{name: "remote root", path: "https://fixture.invalid/inputs"},
		{name: "missing root", path: "absent"},
		{name: "missing ancestor", path: "absent/../inputs"},
		{name: "file root", path: "inputs/file"},
		{name: "unavailable source", path: "inputs"},
		{name: "replaced source", path: "inputs"},
		{name: "canceled", path: "inputs"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			base := t.TempDir()
			root := filepath.Join(base, "source")
			testutil.WriteFile(t, root, "inputs/file", "captured")
			testutil.WriteFile(t, base, "outside/file", "excluded")
			assert.NoError(t, os.Symlink("inputs", filepath.Join(root, "link")))
			assert.NoError(t, os.Symlink("../outside", filepath.Join(root, "escape")))
			st := store.New()
			source := (&manifest.GitRepository{Name: "source", Namespace: "apps"}).Named()
			input := &store.SourceArtifact{LocalPath: root, Content: "captured", FS: os.DirFS(root)}
			if tc.name != "unavailable source" {
				st.SetArtifact(source, input)
			}
			var reads store.ReadSet
			reads.Init(st, manifest.NamedResource{}, nil)
			defer reads.Retire()
			reads.GetArtifact(source, store.ArtifactContent|store.ArtifactRevision)
			if tc.name == "replaced source" {
				replacement := *input
				replacement.Content = "replacement"
				st.SetArtifact(source, &replacement)
				assert.Equal(t, reads.Valid(), false)
			}
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			if tc.name == "canceled" {
				cancel()
			}
			resolved, available, err := New(st).ResolveRoot(ctx, &reads, source, tc.path)
			if tc.name == "canceled" {
				assert.Equal(t, errors.Is(err, context.Canceled), true)
			} else {
				assert.NoError(t, err)
			}
			assert.Equal(t, available, tc.available)
			if tc.available {
				assert.Equal(t, resolved, tc.want)
			}
			assert.Equal(
				t,
				st.GetArtifact(source, store.ArtifactContent) == input,
				tc.name != "unavailable source" && tc.name != "replaced source",
			)
		})
	}
}

func TestSourcePathView_FilesystemErrorContext(t *testing.T) {
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
				view := sourcePathView{FS: input, valid: true}
				var gotErr error
				if method == "lstat" {
					got, err := view.Lstat("/link")
					gotErr = err
					assert.Equal(t, got, info)
				} else {
					got, err := view.Readlink("/link")
					gotErr = err
					assert.Equal(t, got, "file")
				}
				missing := method == "lstat" && errors.Is(failure.err, fs.ErrNotExist)
				assert.Equal(t, view.missing, missing)
				assert.Equal(t, view.valid, failure.err == nil || missing)
				if original == nil {
					assert.NoError(t, gotErr)
					return
				}
				assert.FatalIf(t, gotErr == nil, "%s", "filesystem failure was lost")
				assert.Equal(t, strings.HasPrefix(gotErr.Error(), "source path "+method+": "), true)
				assert.Equal(t, errors.Is(gotErr, failure.err), true)
				pathErr, ok := errors.AsType[*fs.PathError](gotErr)
				assert.Equal(t, ok, true)
				assert.Equal(t, pathErr, original)
			})
		}
	}
}
