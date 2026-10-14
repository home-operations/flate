package source

import (
	"errors"
	"io/fs"
	"net"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"testing/fstest"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
)

func TestLocalFS_Reads(t *testing.T) {
	parent := t.TempDir()
	root := filepath.Join(parent, "root")
	testutil.WriteFile(t, root, "regular.txt", "content")
	testutil.WriteFile(t, parent, "outside", "outside")
	for name, target := range map[string]string{"file-link": "regular.txt", "dir-link": ".", "dangling": "missing", "escape": "../outside"} {
		assert.Equal(t, os.Symlink(target, filepath.Join(root, name)), nil)
	}
	socket, err := net.Listen("unix", filepath.Join(root, "socket"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { assert.Equal(t, socket.Close(), nil) })
	input, err := NewLocalFS(t.Context(), root)
	if err != nil {
		t.Fatal(err)
	}
	view, _, err := ReadView(t.Context(), input, false)
	if err != nil {
		t.Fatal(err)
	}
	for _, reader := range []struct {
		name string
		fs   fs.FS
	}{{name: "descriptor", fs: input}, {name: "view", fs: view}} {
		t.Run(reader.name, func(t *testing.T) {
			entries, err := fs.ReadDir(reader.fs, ".")
			if err != nil || len(entries) != 1 || entries[0].Name() != "regular.txt" {
				t.Fatalf("admitted entries = %v, %v", entries, err)
			}
			for _, name := range []string{"regular.txt", "file-link", "dir-link/regular.txt", "dangling", "escape", "socket"} {
				t.Run(name, func(t *testing.T) {
					var want error
					content := "content"
					if name != "regular.txt" {
						want, content = fs.ErrNotExist, ""
					}
					data, err := fs.ReadFile(reader.fs, name)
					if !errors.Is(err, want) || string(data) != content {
						t.Fatalf("read = %q, %v; want %q, %v", data, err, content, want)
					}
					if _, err := fs.Stat(reader.fs, name); !errors.Is(err, want) {
						t.Fatalf("stat = %v; want %v", err, want)
					}
				})
			}
		})
	}

}

func TestLocalFS_ReadView(t *testing.T) {
	t.Run("point_lookup_without_listing", func(t *testing.T) {
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
		input, err := NewLocalFS(t.Context(), root)
		assert.Equal(t, err, nil)
		for _, name := range []string{"a", "b"} {
			file := "siblings/" + name
			data, readErr := fs.ReadFile(input, file)
			_, statErr := fs.Stat(input, file)
			if readErr != nil || statErr != nil || string(data) != name {
				t.Fatalf("point lookup %s = %q, %v, %v", file, data, readErr, statErr)
			}
		}
	})
	root := t.TempDir()
	testutil.WriteFile(t, root, ".sourceignore", "/*\n!/dir/arrived\n!/hidden/empty/\n")
	testutil.WriteFile(t, root, "dir/excluded", "excluded")
	assert.Equal(t, os.MkdirAll(filepath.Join(root, "hidden/empty"), 0o755), nil)
	input, err := NewLocalFS(t.Context(), root)
	if err != nil {
		t.Fatal(err)
	}
	observed, _, err := ReadView(t.Context(), input, false)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fs.ReadFile(observed, "dir/arrived"); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("initial missing file = %v", err)
	}
	hidden, _, err := ReadView(t.Context(), input, true)
	assert.Equal(t, err, nil)
	if _, err := fs.Stat(hidden, "dir"); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("directory without survivors = %v", err)
	}
	if _, err := fs.Stat(hidden, "hidden/empty"); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("re-included empty directory below hidden ancestor = %v", err)
	}
	if _, err := fs.Stat(observed, "hidden/empty"); err != nil {
		t.Fatalf("unfiltered empty directory = %v", err)
	}
	testutil.WriteFile(t, root, "dir/arrived", "arrived")
	fresh, _, err := ReadView(t.Context(), input, true)
	assert.Equal(t, err, nil)
	testutil.WriteFile(t, root, ".sourceignore", "/*\n")
	readmitted, err := NewLocalFS(t.Context(), root)
	if err != nil {
		t.Fatal(err)
	}
	ignored, _, err := ReadView(t.Context(), readmitted, true)
	assert.Equal(t, err, nil)
	plain, _, err := ReadView(t.Context(), readmitted, false)
	assert.Equal(t, err, nil)
	memory := fstest.MapFS{"kept": {Data: []byte("content")}}
	passthrough, _, err := ReadView(t.Context(), memory, true)
	assert.Equal(t, err, nil)
	memory["later"] = &fstest.MapFile{Data: []byte("same FS")}
	for _, tc := range []struct {
		name, path, content string
		view                fs.FS
		want                error
	}{
		{name: "retained directory observation", path: "dir/arrived", view: observed, want: fs.ErrNotExist},
		{name: "fresh view", path: "dir/arrived", content: "arrived", view: fresh},
		{name: "refreshed ignore policy", path: "dir/arrived", view: ignored, want: fs.ErrNotExist},
		{name: "unfiltered plain read", path: "dir/arrived", content: "arrived", view: plain},
		{name: "caller filesystem", path: "later", content: "same FS", view: passthrough},
	} {
		t.Run(tc.name, func(t *testing.T) {
			data, err := fs.ReadFile(tc.view, tc.path)
			if !errors.Is(err, tc.want) || string(data) != tc.content {
				t.Fatalf("view read = %q, %v; want %q, %v", data, err, tc.content, tc.want)
			}
		})
	}
	start := make(chan struct{})
	var wg sync.WaitGroup
	for range 2 {
		wg.Go(func() {
			<-start
			view, _, err := ReadView(t.Context(), input, true)
			if err != nil {
				t.Error(err)
				return
			}
			for _, reader := range []fs.FS{input, view} {
				data, err := fs.ReadFile(reader, "dir/arrived")
				if err != nil || string(data) != "arrived" {
					t.Errorf("concurrent fresh read = %q, %v", data, err)
				}
			}
		})
	}
	close(start)
	wg.Wait()
	assert.Equal(t, os.RemoveAll(filepath.Join(root, "dir")), nil)
	assert.Equal(t, os.Symlink(".", filepath.Join(root, "dir")), nil)
	for _, name := range []string{"dir", "dir/arrived"} {
		if _, err := fs.Stat(input, name); !errors.Is(err, fs.ErrNotExist) {
			t.Fatalf("fresh lookup through directory link: %s: %v", name, err)
		}
	}
}
