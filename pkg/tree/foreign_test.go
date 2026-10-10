package tree_test

import (
	"errors"
	"io/fs"
	"slices"
	"testing"
	"testing/fstest"

	"github.com/home-operations/flate/pkg/tree"
)

type foreignTree struct{ fs.ReadLinkFS }

func TestBuilder_ForeignLower(t *testing.T) {
	b := tree.NewBuilder(nil)
	must(t, b.AddFile("dir/file", []byte("lower"), 0o644))
	must(t, b.AddFile("dir/other", []byte("other"), 0o644))
	must(t, b.AddSymlink("link", "dir"))
	lower := build(t, b)
	for name, input := range map[string]fs.FS{"indexed": lower, "readlink": foreignTree{ReadLinkFS: lower}, "map": fstest.MapFS{"dir/file": {Data: []byte("lower")}, "dir/other": {Data: []byte("other")}}} {
		t.Run(name, func(t *testing.T) {
			b := tree.NewBuilder(input)
			must(t, b.AddFile("dir/file", []byte("upper"), 0o644))
			data, err := b.ReadFile("dir/file")
			if err != nil || string(data) != "upper" {
				t.Fatalf("upper read: %s %v", data, err)
			}
			if _, err := fs.Lstat(input, "link"); err == nil {
				data, err = b.ReadFile("link/file")
				if err != nil || string(data) != "upper" {
					t.Fatalf("merged link target: %s %v", data, err)
				}
			}
			must(t, b.RemoveAll("dir"))
			must(t, b.AddFile("dir/new", []byte("new"), 0o644))
			if _, err := b.Stat("dir/other"); !errors.Is(err, fs.ErrNotExist) {
				t.Fatalf("masked descendant: %v", err)
			}
			entries, err := b.ReadDir("dir")
			if err != nil || len(entries) != 1 || entries[0].Name() != "new" {
				t.Fatalf("recreated listing: %v %v", entries, err)
			}
			tr := build(t, b)
			var visited []string
			must(t, tr.WalkRaw(".", func(n string, _ fs.DirEntry, err error) error {
				visited = append(visited, n)
				if n == "dir" {
					return fs.SkipDir
				}
				return err
			}))
			if slices.Contains(visited, "dir/new") {
				t.Fatal("merged walk ignored SkipDir")
			}
		})
	}
}

type deniedFS struct{}

func (deniedFS) Open(n string) (fs.File, error) {
	return nil, &fs.PathError{Op: "open", Path: n, Err: fs.ErrPermission}
}

func TestBuilder_LowerError(t *testing.T) {
	b := tree.NewBuilder(deniedFS{})
	if _, err := b.ReadFile("file"); !errors.Is(err, fs.ErrPermission) {
		t.Fatalf("lower error: %v", err)
	}
	if err := b.AddFile("file", []byte("upper"), 0o644); !errors.Is(err, fs.ErrPermission) {
		t.Fatalf("construction swallowed lower error: %v", err)
	}
}

type countedDirFS struct {
	fs.FS
	dirs int
}

func (f *countedDirFS) ReadDir(name string) ([]fs.DirEntry, error) {
	f.dirs++
	return fs.ReadDir(f.FS, name)
}

func TestBuilder_ForeignDirectoriesStayLazy(t *testing.T) {
	lower := &countedDirFS{FS: fstest.MapFS{"apps/one/file": {Data: []byte("one")}, "apps/two/file": {Data: []byte("two")}}}
	b := tree.NewBuilder(lower)
	must(t, b.AddFile("apps/one/generated", []byte("upper"), 0o644))
	if lower.dirs != 0 {
		t.Fatalf("write listed %d lower directories", lower.dirs)
	}
	tr := build(t, b)
	next := tree.NewBuilder(tr)
	must(t, next.AddFile("apps/one/later", []byte("later"), 0o644))
	entries, err := next.ReadDir("apps/one")
	if err != nil {
		t.Fatal(err)
	}
	names := make([]string, len(entries))
	for i, e := range entries {
		names[i] = e.Name()
	}
	if !slices.Equal(names, []string{"file", "generated", "later"}) {
		t.Fatalf("stacked foreign listing: %v", names)
	}
}
