package tree_test

import (
	"errors"
	"fmt"
	"io/fs"
	"slices"
	"sync"
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

func TestBuilder_RemoveAllMasksForeignListing(t *testing.T) {
	b := tree.NewBuilder(fstest.MapFS{"d/a": {Data: []byte("a")}, "d/b": {Data: []byte("b")}, "d/sub/c": {Data: []byte("c")}})
	must(t, b.RemoveAll("d/a"))
	must(t, b.RemoveAll("d/sub"))
	must(t, b.AddFile("d/z", []byte("z"), 0o644))
	entries, err := b.ReadDir("d")
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, e := range entries {
		names = append(names, e.Name())
	}
	if !slices.Equal(names, []string{"b", "z"}) {
		t.Fatalf("removed lower children listed: %v", names)
	}
}

func TestBuilder_SealedForeignOverlayConcurrentReads(t *testing.T) {
	lower := fstest.MapFS{}
	for i := range 64 {
		lower[fmt.Sprintf("d%d/f", i)] = &fstest.MapFile{Data: []byte("x")}
		lower[fmt.Sprintf("d%d/gone", i)] = &fstest.MapFile{Data: []byte("gone")}
	}
	b := tree.NewBuilder(lower)
	for i := range 64 {
		must(t, b.RemoveAll(fmt.Sprintf("d%d/gone", i)))
	}
	must(t, b.AddFile("upper/f", []byte("u"), 0o644))
	sealed := build(t, tree.NewBuilder(build(t, b)))
	var wg sync.WaitGroup
	for g := range 4 {
		wg.Go(func() {
			for i := range 64 {
				if _, err := fs.ReadFile(sealed, fmt.Sprintf("d%d/f", (i+g)%64)); err != nil {
					t.Error(err)
				}
				entries, err := fs.ReadDir(sealed, fmt.Sprintf("d%d", i))
				if err != nil || len(entries) != 1 || entries[0].Name() != "f" {
					t.Errorf("stacked foreign listing: %v, %v", entries, err)
				}
			}
		})
	}
	wg.Wait()
}

func TestBuilder_StackedForeignRemovals(t *testing.T) {
	b := tree.NewBuilder(fstest.MapFS{
		"d/a":     {Data: []byte("a")},
		"d/b":     {Data: []byte("b")},
		"d/sub/c": {Data: []byte("c")},
	})
	must(t, b.RemoveAll("d/a"))
	must(t, b.RemoveAll("d/sub"))
	lower := build(t, b)
	for _, added := range []bool{false, true} {
		t.Run(fmt.Sprintf("added=%v", added), func(t *testing.T) {
			upper := tree.NewBuilder(lower)
			want := []string{"b"}
			if added {
				must(t, upper.AddFile("d/z", []byte("z"), 0o644))
				want = append(want, "z")
			}
			for _, sealed := range []bool{false, true} {
				t.Run(fmt.Sprintf("sealed=%v", sealed), func(t *testing.T) {
					var view fs.FS = upper
					if sealed {
						view = build(t, upper)
					}
					for _, name := range []string{"d/a", "d/sub/c"} {
						if _, err := fs.Stat(view, name); !errors.Is(err, fs.ErrNotExist) {
							t.Fatalf("removed path %s: %v", name, err)
						}
					}
					entries, err := fs.ReadDir(view, "d")
					must(t, err)
					var names []string
					for _, entry := range entries {
						names = append(names, entry.Name())
					}
					if !slices.Equal(names, want) {
						t.Fatalf("stacked listing = %v, want %v", names, want)
					}
					dir, err := view.Open("d")
					must(t, err)
					defer dir.Close()
					reader, ok := dir.(fs.ReadDirFile)
					if !ok {
						t.Fatal("directory handle does not implement fs.ReadDirFile")
					}
					entries, err = reader.ReadDir(-1)
					must(t, err)
					names = nil
					for _, entry := range entries {
						names = append(names, entry.Name())
					}
					if !slices.Equal(names, want) {
						t.Fatalf("directory handle = %v, want %v", names, want)
					}
					var visited []string
					must(t, fs.WalkDir(view, "d", func(name string, _ fs.DirEntry, err error) error {
						visited = append(visited, name)
						return err
					}))
					if len(visited) != len(want)+1 {
						t.Fatalf("walk = %v", visited)
					}
				})
			}
		})
	}
}
