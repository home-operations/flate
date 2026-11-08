package tree_test

import (
	"errors"
	"fmt"
	"io"
	"io/fs"
	"path"
	"slices"
	"sync"
	"testing"
	"testing/fstest"

	"github.com/home-operations/flate/internal/assert"

	"github.com/home-operations/flate/internal/tree"
)

type foreignTree struct{ fs.ReadLinkFS }

func TestBuilder_MountRoot(t *testing.T) {
	t.Parallel()
	for _, indexed := range []bool{false, true} {
		t.Run(fmt.Sprintf("indexed=%t", indexed), func(t *testing.T) {
			t.Parallel()
			tr, contract := rootMountFixture(t, indexed)
			for _, view := range []struct {
				name string
				tree *tree.Tree
			}{{"tree", tr}, {"sealed_overlay", build(t, tree.NewBuilder(tr))}} {
				t.Run(view.name, func(t *testing.T) {
					t.Parallel()
					assertMountView(t, view.tree, contract)
				})
			}
		})
	}
}

func TestBuilder_MountReplacement(t *testing.T) {
	t.Parallel()
	for _, lower := range []bool{false, true} {
		t.Run(fmt.Sprintf("indexed_lower=%t", lower), func(t *testing.T) {
			t.Parallel()
			tr, contract := replacementMountFixture(t, lower)
			for _, view := range []struct {
				name string
				tree *tree.Tree
			}{{"tree", tr}, {"sealed_overlay", build(t, tree.NewBuilder(tr))}} {
				t.Run(view.name, func(t *testing.T) {
					t.Parallel()
					assertMountView(t, view.tree, contract)
				})
			}
		})
	}
}

func TestBuilder_SealedMountConcurrentReads(t *testing.T) {
	t.Parallel()
	for _, fixture := range []struct {
		name  string
		build func(*testing.T, bool) (*tree.Tree, mountContract)
	}{{"root", rootMountFixture}, {"replacement", replacementMountFixture}} {
		t.Run(fixture.name, func(t *testing.T) {
			t.Parallel()
			tr, contract := fixture.build(t, true)
			overlay := build(t, tree.NewBuilder(tr))
			start := make(chan struct{})
			var readers sync.WaitGroup
			for range 2 {
				readers.Go(func() {
					<-start
					assertMountView(t, tr, contract)
					assertMountView(t, overlay, contract)
				})
			}
			close(start)
			readers.Wait()
		})
	}
}

type mountEntry struct {
	name, data string
	mode       fs.FileMode
}

type mountContract struct {
	entries []mountEntry
	missing []string
}

func rootMountFixture(t *testing.T, indexed bool) (*tree.Tree, mountContract) {
	t.Helper()
	b := tree.NewBuilder(nil)
	contract := mountContract{entries: []mountEntry{
		{name: ".", mode: fs.ModeDir | 0o750},
		{name: "f", data: "mounted root", mode: 0o640},
		{name: "sub", mode: fs.ModeDir | 0o710},
		{name: "sub/g", data: "mounted nested", mode: 0o600},
		{name: "sub/override", data: "indexed override", mode: 0o604},
	}}
	if indexed {
		for _, name := range []string{"old", "old-sub/gone", "d2/keep"} {
			must(t, b.AddFile(name, []byte("replaced"), 0o644))
		}
		contract.missing = []string{"old", "old-sub", "old-sub/gone", "d2", "d2/keep"}
	}
	must(t, b.Mount(".", fstest.MapFS{
		".":            {Mode: fs.ModeDir | 0o750},
		"f":            {Data: []byte("mounted root"), Mode: 0o640},
		"sub":          {Mode: fs.ModeDir | 0o710},
		"sub/g":        {Data: []byte("mounted nested"), Mode: 0o600},
		"sub/override": {Data: []byte("mounted override"), Mode: 0o644},
	}))
	must(t, b.AddFile("sub/override", []byte("indexed override"), 0o604))
	return build(t, b), contract
}

func replacementMountFixture(t *testing.T, lower bool) (*tree.Tree, mountContract) {
	t.Helper()
	b := tree.NewBuilder(nil)
	for _, name := range []string{"d/old", "d/sub/old"} {
		must(t, b.AddFile(name, []byte("replaced"), 0o600))
	}
	must(t, b.AddFile("d2/keep", []byte("outside"), 0o641))
	if lower {
		b = tree.NewBuilder(build(t, b))
	}
	must(t, b.Mount("d", fstest.MapFS{
		".":   {Mode: fs.ModeDir | 0o750},
		"new": {Data: []byte("mounted new"), Mode: 0o640},
	}))
	return build(t, b), mountContract{entries: []mountEntry{
		{name: ".", mode: fs.ModeDir | 0o755},
		{name: "d", mode: fs.ModeDir | 0o750},
		{name: "d/new", data: "mounted new", mode: 0o640},
		{name: "d2", mode: fs.ModeDir | 0o755},
		{name: "d2/keep", data: "outside", mode: 0o641},
	}, missing: []string{"d/old", "d/sub", "d/sub/old"}}
}

func assertMountView(t *testing.T, tr *tree.Tree, contract mountContract) {
	t.Helper()
	wantWalk := make([]string, 0, len(contract.entries))
	for _, want := range contract.entries {
		wantWalk = append(wantWalk, want.name)
		info, err := fs.Stat(tr, want.name)
		if err != nil {
			t.Errorf("Stat(%q): %v", want.name, err)
		} else if info.Mode() != want.mode {
			t.Errorf("Stat(%q) mode = %v, want %v", want.name, info.Mode(), want.mode)
		}
		if want.mode.IsDir() {
			assertMountedDirectory(t, tr, contract, want)
			continue
		}
		assertMountedFile(t, tr, want)
	}
	assertMountViewCompletion(t, tr, &contract, &wantWalk)
}

func TestBuilder_ForeignLower(t *testing.T) {
	t.Parallel()
	b := tree.NewBuilder(nil)
	must(t, b.AddFile("dir/file", []byte("lower"), 0o644))
	must(t, b.AddFile("dir/other", []byte("other"), 0o644))
	must(t, b.AddSymlink("link", "dir"))
	lower := build(t, b)
	for name, input := range map[string]fs.FS{"indexed": lower, "readlink": foreignTree{ReadLinkFS: lower}, "map": fstest.MapFS{"dir/file": {Data: []byte("lower")}, "dir/other": {Data: []byte("other")}}} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			b := tree.NewBuilder(input)
			must(t, b.AddFile("dir/file", []byte("upper"), 0o644))
			data, err := b.ReadFile("dir/file")
			assert.FatalIf(t, err != nil || string(data) != "upper", "upper read: %s %v", data, err)
			if _, lstatErr := fs.Lstat(input, "link"); lstatErr == nil {
				data, lstatErr = b.ReadFile("link/file")
				assert.FatalIf(
					t,
					lstatErr != nil || string(data) != "upper",
					"merged link target: %s %v",
					data,
					lstatErr,
				)
			}
			must(t, b.RemoveAll("dir"))
			must(t, b.AddFile("dir/new", []byte("new"), 0o644))
			{
				_, statErr2 := b.Stat("dir/other")
				assert.FatalIf(t, !errors.Is(statErr2, fs.ErrNotExist), "masked descendant: %v", statErr2)
			}
			entries, err := b.ReadDir("dir")
			assert.FatalIf(
				t,
				err != nil || len(entries) != 1 || entries[0].Name() != "new",
				"recreated listing: %v %v",
				entries,
				err,
			)
			tr := build(t, b)
			var visited []string
			must(t, tr.WalkRaw(".", func(n string, _ fs.DirEntry, err error) error {
				visited = append(visited, n)
				if n == "dir" {
					return fs.SkipDir
				}
				return err
			}))
			assert.FatalIf(t, slices.Contains(visited, "dir/new"), "%s", "merged walk ignored SkipDir")
		})
	}
}

type deniedFS struct{}

func (deniedFS) Open(n string) (fs.File, error) {
	return nil, &fs.PathError{Op: "open", Path: n, Err: fs.ErrPermission}
}

func TestBuilder_LowerError(t *testing.T) {
	t.Parallel()
	b := tree.NewBuilder(deniedFS{})
	{
		_, err := b.ReadFile("file")
		assert.FatalIf(t, !errors.Is(err, fs.ErrPermission), "lower error: %v", err)
	}
	{
		err := b.AddFile("file", []byte("upper"), 0o644)
		assert.FatalIf(t, !errors.Is(err, fs.ErrPermission), "construction swallowed lower error: %v", err)
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
	t.Parallel()
	lower := &countedDirFS{
		FS: fstest.MapFS{"apps/one/file": {Data: []byte("one")}, "apps/two/file": {Data: []byte("two")}},
	}
	b := tree.NewBuilder(lower)
	must(t, b.AddFile("apps/one/generated", []byte("upper"), 0o644))
	if lower.dirs != 0 {
		t.Fatalf("write listed %d lower directories", lower.dirs)
	}
	tr := build(t, b)
	next := tree.NewBuilder(tr)
	must(t, next.AddFile("apps/one/later", []byte("later"), 0o644))
	entries, err := next.ReadDir("apps/one")
	assert.NoError(t, err)
	names := make([]string, len(entries))
	for i, e := range entries {
		names[i] = e.Name()
	}
	assert.FatalIf(
		t,
		!slices.Equal(names, []string{"file", "generated", "later"}),
		"stacked foreign listing: %v",
		names,
	)
}

func TestBuilder_RemoveAllMasksForeignListing(t *testing.T) {
	t.Parallel()
	b := tree.NewBuilder(
		fstest.MapFS{"d/a": {Data: []byte("a")}, "d/b": {Data: []byte("b")}, "d/sub/c": {Data: []byte("c")}},
	)
	must(t, b.RemoveAll("d/a"))
	must(t, b.RemoveAll("d/sub"))
	must(t, b.AddFile("d/z", []byte("z"), 0o644))
	entries, err := b.ReadDir("d")
	assert.NoError(t, err)
	names := make([]string, 0, len(entries))
	for _, e := range entries {
		names = append(names, e.Name())
	}
	assert.FatalIf(t, !slices.Equal(names, []string{"b", "z"}), "removed lower children listed: %v", names)
}

func TestBuilder_SealedForeignOverlayConcurrentReads(t *testing.T) {
	t.Parallel()
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
				{
					_, err := fs.ReadFile(sealed, fmt.Sprintf("d%d/f", (i+g)%64))
					assert.ErrorIf(t, err != nil, "%v", err)
				}
				entries, err := fs.ReadDir(sealed, fmt.Sprintf("d%d", i))
				assert.ErrorIf(
					t,
					err != nil || len(entries) != 1 || entries[0].Name() != "f",
					"stacked foreign listing: %v, %v",
					entries,
					err,
				)
			}
		})
	}
	wg.Wait()
}

func TestBuilder_StackedForeignRemovals(t *testing.T) {
	t.Parallel()
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
			t.Parallel()
			testBuilderStackedForeignRemovalsCase(
				t,
				&testBuilderStackedForeignRemovalsCaseInputs{lower: &lower, added: &added},
			)
		},
		)
	}
}

type testBuilderStackedForeignRemovalsCaseInputs struct {
	lower **tree.Tree
	added *bool
}

func testBuilderStackedForeignRemovalsCase(t *testing.T, testCaseInput *testBuilderStackedForeignRemovalsCaseInputs) {
	t.Helper()

	upper := tree.NewBuilder((*testCaseInput.lower))
	want := []string{"b"}
	if *testCaseInput.added {
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
				{
					_, err := fs.Stat(view, name)
					assert.FatalIf(t, !errors.Is(err, fs.ErrNotExist), "removed path %s: %v", name, err)
				}
			}
			entries, err := fs.ReadDir(view, "d")
			must(t, err)
			names := make([]string, 0, len(entries))
			for _, entry := range entries {
				names = append(names, entry.Name())
			}
			assert.FatalIf(t, !slices.Equal(names, want), "stacked listing = %v, want %v", names, want)
			dir, err := view.Open("d")
			must(t, err)
			defer dir.Close()
			reader, ok := dir.(fs.ReadDirFile)
			assert.FatalIf(t, !ok, "%s", "directory handle does not implement fs.ReadDirFile")
			entries, err = reader.ReadDir(-1)
			must(t, err)
			names = nil
			for _, entry := range entries {
				names = append(names, entry.Name())
			}
			assert.FatalIf(t, !slices.Equal(names, want), "directory handle = %v, want %v", names, want)
			var visited []string
			must(t, fs.WalkDir(view, "d", func(name string, _ fs.DirEntry, err error) error {
				visited = append(visited, name)
				return err
			}))
			assert.FatalIf(t, len(visited) != len(want)+1, "walk = %v", visited)
		})
	}
}

func assertMountViewCompletion(t *testing.T, tr *tree.Tree, contract *mountContract, wantWalk *[]string) {
	t.Helper()
	for _, name := range contract.missing {
		{
			_, err := fs.ReadFile(tr, name)
			assert.ErrorIf(t, !errors.Is(err, fs.ErrNotExist), "ReadFile(%q) = %v, want ErrNotExist", name, err)
		}
		if file, err := tr.Open(name); !errors.Is(err, fs.ErrNotExist) {
			if err == nil {
				file.Close()
			}
			t.Errorf("Open(%q) = %v, want ErrNotExist", name, err)
		}
		{
			_, err := fs.Stat(tr, name)
			assert.ErrorIf(t, !errors.Is(err, fs.ErrNotExist), "Stat(%q) = %v, want ErrNotExist", name, err)
		}
	}
	slices.Sort((*wantWalk))
	for _, walk := range []struct {
		name string
		run  func(fs.WalkDirFunc) error
	}{{"WalkDir", func(fn fs.WalkDirFunc) error { return fs.WalkDir(tr, ".", fn) }}, {"WalkRaw", func(fn fs.WalkDirFunc) error { return tr.WalkRaw(".", fn) }}} {
		for range 2 {
			var visited []string
			err := walk.run(func(name string, _ fs.DirEntry, err error) error {
				visited = append(visited, name)
				return err
			})
			assert.ErrorIf(
				t,
				err != nil || !slices.Equal(visited, (*wantWalk)),
				"%s = %v, %v; want %v",
				walk.name,
				visited,
				err,
				(*wantWalk),
			)
		}
	}
}

func assertMountedDirectory(t *testing.T, tr *tree.Tree, contract mountContract, want mountEntry) {
	t.Helper()
	entries, readDirErr := fs.ReadDir(tr, want.name)
	if readDirErr != nil {
		t.Errorf("ReadDir(%q): %v", want.name, readDirErr)
		return
	}
	names := make([]string, 0, len(entries))
	var wantNames []string
	for _, child := range contract.entries {
		if child.name != "." && path.Dir(child.name) == want.name {
			wantNames = append(wantNames, path.Base(child.name))
		}
	}
	slices.Sort(wantNames)
	for _, entry := range entries {
		names = append(names, entry.Name())
		listed, err := entry.Info()
		actual, statErr := fs.Stat(tr, path.Join(want.name, entry.Name()))
		if err != nil || statErr != nil {
			t.Errorf("listed %q metadata: Info=%v Stat=%v", entry.Name(), err, statErr)
		} else if listed.Mode() != actual.Mode() || listed.Size() != actual.Size() {
			t.Errorf("listed %q metadata disagrees with Stat", entry.Name())
		}
	}
	assert.ErrorIf(
		t,
		!slices.Equal(names, wantNames),
		"ReadDir(%q) = %v, want %v",
		want.name,
		names,
		wantNames,
	)
}

func assertMountedFile(t *testing.T, tr *tree.Tree, want mountEntry) {
	t.Helper()

	data, err := fs.ReadFile(tr, want.name)
	assert.ErrorIf(
		t,
		err != nil || string(data) != want.data,
		"ReadFile(%q) = %q, %v; want %q",
		want.name,
		data,
		err,
		want.data,
	)
	file, err := tr.Open(want.name)
	if err != nil {
		t.Errorf("Open(%q): %v", want.name, err)
		return
	}
	opened, statErr := file.Stat()
	data, readErr := io.ReadAll(file)
	closeErr := file.Close()
	if statErr != nil || readErr != nil || closeErr != nil || string(data) != want.data {
		t.Errorf("opened %q = %q, Stat=%v Read=%v Close=%v", want.name, data, statErr, readErr, closeErr)
	} else if opened.Mode() != want.mode || opened.Size() != int64(len(want.data)) {
		t.Errorf("opened %q metadata = %v, %d", want.name, opened.Mode(), opened.Size())
	}
}
