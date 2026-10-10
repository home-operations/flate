package tree_test

import (
	"errors"
	"fmt"
	"io/fs"
	"sync"
	"testing"
	"testing/fstest"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/pkg/tree"
)

func TestBuilder_Normalization(t *testing.T) {
	for _, name := range []string{"a/../b", "a//b", "a +@日本.yaml", `a\b`} {
		t.Run(name, func(t *testing.T) {
			b := tree.NewBuilder(nil)
			must(t, b.AddFile(name, []byte("data"), 0o600))
			tr := build(t, b)
			p := name
			if name == "a/../b" {
				p = "b"
			}
			if name == "a//b" {
				p = "a/b"
			}
			data, err := tr.ReadFile(p)
			must(t, err)
			assert.Equal(t, string(data), "data")
		})
	}
	for _, name := range []string{"", ".", "../b", "/b", "a/../../b", "a\x00b"} {
		t.Run(name, func(t *testing.T) {
			b := tree.NewBuilder(nil)
			assert.Equal(t, errors.Is(b.AddFile(name, nil, 0o600), fs.ErrInvalid), true)
		})
	}
}

func TestBuilder_DuplicateLastWins(t *testing.T) {
	b := tree.NewBuilder(nil)
	data := []byte("first")
	must(t, b.AddFile("file", data, 0o600))
	data[0] = '!'
	got, err := b.ReadFile("file")
	must(t, err)
	assert.Equal(t, string(got), "first")
	must(t, b.AddFile("file", []byte("last"), 0o755))
	must(t, b.AddSymlink("link", "file"))
	must(t, b.AddSymlink("link", "other"))
	tr := build(t, b)
	got, err = tr.ReadFile("file")
	must(t, err)
	assert.Equal(t, string(got), "last")
	info, err := tr.Stat("file")
	must(t, err)
	assert.Equal(t, info.Mode(), fs.FileMode(0o755))
	target, err := tr.ReadLink("link")
	must(t, err)
	assert.Equal(t, target, "other")
}

func TestBuilder_FileDirConflictErrors(t *testing.T) {
	cases := []struct {
		name   string
		first  func(*tree.Builder) error
		second func(*tree.Builder) error
	}{
		{name: "file-then-dir", first: func(b *tree.Builder) error { return b.AddFile("a", nil, 0o600) },
			second: func(b *tree.Builder) error { return b.AddDir("a", 0o755) }},
		{name: "dir-then-file", first: func(b *tree.Builder) error { return b.AddDir("a", 0o755) },
			second: func(b *tree.Builder) error { return b.AddFile("a", nil, 0o600) }},
		{name: "file-then-child", first: func(b *tree.Builder) error { return b.AddFile("a", nil, 0o600) },
			second: func(b *tree.Builder) error { return b.AddFile("a/b", nil, 0o600) }},
		{name: "child-then-file", first: func(b *tree.Builder) error { return b.AddFile("a/b", nil, 0o600) },
			second: func(b *tree.Builder) error { return b.AddFile("a", nil, 0o600) }},
		{name: "link-then-child", first: func(b *tree.Builder) error { return b.AddSymlink("a", ".") },
			second: func(b *tree.Builder) error { return b.AddDir("a/b", 0o755) }},
		{name: "dir-then-link", first: func(b *tree.Builder) error { return b.AddDir("a", 0o755) },
			second: func(b *tree.Builder) error { return b.AddSymlink("a", ".") }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			b := tree.NewBuilder(nil)
			must(t, tc.first(b))
			assert.Equal(t, errors.Is(tc.second(b), tree.ErrConflict), true)
			lower := build(t, b)
			overlay := tree.NewBuilder(lower)
			assert.Equal(t, errors.Is(tc.second(overlay), tree.ErrConflict), true)
		})
	}
}

func TestBuilder_Sealed(t *testing.T) {
	b := tree.NewBuilder(nil)
	must(t, b.AddFile("file", []byte("data"), 0o600))
	tr := build(t, b)
	assert.Equal(t, errors.Is(b.AddFile("file", nil, 0o600), tree.ErrSealed), true)
	assert.Equal(t, errors.Is(b.AddDir("dir", 0o755), tree.ErrSealed), true)
	assert.Equal(t, errors.Is(b.AddSymlink("link", "file"), tree.ErrSealed), true)
	assert.Equal(t, errors.Is(b.RemoveAll("."), tree.ErrSealed), true)
	_, err := b.Build()
	assert.Equal(t, errors.Is(err, tree.ErrSealed), true)
	data, err := tr.ReadFile("file")
	must(t, err)
	assert.Equal(t, string(data), "data")
}

func TestOverlay_MergedWalkSkipDir(t *testing.T) {
	lower := fixture(t)
	b := tree.NewBuilder(lower)
	must(t, b.AddFile("dir/generated +@日本.yaml", []byte("generated"), 0o600))
	must(t, b.AddFile("dir/z.yaml", []byte("replaced"), 0o600))
	must(t, b.AddFile("upper/file", []byte("upper"), 0o600))
	tr := build(t, b)
	must(t, fstest.TestFS(tr, "dir/generated +@日本.yaml", "dir/a +@日本.yaml", "upper/file"))
	for _, raw := range []bool{false, true} {
		t.Run(map[bool]string{false: "WalkDir", true: "WalkRaw"}[raw], func(t *testing.T) {
			var visited []string
			fn := func(p string, _ fs.DirEntry, err error) error {
				visited = append(visited, p)
				if p == "dir" {
					return fs.SkipDir
				}
				return err
			}
			if raw {
				must(t, tr.WalkRaw(".", fn))
			} else {
				must(t, fs.WalkDir(tr, ".", fn))
			}
			assert.Diff(t, visited, []string{".", "alias", "dir", "empty", "upper", "upper/file"})
		})
	}
	data, err := lower.ReadFile("dir/z.yaml")
	must(t, err)
	assert.Equal(t, string(data), "zulu")
	_, err = lower.Stat("upper")
	assert.Equal(t, errors.Is(err, fs.ErrNotExist), true)
}

func TestOverlay_RemoveRecreate(t *testing.T) {
	lower := fixture(t)
	b := tree.NewBuilder(lower)
	must(t, b.AddFile("dir/upper", []byte("upper"), 0o600))
	must(t, b.RemoveAll("dir"))
	must(t, b.AddDir("dir", 0o750))
	must(t, b.AddFile("dir/new", []byte("new"), 0o600))
	for _, name := range []string{"dir/a +@日本.yaml", "dir/z.yaml", "dir/upper"} {
		_, err := b.Stat(name)
		assert.Equal(t, errors.Is(err, fs.ErrNotExist), true)
	}
	entries, err := b.ReadDir("dir")
	must(t, err)
	assert.Equal(t, len(entries), 1)
	assert.Equal(t, entries[0].Name(), "new")
	must(t, b.RemoveAll("dir"))
	must(t, b.AddFile("dir", []byte("masked"), 0o600))
	_, err = b.ReadDir("dir")
	assert.Equal(t, errors.Is(err, fs.ErrInvalid), true)
	_, err = b.Stat("dir/new")
	assert.Equal(t, errors.Is(err, fs.ErrInvalid), true)
	must(t, b.RemoveAll("dir"))
	must(t, b.AddFile("dir/reborn", []byte("reborn"), 0o600))
	tr := build(t, b)
	entries, err = tr.ReadDir("dir")
	must(t, err)
	assert.Equal(t, len(entries), 1)
	assert.Equal(t, entries[0].Name(), "reborn")
	_, err = tr.Stat("dir/z.yaml")
	assert.Equal(t, errors.Is(err, fs.ErrNotExist), true)
	data, err := lower.ReadFile("dir/z.yaml")
	must(t, err)
	assert.Equal(t, string(data), "zulu")
	upper := tree.NewBuilder(tr)
	must(t, upper.RemoveAll("."))
	must(t, upper.AddFile("fresh", []byte("fresh"), 0o600))
	must(t, fstest.TestFS(build(t, upper), "fresh"))
	_, err = tr.Stat("empty")
	must(t, err)
}

func TestOverlay_ConcurrentBuilders(t *testing.T) {
	for _, workers := range []int{2, 4} {
		t.Run(fmt.Sprint(workers), func(t *testing.T) {
			lower := fixture(t)
			start := make(chan struct{})
			var wg sync.WaitGroup
			for i := range workers {
				wg.Go(func() {
					<-start
					b := tree.NewBuilder(lower)
					want := fmt.Sprintf("render-%d", i)
					if err := b.AddFile("dir/z.yaml", []byte(want), 0o600); err != nil {
						t.Error(err)
						return
					}
					if err := b.RemoveAll("empty"); err != nil {
						t.Error(err)
						return
					}
					tr, err := b.Build()
					if err != nil {
						t.Error(err)
						return
					}
					data, err := tr.ReadFile("dir/z.yaml")
					if err != nil || string(data) != want {
						t.Errorf("private render: %q, %v; want %q", data, err, want)
					}
				})
			}
			close(start)
			wg.Wait()
			data, err := lower.ReadFile("dir/z.yaml")
			must(t, err)
			assert.Equal(t, string(data), "zulu")
			_, err = lower.Stat("empty")
			must(t, err)
		})
	}
}

func TestOverlay_RemoveAllIsExact(t *testing.T) {
	for _, overlay := range []bool{false, true} {
		name := map[bool]string{false: "source", true: "overlay"}[overlay]
		t.Run(name, func(t *testing.T) {
			var b *tree.Builder
			if overlay {
				b = tree.NewBuilder(fixture(t))
			} else {
				b = tree.NewBuilder(nil)
				must(t, b.AddDir("empty", 0o750))
			}
			must(t, b.AddFile("dir/a", []byte("a"), 0o600))
			must(t, b.AddFile("dir2/b", []byte("b"), 0o600))
			must(t, b.AddFile("dir.yaml", []byte("y"), 0o600))
			must(t, b.RemoveAll("dir"))
			for p, want := range map[string]string{"dir2/b": "b", "dir.yaml": "y"} {
				data, err := b.ReadFile(p)
				must(t, err)
				assert.Equal(t, string(data), want)
			}
			_, err := b.Stat("dir/a")
			assert.Equal(t, errors.Is(err, fs.ErrNotExist), true)
			must(t, b.RemoveAll("empty"))
			if overlay {
				must(t, b.RemoveAll("alias"))
			}
			entries, err := b.ReadDir(".")
			must(t, err)
			for _, e := range entries {
				assert.Equal(t, e.Name() != "dir" && e.Name() != "empty", true)
			}
			var visited []string
			must(t, b.WalkRaw(".", func(p string, _ fs.DirEntry, err error) error {
				visited = append(visited, p)
				return err
			}))
			for _, p := range visited {
				assert.Equal(t, p != "dir" && p != "empty", true)
			}
			must(t, fstest.TestFS(build(t, b), "dir2/b", "dir.yaml"))
		})
	}
}

func TestOverlay_RemoveAllRootKeepsRoot(t *testing.T) {
	for _, overlay := range []bool{false, true} {
		name := map[bool]string{false: "source", true: "overlay"}[overlay]
		t.Run(name, func(t *testing.T) {
			var b *tree.Builder
			if overlay {
				b = tree.NewBuilder(fixture(t))
			} else {
				b = tree.NewBuilder(nil)
			}
			must(t, b.AddFile("upper/file", []byte("u"), 0o600))
			must(t, b.AddDir("d", 0o700))
			must(t, b.RemoveAll("."))
			info, err := b.Stat(".")
			must(t, err)
			assert.Equal(t, info.IsDir(), true)
			entries, err := b.ReadDir(".")
			must(t, err)
			assert.Equal(t, len(entries), 0)
			for _, p := range []string{"upper/file", "upper", "d"} {
				_, err := b.Stat(p)
				assert.Equal(t, errors.Is(err, fs.ErrNotExist), true)
			}
			must(t, fstest.TestFS(build(t, b)))
		})
	}
}

func TestBuilder_RejectsMalformedKinds(t *testing.T) {
	b := tree.NewBuilder(nil)
	for name, err := range map[string]error{
		"file-dir-bit":       b.AddFile("f", nil, fs.ModeDir|0o755),
		"file-symlink-bit":   b.AddFile("f", nil, fs.ModeSymlink|0o777),
		"dir-symlink-bit":    b.AddDir("d", fs.ModeSymlink|0o755),
		"dir-file-bit":       b.AddDir("d", fs.ModeDevice|0o755),
		"symlink-root":       b.AddSymlink(".", "x"),
		"symlink-nul":        b.AddSymlink("l", "a\x00b"),
		"symlink-empty-name": b.AddSymlink("", "x"),
	} {
		t.Run(name, func(t *testing.T) { assert.Equal(t, errors.Is(err, fs.ErrInvalid), true) })
	}
	_, err := b.Stat("f")
	assert.Equal(t, errors.Is(err, fs.ErrNotExist), true)
}

func TestBuilder_Metadata(t *testing.T) {
	b := tree.NewBuilder(nil)
	must(t, b.AddDir("empty", 0o750))
	must(t, b.AddFile("implicit/file", []byte("x"), 0o600))
	must(t, b.AddFile("later/file", []byte("x"), 0o600))
	must(t, b.AddDir("later", 0o700))
	must(t, b.AddSymlink("link", "implicit/file"))
	lower := build(t, b)
	check := func(t *testing.T, tr *tree.Tree) {
		t.Helper()
		for p, want := range map[string]fs.FileMode{
			"empty": fs.ModeDir | 0o750, "implicit": fs.ModeDir | 0o755, "later": fs.ModeDir | 0o700,
		} {
			info, err := tr.Stat(p)
			must(t, err)
			assert.Equal(t, info.Mode(), want)
		}
		info, err := tr.Lstat("link")
		must(t, err)
		assert.Equal(t, info.Mode(), fs.ModeSymlink|0o777)
		assert.Equal(t, info.Size(), int64(len("implicit/file")))
	}
	check(t, lower)
	o := tree.NewBuilder(lower)
	must(t, o.AddDir("empty", 0o700))
	must(t, o.AddDir("implicit", 0o710))
	over := build(t, o)
	info, err := over.Stat("empty")
	must(t, err)
	assert.Equal(t, info.Mode(), fs.ModeDir|0o700)
	info, err = over.Stat("implicit")
	must(t, err)
	assert.Equal(t, info.Mode(), fs.ModeDir|0o710)
	_, err = over.Stat("implicit/file")
	must(t, err)
	check(t, lower)
}
