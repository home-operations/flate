package tree_test

import (
	"errors"
	"fmt"
	"io"
	"io/fs"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"testing/fstest"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/tree"
)

var (
	_ fs.ReadFileFS = (*tree.Tree)(nil)
	_ fs.ReadDirFS  = (*tree.Tree)(nil)
	_ fs.StatFS     = (*tree.Tree)(nil)
	_ fs.ReadLinkFS = (*tree.Tree)(nil)
)

func must(t testing.TB, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

func build(t testing.TB, b *tree.Builder) *tree.Tree {
	t.Helper()
	tr, err := b.Build()
	must(t, err)
	return tr
}

func fixture(t testing.TB) *tree.Tree {
	t.Helper()
	b := tree.NewBuilder(nil)
	must(t, b.AddDir("empty", 0o750))
	must(t, b.AddFile("dir/a +@日本.yaml", []byte("alpha"), 0o640))
	must(t, b.AddFile("dir/z.yaml", []byte("zulu"), 0o755))
	must(t, b.AddSymlink("alias", "dir/a +@日本.yaml"))
	return build(t, b)
}

func TestTree_FSContract(t *testing.T) {
	tr := fixture(t)
	must(t, fstest.TestFS(tr, "empty", "dir/a +@日本.yaml", "dir/z.yaml", "alias"))
	info, err := tr.Stat("dir/z.yaml")
	must(t, err)
	assert.Equal(t, info.Mode(), fs.FileMode(0o755))
	assert.Equal(t, info.Size(), int64(4))
	must(t, fstest.TestFS(build(t, tree.NewBuilder(nil))))
}

func TestTree_OpenIndependentHandles(t *testing.T) {
	tr := fixture(t)
	a, err := tr.Open("alias")
	must(t, err)
	defer a.Close()
	b, err := tr.Open("alias")
	must(t, err)
	defer b.Close()
	buf := make([]byte, 2)
	_, err = a.Read(buf)
	must(t, err)
	assert.Equal(t, string(buf), "al")
	data, err := io.ReadAll(b)
	must(t, err)
	assert.Equal(t, string(data), "alpha")
	info, err := a.Stat()
	must(t, err)
	want, err := tr.Stat("alias")
	must(t, err)
	assert.Equal(t, info.Name(), want.Name())
	assert.Equal(t, info.Mode(), want.Mode())
	must(t, a.Close())
	_, err = a.Read(buf)
	assert.Equal(t, errors.Is(err, fs.ErrClosed), true)
	_, err = a.Stat()
	assert.Equal(t, errors.Is(err, fs.ErrClosed), true)
}

func TestTree_DirectoryHandle(t *testing.T) {
	tr := fixture(t)
	f, err := tr.Open("dir")
	must(t, err)
	defer f.Close()
	d := f.(fs.ReadDirFile)
	entries, err := d.ReadDir(1)
	must(t, err)
	assert.Equal(t, entries[0].Name(), "a +@日本.yaml")
	entries[0] = nil
	entries, err = d.ReadDir(2)
	must(t, err)
	assert.Equal(t, len(entries), 1)
	assert.Equal(t, entries[0].Name(), "z.yaml")
	_, err = d.ReadDir(1)
	assert.Equal(t, err, io.EOF)
	entries, err = d.ReadDir(-1)
	must(t, err)
	assert.Equal(t, len(entries), 0)
	_, err = f.Read(make([]byte, 1))
	assert.Equal(t, errors.Is(err, fs.ErrInvalid), true)
	must(t, f.Close())
	_, err = d.ReadDir(1)
	assert.Equal(t, errors.Is(err, fs.ErrClosed), true)
}

func TestTree_MethodErrors(t *testing.T) {
	tr := fixture(t)
	methods := []struct {
		name string
		call func(string) error
	}{
		{name: "open", call: func(p string) error {
			f, err := tr.Open(p)
			if err == nil {
				f.Close()
			}
			return err
		}},
		{name: "readfile", call: func(p string) error { _, err := tr.ReadFile(p); return err }},
		{name: "readdir", call: func(p string) error { _, err := tr.ReadDir(p); return err }},
		{name: "stat", call: func(p string) error { _, err := tr.Stat(p); return err }},
		{name: "lstat", call: func(p string) error { _, err := tr.Lstat(p); return err }},
		{name: "readlink", call: func(p string) error { _, err := tr.ReadLink(p); return err }},
	}
	for _, method := range methods {
		t.Run(method.name, func(t *testing.T) {
			for _, p := range []string{"", "/dir", "../dir", "dir/../empty", "dir//z.yaml", "dir/", "dir/\x00"} {
				t.Run(fmt.Sprintf("%q", p), func(t *testing.T) {
					err := method.call(p)
					assert.Equal(t, errors.Is(err, fs.ErrInvalid), true)
					pe, ok := errors.AsType[*fs.PathError](err)
					if !ok {
						t.Fatalf("expected PathError, got %v", err)
					}
					assert.Equal(t, pe.Path, p)
				})
			}
			assert.Equal(t, errors.Is(method.call("missing"), fs.ErrNotExist), true)
		})
	}
	_, err := tr.ReadFile("dir")
	assert.Equal(t, errors.Is(err, fs.ErrInvalid), true)
	_, err = tr.ReadDir("dir/z.yaml")
	assert.Equal(t, errors.Is(err, fs.ErrInvalid), true)
	_, err = tr.ReadLink("dir/z.yaml")
	assert.Equal(t, errors.Is(err, fs.ErrInvalid), true)
}

func TestTree_SymlinkContract(t *testing.T) {
	root := t.TempDir()
	testutil.WriteFile(t, root, "file", "root")
	absoluteTarget := filepath.ToSlash(filepath.Join(root, "file"))
	absoluteTarget = strings.TrimPrefix(absoluteTarget, filepath.ToSlash(filepath.VolumeName(root)))
	b := tree.NewBuilder(nil)
	must(t, b.AddFile("file", []byte("root"), 0o600))
	must(t, b.AddFile("d/file", []byte("parent"), 0o600))
	must(t, b.AddFile("d/sub/file", []byte("child"), 0o600))
	links := []struct{ name, target string }{
		{name: "alias", target: "d/sub"},
		{name: "d/sub/up", target: "../file"},
		{name: "dangling", target: "missing"},
		{name: "empty-target", target: ""},
		{name: "loop", target: "loop"},
		{name: "escape", target: "../file"},
		{name: "absolute-absent", target: "/absent-target"},
		{name: "absolute-in-root", target: absoluteTarget},
		{name: "root-link", target: "."},
		{name: "pair-a", target: "pair-b"},
		{name: "pair-b", target: "pair-a"},
	}
	for _, link := range links {
		must(t, b.AddSymlink(link.name, link.target))
	}
	for i := range 9 {
		target := "file"
		if i < 8 {
			target = fmt.Sprintf("chain-%d", i+1)
		}
		must(t, b.AddSymlink(fmt.Sprintf("chain-%d", i), target))
	}
	tr := build(t, b)
	cases := []struct {
		name string
		want string
		err  error
	}{
		{name: "chain-1", want: "root"},
		{name: "chain-0", err: tree.ErrLoop},
		{name: "dangling", err: fs.ErrNotExist},
		{name: "empty-target", err: fs.ErrNotExist},
		{name: "loop", err: tree.ErrLoop},
		{name: "pair-a", err: tree.ErrLoop},
		{name: "escape", err: tree.ErrEscape},
		{name: "absolute-absent", err: tree.ErrEscape},
		{name: "absolute-in-root", err: tree.ErrEscape},
		{name: "alias/up", want: "parent"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			data, err := tr.ReadFile(tc.name)
			if tc.err == nil {
				must(t, err)
				assert.Equal(t, string(data), tc.want)
				return
			}
			assert.Equal(t, errors.Is(err, tc.err), true)
			_, err = tr.Open(tc.name)
			assert.Equal(t, errors.Is(err, tc.err), true)
			_, err = tr.Stat(tc.name)
			assert.Equal(t, errors.Is(err, tc.err), true)
			_, err = tr.ReadDir(tc.name)
			assert.Equal(t, errors.Is(err, tc.err), true)
		})
	}
	p, err := tr.Resolve("alias/../file")
	must(t, err)
	assert.Equal(t, p, "d/file")
	p, err = tr.Resolve("root-link/d/./sub/../file")
	must(t, err)
	assert.Equal(t, p, "d/file")
	_, err = tr.Resolve("file/../d/file")
	assert.Equal(t, errors.Is(err, fs.ErrInvalid), true)
	_, err = tr.Resolve("alias/../../../file")
	assert.Equal(t, errors.Is(err, tree.ErrEscape), true)
	info, err := tr.Stat("root-link")
	must(t, err)
	assert.Equal(t, info.IsDir(), true)
	info, err = tr.Lstat("alias/up")
	must(t, err)
	assert.Equal(t, info.Mode().Type(), fs.ModeSymlink)
	target, err := tr.ReadLink("alias/up")
	must(t, err)
	assert.Equal(t, target, "../file")
	for _, link := range links {
		info, err := tr.Lstat(link.name)
		must(t, err)
		assert.Equal(t, info.Mode().Type(), fs.ModeSymlink)
		target, err := tr.ReadLink(link.name)
		must(t, err)
		assert.Equal(t, target, link.target)
	}
}

func TestTree_GlobLinkContract(t *testing.T) {
	b := tree.NewBuilder(nil)
	must(t, b.AddFile("file", []byte("data"), 0o600))
	must(t, b.AddSymlink("dangling", "missing"))
	must(t, b.AddSymlink("escape", "../file"))
	must(t, b.AddSymlink("loop", "loop"))
	tr := build(t, b)
	matches, err := tr.Glob("*")
	must(t, err)
	assert.Diff(t, matches, []string{"dangling", "file", "loop"})
	for _, name := range []string{"dangling", "loop"} {
		matches, err := tr.Glob(name)
		must(t, err)
		assert.Diff(t, matches, []string{name})
	}
	_, err = tr.Glob("[")
	assert.Equal(t, errors.Is(err, path.ErrBadPattern), true)
}

func TestTree_WalkRawLinkRoot(t *testing.T) {
	b := tree.NewBuilder(nil)
	must(t, b.AddFile("dir/file", []byte("data"), 0o600))
	must(t, b.AddSymlink("alias", "dir"))
	tr := build(t, b)
	var raw, followed []string
	must(t, tr.WalkRaw("alias", func(p string, _ fs.DirEntry, err error) error {
		raw = append(raw, p)
		return err
	}))
	must(t, fs.WalkDir(tr, "alias", func(p string, _ fs.DirEntry, err error) error {
		followed = append(followed, p)
		return err
	}))
	assert.Diff(t, raw, []string{"alias"})
	assert.Diff(t, followed, []string{"alias", "alias/file"})
}

func TestTree_ConcurrentReaders(t *testing.T) {
	for _, workers := range []int{2, 4} {
		t.Run(fmt.Sprint(workers), func(t *testing.T) {
			tr := fixture(t)
			start := make(chan struct{})
			var wg sync.WaitGroup
			for range workers {
				wg.Go(func() {
					<-start
					for range 32 {
						data, err := fs.ReadFile(tr, "alias")
						if err != nil || string(data) != "alpha" {
							t.Errorf("ReadFile: %q, %v", data, err)
							return
						}
						if err := fstest.TestFS(tr, "empty", "dir/a +@日本.yaml", "alias"); err != nil {
							t.Error(err)
							return
						}
					}
				})
			}
			close(start)
			wg.Wait()
		})
	}
}

func TestTree_ReturnedSlicesAreCallerOwned(t *testing.T) {
	for _, workers := range []int{2, 4} {
		t.Run(fmt.Sprint(workers), func(t *testing.T) {
			tr := fixture(t)
			start := make(chan struct{})
			var wg sync.WaitGroup
			for range workers {
				wg.Go(func() {
					<-start
					for range 32 {
						data, err := tr.ReadFile("alias")
						if err != nil || string(data) != "alpha" {
							t.Errorf("ReadFile: %q, %v", data, err)
							return
						}
						data[0] = '!'
						entries, err := tr.ReadDir("dir")
						if err != nil || len(entries) != 2 || entries[0].Name() != "a +@日本.yaml" {
							t.Errorf("ReadDir: %v, %v", entries, err)
							return
						}
						entries[0] = nil
						matches, err := tr.Glob("dir/*")
						if err != nil || len(matches) != 2 || matches[0] != "dir/a +@日本.yaml" {
							t.Errorf("Glob: %v, %v", matches, err)
							return
						}
						matches[0] = "changed"
						f, err := tr.Open("dir")
						if err != nil {
							t.Error(err)
							return
						}
						entries, err = f.(fs.ReadDirFile).ReadDir(-1)
						f.Close()
						if err != nil || len(entries) != 2 || entries[0].Name() != "a +@日本.yaml" {
							t.Errorf("handle ReadDir: %v, %v", entries, err)
							return
						}
						entries[0] = nil
					}
				})
			}
			close(start)
			wg.Wait()
		})
	}
}
