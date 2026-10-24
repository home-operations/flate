package gittree

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/fstest"
	"time"

	"github.com/go-git/go-billy/v5"
	"github.com/go-git/go-billy/v5/osfs"
	"github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/filemode"
	"github.com/go-git/go-git/v5/plumbing/object"
	"github.com/go-git/go-git/v5/storage/filesystem"
	"github.com/google/go-cmp/cmp"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source/gitrepo"
	"github.com/home-operations/flate/pkg/store"
	"github.com/home-operations/flate/pkg/tree"
)

func admitLocal(t *testing.T, root string) (*store.SourceArtifact, error) {
	t.Helper()
	location, err := gitrepo.Locate(t.Context(), root, root)
	if err != nil {
		return nil, err
	}
	return AdmitDirectory(t.Context(), location)
}

func operation(t *testing.T, input fs.FS, mode ViewMode) (*FS, string, error) {
	t.Helper()
	return New(t.Context(), nil, plumbing.ZeroHash, ViewOptions{Mode: mode, Current: input})
}

func craftedCommit(t *testing.T, repo *git.Repository, entry object.TreeEntry) plumbing.Hash {
	t.Helper()
	blob := repo.Storer.NewEncodedObject()
	blob.SetType(plumbing.BlobObject)
	w, err := blob.Writer()
	assert.NoError(t, err)
	if _, err := w.Write([]byte("malicious")); err != nil {
		t.Fatal(err)
	}
	assert.NoError(t, w.Close())
	entry.Hash, err = repo.Storer.SetEncodedObject(blob)
	assert.NoError(t, err)
	if entry.Mode == filemode.Dir {
		child := repo.Storer.NewEncodedObject()
		assert.NoError(t, (&object.Tree{}).Encode(child))
		entry.Hash, err = repo.Storer.SetEncodedObject(child)
		assert.NoError(t, err)
	}
	tree := &object.Tree{Entries: []object.TreeEntry{entry}}
	encoded := repo.Storer.NewEncodedObject()
	assert.NoError(t, tree.Encode(encoded))
	treeHash, err := repo.Storer.SetEncodedObject(encoded)
	assert.NoError(t, err)
	commit := &object.Commit{TreeHash: treeHash, Author: object.Signature{Name: "t", Email: "t@e", When: time.Unix(0, 0)}}
	encoded = repo.Storer.NewEncodedObject()
	assert.NoError(t, commit.Encode(encoded))
	hash, err := repo.Storer.SetEncodedObject(encoded)
	assert.NoError(t, err)
	return hash
}

type admissionErrorFS struct{ err error }

func (f admissionErrorFS) Open(name string) (fs.File, error) {
	return nil, &fs.PathError{Op: "open", Path: name, Err: f.err}
}

type policyReadFS struct {
	fs.FS
	reads atomic.Int64
}

func (f *policyReadFS) ReadFile(name string) ([]byte, error) {
	if filepath.Base(name) == ".sourceignore" {
		f.reads.Add(1)
	}
	return fs.ReadFile(f.FS, name)
}

func TestNew_EntryPolicy(t *testing.T) {
	t.Parallel()
	t.Run("ownership labels", func(t *testing.T) {
		t.Parallel()
		root := t.TempDir()
		content := struct{ Tree string }{"captured"}
		key := manifest.Fingerprint(content)
		for _, tc := range []struct {
			name      string
			opts      LabelOptions
			want, key string
		}{
			{"baseline", LabelOptions{Root: root, Mode: Direct, Key: "revision"}, filepath.Join(root, ".flate-base", "revision"), "revision"},
			{"local source", LabelOptions{Root: root, Mode: Source, Key: "content"}, filepath.Join(filepath.Dir(root), ".flate-sources", "content"), "content"},
			{"alias", LabelOptions{Root: root, Mode: Source, Content: content, Source: manifest.NamedResource{Namespace: "flux-system", Name: "cluster"}}, filepath.Join(filepath.Dir(root), ".flate-sources", key, "flux-system", "cluster"), key},
		} {
			t.Run(tc.name, func(t *testing.T) {
				t.Parallel()
				label, key := Label(tc.opts)
				assert.Equal(t, label, tc.want)
				assert.Equal(t, key, tc.key)
			})
		}
	})

	t.Run("warning evidence reuse", func(t *testing.T) {
		t.Parallel()
		repo := testutil.GitInit(t, t.TempDir())
		commit := craftedCommit(t, repo, object.TreeEntry{Name: "module", Mode: filemode.Submodule})
		input, _, err := New(t.Context(), repo.Storer.(*filesystem.Storage).Filesystem(), commit, ViewOptions{Content: "pinned", Mode: Direct})
		assert.NoError(t, err)
		if len(input.Warnings) != 1 {
			t.Fatalf("traversal evidence = %+v", input.Warnings)
		}
		for _, mode := range []ViewMode{Direct, Source} {
			view, _, err := New(t.Context(), nil, plumbing.ZeroHash, ViewOptions{Mode: mode, Current: input})
			assert.NoError(t, err)
			var want []manifest.Warning
			if mode == Direct {
				want = input.Warnings
			}
			assert.Diff(t, view.Warnings, want)
		}
	})

	t.Run("local reads", func(t *testing.T) {
		t.Parallel()
		parent := t.TempDir()
		root := filepath.Join(parent, "root")
		testutil.WriteFile(t, root, "regular.txt", "content")
		testutil.WriteFile(t, root, "nested/deeper/regular.txt", "nested content")
		testutil.WriteFile(t, parent, "outside", "outside")
		for name, target := range map[string]string{"file-link": "regular.txt", "dir-link": ".", "dangling": "missing", "escape": "../outside", "broken-escape": "../absent"} {
			assert.Equal(t, os.Symlink(target, filepath.Join(root, name)), nil)
		}
		socket, err := net.Listen("unix", filepath.Join(root, "socket"))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { assert.Equal(t, socket.Close(), nil) })
		artifact, err := admitLocal(t, root)
		if err != nil {
			t.Fatal(err)
		}
		assert.Equal(t, artifact.ContentFingerprint != nil, true)
		input := artifact.FS
		view, pin, err := operation(t, input, Direct)
		assert.Equal(t, pin, "")
		if err != nil {
			t.Fatal(err)
		}
		data, err := fs.ReadFile(view, "nested/deeper/regular.txt")
		assert.NoError(t, err)
		assert.Equal(t, string(data), "nested content")
		info, err := fs.Stat(view, "nested/deeper/regular.txt")
		assert.NoError(t, err)
		assert.Equal(t, info.Mode().IsRegular(), true)
		assert.Equal(t, info.Size(), int64(len("nested content")))
		for _, reader := range []struct {
			name string
			fs   fs.FS
		}{{name: "descriptor", fs: input}, {name: "view", fs: view}} {
			t.Run(reader.name, func(t *testing.T) {
				t.Parallel()
				entries, err := fs.ReadDir(reader.fs, ".")
				if err != nil || len(entries) != 2 || entries[0].Name() != "nested" || entries[1].Name() != "regular.txt" {
					t.Fatalf("admitted entries = %v, %v", entries, err)
				}
				for _, name := range []string{"regular.txt", "file-link", "dir-link/regular.txt", "dangling", "escape", "escape/child", "broken-escape", "broken-escape/child", "socket", "absent"} {
					t.Run(name, func(t *testing.T) {
						t.Parallel()
						var want error
						content := "content"
						if name != "regular.txt" {
							want, content = fs.ErrPermission, ""
							if name == "socket" || name == "absent" {
								want = fs.ErrNotExist
							}
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
	})
	t.Run("local operation observations", func(t *testing.T) {
		t.Parallel()
		t.Run("ignore_snapshot", func(t *testing.T) {
			t.Parallel()
			root := t.TempDir()
			testutil.WriteFile(t, root, ".sourceignore", "root\n")
			testutil.WriteFile(t, root, "nested/.sourceignore", "first\n")
			testutil.WriteFile(t, root, ".git/.sourceignore", "excluded\n")
			artifact, err := admitLocal(t, root)
			assert.NoError(t, err)
			input := artifact.FS
			shaped, pin, err := operation(t, input, Source)
			assert.NoError(t, err)
			assert.Equal(t, pin, "")
			content := shaped.PolicyEvidence()
			want := []struct {
				Path string
				Data []byte
			}{{".sourceignore", []byte("root\n")}, {"nested/.sourceignore", []byte("first\n")}}
			assert.Diff(t, content, any(want))
			testutil.WriteFile(t, root, "nested/.sourceignore", "other\n")
			shaped, pin, err = operation(t, input, Source)
			assert.Equal(t, pin, "")
			retained := shaped.PolicyEvidence()
			assert.NoError(t, err)
			assert.Diff(t, retained, content)
			artifact, err = admitLocal(t, root)
			assert.NoError(t, err)
			input = artifact.FS
			shaped, pin, err = operation(t, input, Source)
			assert.Equal(t, pin, "")
			refreshed := shaped.PolicyEvidence()
			assert.NoError(t, err)
			want[1].Data = []byte("other\n")
			assert.Diff(t, refreshed, any(want))
			assert.NoError(t, os.Rename(filepath.Join(root, "nested"), filepath.Join(root, "renamed")))
			artifact, err = admitLocal(t, root)
			assert.NoError(t, err)
			input = artifact.FS
			shaped, pin, err = operation(t, input, Source)
			assert.Equal(t, pin, "")
			moved := shaped.PolicyEvidence()
			assert.NoError(t, err)
			want[1].Path = "renamed/.sourceignore"
			assert.Diff(t, moved, any(want))
			unfiltered, pin, err := operation(t, input, Direct)
			assert.Equal(t, pin, "")
			assert.NoError(t, err)
			if unfiltered.PolicyEvidence() != nil {
				t.Fatalf("unfiltered policy = %v", unfiltered)
			}
		})
		t.Run("point_lookup_without_listing", func(t *testing.T) {
			t.Parallel()
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
			artifact, err := admitLocal(t, root)
			assert.Equal(t, err, nil)
			input := artifact.FS
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
		artifact, err := admitLocal(t, root)
		if err != nil {
			t.Fatal(err)
		}
		input := artifact.FS
		observed, pin, err := operation(t, input, Direct)
		assert.Equal(t, pin, "")
		if err != nil {
			t.Fatal(err)
		}
		if _, err := fs.ReadFile(observed, "dir/arrived"); !errors.Is(err, fs.ErrNotExist) {
			t.Fatalf("initial missing file = %v", err)
		}
		hidden, pin, err := operation(t, input, Source)
		assert.NoError(t, err)
		snapshot := hidden.PolicyEvidence()
		assert.Equal(t, pin, "")
		assert.Equal(t, snapshot == nil, false)
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
		fresh, pin, err := operation(t, input, Source)
		assert.Equal(t, pin, "")
		assert.Diff(t, fresh.PolicyEvidence(), snapshot)
		assert.Equal(t, err, nil)
		testutil.WriteFile(t, root, ".sourceignore", "/*\n")
		readmitted, err := admitLocal(t, root)
		if err != nil {
			t.Fatal(err)
		}
		ignored, pin, err := operation(t, readmitted.FS, Source)
		assert.Equal(t, pin, "")
		if cmp.Equal(ignored.PolicyEvidence(), snapshot) {
			t.Fatal("re-admission retained changed ignore bytes")
		}
		assert.Equal(t, err, nil)
		plain, pin, err := operation(t, readmitted.FS, Direct)
		assert.Equal(t, pin, "")
		assert.Equal(t, err, nil)
		memory := fstest.MapFS{"kept": {Data: []byte("content")}}
		passthrough, pin, err := operation(t, memory, Direct)
		assert.Equal(t, pin, "")
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
				view, pin, err := operation(t, input, Source)
				assert.Equal(t, pin, "")
				assert.Diff(t, view.PolicyEvidence(), snapshot)
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
			if _, err := fs.Stat(input, name); !errors.Is(err, fs.ErrPermission) {
				t.Fatalf("fresh lookup through directory link: %s: %v", name, err)
			}
		}

	})
	t.Run("caller_owned_listing", func(t *testing.T) {
		t.Parallel()
		for _, name := range []string{"replacement", "reversed"} {
			t.Run(name, func(t *testing.T) {
				t.Parallel()
				root := t.TempDir()
				for name, data := range map[string]string{"a": "a", "child": "content", "z": "longer content"} {
					testutil.WriteFile(t, root, "dir/"+name, data)
				}
				want, err := os.Lstat(filepath.Join(root, "dir/a"))
				assert.NoError(t, err)
				artifact, err := admitLocal(t, root)
				assert.NoError(t, err)
				view, _, err := operation(t, artifact.FS, Direct)
				assert.NoError(t, err)
				entries, err := view.ReadDir("dir")
				assert.NoError(t, err)
				if len(entries) != 3 {
					t.Fatalf("listing length = %d, want 3", len(entries))
				}
				if name == "replacement" {
					entries[0] = entries[2]
				} else {
					slices.Reverse(entries)
				}
				entries, err = view.ReadDir("dir")
				assert.NoError(t, err)
				var names []string
				for _, entry := range entries {
					names = append(names, entry.Name())
				}
				assert.Diff(t, names, []string{"a", "child", "z"})
				info, err := fs.Stat(view, "dir/a")
				assert.NoError(t, err)
				assert.Equal(t, info.Name(), want.Name())
				assert.Equal(t, info.Mode(), want.Mode())
				assert.Equal(t, info.Size(), want.Size())
				assert.Equal(t, info.ModTime().Equal(want.ModTime()), true)
			})
		}
	})
	t.Run("concurrent first listing", func(t *testing.T) {
		t.Parallel()
		root := t.TempDir()
		testutil.WriteFile(t, root, "dir/a", "a")
		testutil.WriteFile(t, root, "dir/child", "content")
		testutil.WriteFile(t, root, "dir/nested/file", "nested")
		assert.NoError(t, os.Symlink("../../absent", filepath.Join(root, "dir/link")))
		want, err := os.Lstat(filepath.Join(root, "dir/child"))
		assert.NoError(t, err)
		artifact, err := admitLocal(t, root)
		assert.NoError(t, err)
		view, _, err := operation(t, artifact.FS, Direct)
		assert.NoError(t, err)
		start := make(chan struct{})
		var ready, wg sync.WaitGroup
		ready.Add(8)
		for range 8 {
			wg.Go(func() {
				ready.Done()
				<-start
				entries, err := view.ReadDir("dir")
				if err != nil {
					t.Error(err)
					return
				}
				if len(entries) != 3 {
					t.Errorf("listing length = %d, want 3", len(entries))
					return
				}
				entries[1] = entries[0]
				entries, err = view.ReadDir("dir")
				if err != nil {
					t.Error(err)
					return
				}
				var names []string
				for _, entry := range entries {
					names = append(names, entry.Name())
				}
				assert.Diff(t, names, []string{"a", "child", "nested"})
				info, err := fs.Stat(view, "dir/child")
				if err != nil {
					t.Error(err)
					return
				}
				assert.Equal(t, info.Name(), want.Name())
				assert.Equal(t, info.Mode(), want.Mode())
				assert.Equal(t, info.Size(), want.Size())
				assert.Equal(t, info.ModTime().Equal(want.ModTime()), true)
				_, err = fs.ReadFile(view, "dir/link/child")
				assert.Equal(t, errors.Is(err, fs.ErrPermission), true)
			})
		}
		ready.Wait()
		close(start)
		wg.Wait()
	})
	t.Run("retained directory listing", func(t *testing.T) {
		t.Parallel()
		root := t.TempDir()
		testutil.WriteFile(t, root, "dir/kept", "kept")
		artifact, err := admitLocal(t, root)
		assert.NoError(t, err)
		observed, _, err := operation(t, artifact.FS, Direct)
		assert.NoError(t, err)
		_, err = fs.ReadDir(observed, "dir")
		assert.NoError(t, err)
		testutil.WriteFile(t, root, "dir/arrived", "arrived")
		_, err = fs.Stat(observed, "dir/arrived")
		assert.Equal(t, errors.Is(err, fs.ErrNotExist), true)
		fresh, pin, err := operation(t, observed, Direct)
		assert.NoError(t, err)
		assert.Equal(t, pin, "")
		data, err := fs.ReadFile(fresh, "dir/arrived")
		assert.NoError(t, err)
		assert.Equal(t, string(data), "arrived")
	})
	t.Run("local metadata", func(t *testing.T) {
		t.Parallel()
		for _, directory := range []bool{false, true} {
			t.Run(fmt.Sprint(directory), func(t *testing.T) {
				t.Parallel()
				root := t.TempDir()
				testutil.WriteFile(t, root, "file", "content")
				for _, prefix := range []string{"", "nested/"} {
					name := prefix + ".git"
					if directory {
						name += "/config"
					}
					testutil.WriteFile(t, root, name, "metadata")
				}
				artifact, err := admitLocal(t, root)
				assert.NoError(t, err)
				input := artifact.FS.(*FS)
				location := input.Location()
				assert.Equal(t, location.ContentRoot, root)
				for _, mode := range []ViewMode{Direct, Source, RemoteBase} {
					view, pin, err := operation(t, input, mode)
					assert.NoError(t, err)
					assert.Equal(t, pin, "")
					assert.Equal(t, view.Location() == location, true)
					for _, reader := range []fs.FS{input, view} {
						for _, name := range []string{".git", ".git/config", "nested/.git", "nested/.git/config"} {
							_, err := fs.Stat(reader, name)
							assert.Equal(t, errors.Is(err, fs.ErrNotExist), true)
							_, err = fs.ReadFile(reader, name)
							assert.Equal(t, errors.Is(err, fs.ErrNotExist), true)
							_, err = reader.Open(name)
							assert.Equal(t, errors.Is(err, fs.ErrNotExist), true)
							_, err = fs.ReadDir(reader, name)
							assert.Equal(t, errors.Is(err, fs.ErrNotExist), true)
						}
						assert.NoError(t, fs.WalkDir(reader, ".", func(name string, entry fs.DirEntry, err error) error {
							assert.NoError(t, err)
							if entry.Name() == ".git" {
								t.Errorf("metadata survived traversal: %s", name)
							}
							return nil
						}))
					}
				}
				assert.NoError(t, os.RemoveAll(filepath.Join(root, ".git")))
				testutil.WriteFile(t, root, ".git", "changed metadata")
				fresh, _, err := operation(t, input, Direct)
				assert.NoError(t, err)
				assert.Equal(t, fresh.Location() == location, true)
			})
		}
	})
	t.Run("policy read once and extras", func(t *testing.T) {
		t.Parallel()
		root := t.TempDir()
		testutil.WriteFile(t, root, ".sourceignore", "discard\n")
		testutil.WriteFile(t, root, "nested/.sourceignore", "hidden\n")
		for _, name := range []string{"discard", "kept", "image.png", "nested/hidden"} {
			testutil.WriteFile(t, root, name, name)
		}
		artifact, err := admitLocal(t, root)
		assert.NoError(t, err)
		input := artifact.FS.(*FS)
		counted := &policyReadFS{FS: input}
		policy, err := input.snapshot.load(counted)
		assert.NoError(t, err)
		reads := counted.reads.Load()
		assert.Equal(t, reads, int64(2))
		assert.Equal(t, policy.Match("discard", false), true)
		assert.Equal(t, policy.Match("nested/hidden", false), true)
		want := []struct{ Path, Data string }{{".sourceignore", "discard\n"}, {"nested/.sourceignore", "hidden\n"}}
		assert.Diff(t, policy.Evidence(), want)
		content := []struct {
			Path string
			Data []byte
		}{{".sourceignore", []byte("discard\n")}, {"nested/.sourceignore", []byte("hidden\n")}}
		testutil.WriteFile(t, root, ".sourceignore", "kept\n")
		shaped, pin, err := New(t.Context(), nil, plumbing.ZeroHash, ViewOptions{Mode: Source, Current: input, Ignore: "kept\n"})
		assert.NoError(t, err)
		assert.Equal(t, pin, "")
		assert.Diff(t, shaped.PolicyEvidence(), any(content))
		_, err = fs.Stat(shaped, "image.png")
		assert.NoError(t, err)
		for _, name := range []string{"discard", "kept", "nested/hidden"} {
			_, err = fs.Stat(shaped, name)
			assert.Equal(t, errors.Is(err, fs.ErrNotExist), true)
		}
		testutil.WriteFile(t, root, ".sourceignore", "image.png\n")
		second, pin, err := New(t.Context(), nil, plumbing.ZeroHash, ViewOptions{Mode: Source, Current: input, Ignore: "image.png\n"})
		assert.NoError(t, err)
		assert.Equal(t, pin, "")
		assert.Diff(t, second.PolicyEvidence(), any(content))
		assert.Diff(t, shaped.PolicyEvidence(), any(content))
		assert.Diff(t, policy.Evidence(), want)
		_, err = fs.Stat(second, "kept")
		assert.NoError(t, err)
		for _, name := range []string{"image.png", "discard", "nested/hidden"} {
			_, err = fs.Stat(second, name)
			assert.Equal(t, errors.Is(err, fs.ErrNotExist), true)
		}
		assert.Equal(t, policy.Match("kept", false), false)
		assert.Equal(t, policy.Match("image.png", false), false)
		_, err = fs.Stat(shaped, "kept")
		assert.Equal(t, errors.Is(err, fs.ErrNotExist), true)
		_, err = fs.Stat(shaped, "image.png")
		assert.NoError(t, err)
		retained, pin, err := operation(t, shaped, Source)
		assert.NoError(t, err)
		assert.Equal(t, pin, "")
		assert.Diff(t, retained.PolicyEvidence(), any(content))
		_, err = fs.Stat(retained, "kept")
		assert.Equal(t, errors.Is(err, fs.ErrNotExist), true)
		_, err = input.snapshot.load(counted)
		assert.NoError(t, err)
		assert.Equal(t, counted.reads.Load(), reads)
	})
	t.Run("policy evidence preserves bytes", func(t *testing.T) {
		t.Parallel()
		root := t.TempDir()
		var keys []string
		for _, data := range []string{"#\xff\n", "#\xfe\n"} {
			testutil.WriteFile(t, root, ".sourceignore", data)
			artifact, err := admitLocal(t, root)
			assert.NoError(t, err)
			input, pin, err := operation(t, artifact.FS, Source)
			assert.NoError(t, err)
			assert.Equal(t, pin, "")
			assert.Diff(t, input.PolicyEvidence(), any([]struct {
				Path string
				Data []byte
			}{{".sourceignore", []byte(data)}}))
			keys = append(keys, manifest.Fingerprint(input.PolicyEvidence()))
		}
		assert.Equal(t, keys[0] != keys[1], true)
	})
	t.Run("concurrent first policy and cancellation", func(t *testing.T) {
		t.Parallel()
		root := t.TempDir()
		testutil.WriteFile(t, root, ".sourceignore", "discard\n")
		testutil.WriteFile(t, root, "discard", "discard")
		testutil.WriteFile(t, root, "kept", "kept")
		artifact, err := admitLocal(t, root)
		assert.NoError(t, err)
		start := make(chan struct{})
		var wg sync.WaitGroup
		for range 2 {
			wg.Go(func() {
				<-start
				input, pin, err := operation(t, artifact.FS, Source)
				if err != nil {
					t.Error(err)
					return
				}
				assert.Equal(t, pin, "")
				assert.Diff(t, input.PolicyEvidence(), any([]struct {
					Path string
					Data []byte
				}{{".sourceignore", []byte("discard\n")}}))
				_, err = fs.Stat(input, "discard")
				assert.Equal(t, errors.Is(err, fs.ErrNotExist), true)
				data, err := fs.ReadFile(input, "kept")
				assert.NoError(t, err)
				assert.Equal(t, string(data), "kept")
			})
		}
		close(start)
		wg.Wait()
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		_, err = AdmitDirectory(ctx, artifact.FS.(*FS).Location())
		assert.Equal(t, err, context.Canceled)
		for _, mode := range []ViewMode{Direct, Source} {
			_, _, err = New(ctx, nil, plumbing.ZeroHash, ViewOptions{Mode: mode, Current: artifact.FS})
			assert.Equal(t, err, context.Canceled)
		}
	})
	t.Run("identity", func(t *testing.T) {
		t.Parallel()
		repo := testutil.GitInit(t, t.TempDir())
		commit := craftedCommit(t, repo, object.TreeEntry{Name: "file", Mode: filemode.Regular})
		backing := repo.Storer.(*filesystem.Storage).Filesystem()
		current := fstest.MapFS{"file": {Data: []byte("dirty")}}
		location := &gitrepo.Location{ContentRoot: "root", Repository: repo}
		opts := ViewOptions{Content: struct{ Root string }{"root"}, Location: location, Mode: Direct, Current: current, Changes: []string{"file"}}
		input, identity, err := New(t.Context(), backing, commit, opts)
		assert.NoError(t, err)
		if identity == "" {
			t.Fatal("committed baseline with current routing has no identity")
		}
		fingerprint, err := ContentFingerprint(t.Context(), input)
		assert.NoError(t, err)
		assert.Equal(t, fingerprint, identity)
		assert.Equal(t, input.Location() == location, true)
		refreshed, _, err := operation(t, input, Direct)
		assert.NoError(t, err)
		assert.Equal(t, refreshed.Location() == location, true)
		data, err := fs.ReadFile(input, "file")
		assert.NoError(t, err)
		assert.Equal(t, string(data), "malicious")
		for _, tc := range []struct {
			name   string
			change func(*ViewOptions)
			equal  bool
		}{
			{"empty_sparse", func(o *ViewOptions) { o.Sparse = []string{} }, true},
			{"routing", func(o *ViewOptions) {
				o.Current = fstest.MapFS{"file": {Data: []byte("other")}}
				o.Changes = []string{"file", "absent"}
			}, true},
			{"coordinates", func(o *ViewOptions) { o.Content = struct{ Root string }{"other"} }, false},
			{"mode", func(o *ViewOptions) { o.Mode = Source }, false},
			{"sparse", func(o *ViewOptions) { o.Sparse = []string{"file"} }, false},
			{"ignore", func(o *ViewOptions) { o.Ignore = "absent\n" }, false},
		} {
			t.Run(tc.name, func(t *testing.T) {
				changed := opts
				tc.change(&changed)
				_, got, err := New(t.Context(), backing, commit, changed)
				assert.NoError(t, err)
				if got == "" || (got == identity) != tc.equal {
					t.Fatalf("identity = %q, base = %q; equal=%t", got, identity, tc.equal)
				}
			})
		}
		other := craftedCommit(t, repo, object.TreeEntry{Name: "other", Mode: filemode.Regular})
		_, changed, err := New(t.Context(), backing, other, opts)
		assert.NoError(t, err)
		if changed == identity {
			t.Fatal("commit change reused an admission identity")
		}
		for _, mode := range []ViewMode{Direct, Source} {
			mutable, key, err := New(t.Context(), nil, plumbing.ZeroHash, ViewOptions{Mode: mode, Current: current})
			assert.NoError(t, err)
			if key != "" {
				t.Fatalf("mutable mode %d has identity %q", mode, key)
			}
			_, key, err = New(t.Context(), nil, plumbing.ZeroHash, ViewOptions{Mode: mode, Current: mutable})
			assert.NoError(t, err)
			if key != "" {
				t.Fatalf("refreshed mutable mode %d has identity %q", mode, key)
			}
		}
	})
	for _, tc := range []struct {
		name    string
		mode    filemode.FileMode
		invalid bool
	}{
		{"a/b", filemode.Regular, true}, {"module", filemode.Submodule, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			repo := testutil.GitInit(t, t.TempDir())
			commit := craftedCommit(t, repo, object.TreeEntry{Name: tc.name, Mode: tc.mode})
			input, identity, err := New(t.Context(), repo.Storer.(*filesystem.Storage).Filesystem(), commit, ViewOptions{})
			if tc.invalid {
				if err == nil || identity != "" {
					t.Fatal("malformed entry accepted or assigned an identity")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if identity == "" {
				t.Fatal("committed view has no identity")
			}
			entries, err := fs.ReadDir(input, ".")
			if err != nil || len(entries) != 0 {
				t.Fatalf("gitlink included: %v, %v", entries, err)
			}
		})
	}

	t.Run("mutable membership and live worktree", func(t *testing.T) {
		t.Parallel()
		for _, live := range []bool{false, true} {
			root := t.TempDir()
			repo := testutil.GitInit(t, root)
			commit := craftedCommit(t, repo, object.TreeEntry{Name: "committed", Mode: filemode.Regular})
			testutil.WriteFile(t, root, "old/file", "old")
			testutil.WriteFile(t, root, ".sourceignore", "ignored/\n")
			testutil.WriteFile(t, root, "ignored/file", "hidden")
			local, err := admitLocal(t, root)
			assert.NoError(t, err)
			opts := ViewOptions{Mode: Source, Current: local.FS}
			if live {
				opts.Current = nil
				opts.Worktree = osfs.New(root, osfs.WithBoundOS())
			}
			backing := repo.Storer.(*filesystem.Storage).Filesystem()
			if !live {
				backing, commit = nil, plumbing.ZeroHash
			}
			view, pin, err := New(t.Context(), backing, commit, opts)
			assert.NoError(t, err)
			assert.Equal(t, pin, "")
			_, missingErr := fs.Stat(view, "new/deep/file")
			assert.Equal(t, errors.Is(missingErr, fs.ErrNotExist), true)
			_, err = fs.ReadDir(view, "old")
			assert.NoError(t, err)
			assert.NoError(t, os.RemoveAll(filepath.Join(root, "old")))
			testutil.WriteFile(t, root, "new/deep/file", "new")
			fresh, pin, err := New(t.Context(), nil, plumbing.ZeroHash, ViewOptions{Mode: Direct, Current: view})
			assert.NoError(t, err)
			assert.Equal(t, pin, "")
			data, err := fs.ReadFile(fresh, "new/deep/file")
			assert.NoError(t, err)
			assert.Equal(t, string(data), "new")
			for _, name := range []string{"old/file", "ignored/file"} {
				_, err := fs.Stat(fresh, name)
				assert.Equal(t, errors.Is(err, fs.ErrNotExist), true)
			}
		}
	})
	t.Run("committed tree overrides current", func(t *testing.T) {
		t.Parallel()
		repo := testutil.GitInit(t, t.TempDir())
		backing := repo.Storer.(*filesystem.Storage).Filesystem()
		currentCommit := craftedCommit(t, repo, object.TreeEntry{Name: "current", Mode: filemode.Regular})
		current, _, err := New(t.Context(), backing, currentCommit, ViewOptions{})
		assert.NoError(t, err)
		target := craftedCommit(t, repo, object.TreeEntry{Name: "target", Mode: filemode.Regular})
		view, _, err := New(t.Context(), backing, target, ViewOptions{Mode: Direct, Current: current, Changes: []string{"target"}})
		assert.NoError(t, err)
		data, err := fs.ReadFile(view, "target")
		assert.NoError(t, err)
		assert.Equal(t, string(data), "malicious")
		_, err = fs.Stat(view, "current")
		assert.Equal(t, errors.Is(err, fs.ErrNotExist), true)
	})
	t.Run("retained identity", func(t *testing.T) {
		t.Parallel()
		repo := testutil.GitInit(t, t.TempDir())
		commit := craftedCommit(t, repo, object.TreeEntry{Name: "file", Mode: filemode.Regular})
		identity := "source-git-acquired@" + commit.String()
		input, pin, err := New(t.Context(), repo.Storer.(*filesystem.Storage).Filesystem(), commit, ViewOptions{Mode: Direct, Content: identity})
		assert.NoError(t, err)
		assert.Equal(t, pin == "", false)
		originalPin := pin
		held, pin, err := New(t.Context(), nil, plumbing.ZeroHash, ViewOptions{Mode: Direct, Current: input})
		assert.NoError(t, err)
		assert.Equal(t, pin, originalPin)
		assert.Equal(t, held == input, true)
		_, pin, err = New(t.Context(), repo.Storer.(*filesystem.Storage).Filesystem(), commit, ViewOptions{Mode: Direct})
		assert.NoError(t, err)
		assert.Equal(t, pin == "", false)
	})
	t.Run("source reuse", func(t *testing.T) {
		t.Parallel()
		repo := testutil.GitInit(t, t.TempDir())
		commit := craftedCommit(t, repo, object.TreeEntry{Name: "file", Mode: filemode.Regular})
		input, pin, err := New(t.Context(), repo.Storer.(*filesystem.Storage).Filesystem(), commit, ViewOptions{Mode: Source})
		assert.NoError(t, err)
		for _, tc := range []struct {
			name   string
			ignore string
			sparse []string
			reuse  bool
		}{
			{name: "empty", reuse: true},
			{name: "ignore", ignore: "file\n"},
			{name: "sparse", sparse: []string{"other"}},
		} {
			t.Run(tc.name, func(t *testing.T) {
				t.Parallel()
				got, key, err := New(t.Context(), nil, plumbing.ZeroHash, ViewOptions{Mode: Source, Current: input, Ignore: tc.ignore, Sparse: tc.sparse})
				assert.NoError(t, err)
				assert.Equal(t, got == input, tc.reuse)
				assert.Equal(t, key == pin, tc.reuse)
				assert.Equal(t, key != "", true)
				_, err = fs.Stat(got, "file")
				assert.Equal(t, err == nil, tc.reuse)
			})
		}
	})
	t.Run("direct arbitrary and wrapper reuse", func(t *testing.T) {
		t.Parallel()
		current := fstest.MapFS{"file": {Data: []byte("current")}, ".git": {Data: []byte("gitfile")}, "nested/.git/config": {Data: []byte("config")}}
		input, key, err := New(t.Context(), nil, plumbing.ZeroHash, ViewOptions{Mode: Direct, Current: current, Ignore: "file", Sparse: []string{"other"}})
		assert.NoError(t, err)
		assert.Equal(t, key, "")
		current["new"] = &fstest.MapFile{Data: []byte("new")}
		got, key, err := New(t.Context(), nil, plumbing.ZeroHash, ViewOptions{Mode: Direct, Current: input})
		assert.NoError(t, err)
		assert.Equal(t, got == input, true)
		assert.Equal(t, key, "")
		for _, name := range []string{"file", "new", ".git", "nested/.git/config"} {
			data, err := fs.ReadFile(got, name)
			assert.NoError(t, err)
			assert.Equal(t, string(data), string(current[name].Data))
		}
	})
	t.Run("direct pinned worktree", func(t *testing.T) {
		t.Parallel()
		root := t.TempDir()
		testutil.WriteFile(t, root, "file", "local")
		input, key, err := New(t.Context(), nil, plumbing.NewHash(strings.Repeat("a", 40)), ViewOptions{Mode: Direct, Worktree: osfs.New(root, osfs.WithBoundOS())})
		assert.NoError(t, err)
		assert.Equal(t, key, "")
		data, err := fs.ReadFile(input, "file")
		assert.NoError(t, err)
		assert.Equal(t, string(data), "local")
		testutil.WriteFile(t, root, "new/deep/file", "new")
		data, err = fs.ReadFile(input, "new/deep/file")
		assert.NoError(t, err)
		assert.Equal(t, string(data), "new")
		fresh, _, err := New(t.Context(), nil, plumbing.ZeroHash, ViewOptions{Mode: Direct, Current: input})
		assert.NoError(t, err)
		assert.Equal(t, fresh != input, true)
	})
	t.Run("refresh before pinned construction", func(t *testing.T) {
		t.Parallel()
		root := t.TempDir()
		repo := testutil.GitInit(t, root)
		commit := craftedCommit(t, repo, object.TreeEntry{Name: "new", Mode: filemode.Regular})
		testutil.WriteFile(t, root, "old", "old")
		local, err := admitLocal(t, root)
		assert.NoError(t, err)
		current, _, err := New(t.Context(), nil, plumbing.ZeroHash, ViewOptions{Mode: Source, Current: local.FS})
		assert.NoError(t, err)
		testutil.WriteFile(t, root, "new", "current")
		input, key, err := New(t.Context(), repo.Storer.(*filesystem.Storage).Filesystem(), commit, ViewOptions{Mode: Direct, Current: current})
		assert.NoError(t, err)
		assert.Equal(t, key != "", true)
		data, err := fs.ReadFile(input, "new")
		assert.NoError(t, err)
		assert.Equal(t, string(data), "current")
	})
	t.Run("refresh snapshots", func(t *testing.T) {
		t.Parallel()
		repo := testutil.GitInit(t, t.TempDir())
		commit := craftedCommit(t, repo, object.TreeEntry{Name: "file", Mode: filemode.Regular})
		opts := ViewOptions{Mode: Source, Current: fstest.MapFS{"file": {Data: []byte("dirty")}}, Sparse: []string{"file"}, Changes: []string{"file"}}
		input, key, err := New(t.Context(), repo.Storer.(*filesystem.Storage).Filesystem(), commit, opts)
		assert.NoError(t, err)
		opts.Sparse[0], opts.Changes[0] = "other", "other"
		fresh, pin, err := New(t.Context(), nil, plumbing.ZeroHash, ViewOptions{Mode: Direct, Current: input})
		assert.NoError(t, err)
		assert.Equal(t, pin, key)
		data, err := fs.ReadFile(fresh, "file")
		assert.NoError(t, err)
		assert.Equal(t, string(data), "malicious")
	})
	t.Run("shaping", func(t *testing.T) {
		t.Parallel()
		root := t.TempDir()
		repo := testutil.GitInit(t, root)
		for _, name := range []string{"target", ".rootfile", ".hidden/file", "node_modules/file", "dir/file", "templates/file", "crds/file"} {
			testutil.WriteFile(t, root, name, name)
		}
		for name, target := range map[string]string{
			"alias": ".hidden/link", ".hidden/link": "../target", "escape": "../outside", "missing": "absent", "directory": "dir",
		} {
			assert.NoError(t, os.Symlink(target, filepath.Join(root, name)))
		}
		commit := testutil.GitCommitAll(t, repo, "fixture", &git.CommitOptions{Author: &object.Signature{Name: "t", Email: "t@e", When: time.Unix(0, 0)}})
		for _, tc := range []struct {
			name  string
			opts  ViewOptions
			files []string
		}{
			{"source", ViewOptions{Mode: Source, Sparse: []string{"target", ".root"}, Ignore: "target/\n"}, []string{".rootfile", "target"}},
			{"source directories", ViewOptions{Mode: Source}, []string{".hidden/file", ".rootfile", "crds/file", "dir/file", "node_modules/file", "target", "templates/file"}},
			{"direct", ViewOptions{Mode: Direct}, []string{".hidden/file", ".rootfile", "crds/file", "dir/file", "node_modules/file", "target", "templates/file"}},
			{"remote", ViewOptions{Mode: RemoteBase}, []string{".rootfile", "alias", "crds/file", "dir/file", "target", "templates/file"}},
		} {
			t.Run(tc.name, func(t *testing.T) {
				t.Parallel()
				input, _, err := New(t.Context(), repo.Storer.(*filesystem.Storage).Filesystem(), commit, tc.opts)
				assert.NoError(t, err)
				var files []string
				assert.NoError(t, fs.WalkDir(input, ".", func(name string, entry fs.DirEntry, err error) error {
					if err != nil {
						return err
					}
					if !entry.IsDir() {
						files = append(files, name)
					}
					return nil
				}))
				assert.Diff(t, files, tc.files)
				omitted := []string{"escape", "missing", "directory"}
				if tc.opts.Mode == RemoteBase {
					omitted = append(omitted, ".hidden", "node_modules")
				} else {
					omitted = append(omitted, "alias", ".hidden/link")
				}
				for _, name := range omitted {
					want := fs.ErrNotExist
					if tc.opts.Mode != RemoteBase {
						want = fs.ErrPermission
					}
					_, err := fs.Stat(input, name)
					if !errors.Is(err, want) {
						t.Fatalf("omitted %s: %v; want %v", name, err, want)
					}
				}
				if tc.opts.Mode == RemoteBase {
					data, err := fs.ReadFile(input, "alias")
					assert.NoError(t, err)
					assert.Equal(t, string(data), "target")
				}
			})
		}
	})
	t.Run("rejected links", func(t *testing.T) {
		t.Parallel()
		root := t.TempDir()
		repo := testutil.GitInit(t, root)
		testutil.WriteFile(t, root, "regular", "content")
		testutil.WriteFile(t, root, "ignored", "ignored")
		testutil.WriteFile(t, root, ".sourceignore", "ignored\n")
		assert.NoError(t, os.Symlink("../absent", filepath.Join(root, "link")))
		commit := testutil.GitCommitAll(t, repo, "fixture", &git.CommitOptions{Author: &object.Signature{Name: "t", Email: "t@e", When: time.Unix(0, 0)}})
		backing := repo.Storer.(*filesystem.Storage).Filesystem()
		local, err := admitLocal(t, root)
		assert.NoError(t, err)
		for _, mode := range []ViewMode{Direct, Source} {
			t.Run(fmt.Sprint(mode), func(t *testing.T) {
				t.Parallel()
				committed, _, err := New(t.Context(), backing, commit, ViewOptions{Mode: mode})
				assert.NoError(t, err)
				mutable, _, err := New(t.Context(), backing, plumbing.ZeroHash, ViewOptions{Mode: mode, Worktree: osfs.New(root)})
				assert.NoError(t, err)
				mounted, _, err := operation(t, local.FS, mode)
				assert.NoError(t, err)
				for _, input := range []struct {
					name string
					fs   *FS
				}{{"committed", committed}, {"mutable", mutable}, {"mounted", mounted}} {
					refreshed, _, err := operation(t, input.fs, Source)
					assert.NoError(t, err)
					for _, view := range []struct {
						name string
						fs   *FS
					}{{"initial", input.fs}, {"refreshed", refreshed}} {
						reader := view.fs
						entries, err := fs.ReadDir(reader, ".")
						assert.NoError(t, err)
						for _, entry := range entries {
							if entry.Name() == "link" {
								t.Fatal("listing exposes rejected link")
							}
						}
						for _, name := range []string{"link", "link/child", "link/nested/child"} {
							t.Run(input.name+"/"+view.name+"/"+name, func(t *testing.T) {
								t.Parallel()
								data, err := fs.ReadFile(reader, name)
								assert.Equal(t, len(data), 0)
								assert.Equal(t, errors.Is(err, fs.ErrPermission), true)
								_, err = fs.Stat(reader, name)
								assert.Equal(t, errors.Is(err, fs.ErrPermission), true)
								target, err := fs.ReadLink(reader, name)
								assert.Equal(t, target, "")
								assert.Equal(t, errors.Is(err, fs.ErrPermission), true)
							})
						}
						_, err = fs.ReadFile(reader, "absent")
						assert.Equal(t, errors.Is(err, fs.ErrNotExist), true)
						if reader == refreshed || mode == Source {
							_, err = fs.ReadFile(reader, "ignored")
							assert.Equal(t, errors.Is(err, fs.ErrNotExist), true)
						}
						data, err := fs.ReadFile(reader, "regular")
						assert.NoError(t, err)
						assert.Equal(t, string(data), "content")
					}
				}
			})
		}
	})
	t.Run("unknown mode visibility", func(t *testing.T) {
		t.Parallel()
		current := fstest.MapFS{
			".rootfile": {Data: []byte("root")}, "target": {Data: []byte("target")},
			".hidden/file": {Data: []byte("hidden")}, "node_modules/file": {Data: []byte("modules")},
		}
		input, _, err := New(t.Context(), nil, plumbing.ZeroHash, ViewOptions{Mode: ViewMode(255), Current: current})
		assert.NoError(t, err)
		entries, err := fs.ReadDir(input, ".")
		assert.NoError(t, err)
		var names []string
		for _, entry := range entries {
			names = append(names, entry.Name())
		}
		assert.Diff(t, names, []string{".rootfile", "target"})
	})
	t.Run("link errors", func(t *testing.T) {
		t.Parallel()
		for _, tc := range []struct {
			name    string
			missing bool
			want    error
		}{
			{"loop", false, tree.ErrLoop}, {"read", true, plumbing.ErrObjectNotFound},
		} {
			t.Run(tc.name, func(t *testing.T) {
				t.Parallel()
				repo := testutil.GitInit(t, t.TempDir())
				commit := craftedCommit(t, repo, object.TreeEntry{Name: "malicious", Mode: filemode.Symlink})
				backing := repo.Storer.(*filesystem.Storage).Filesystem()
				if tc.missing {
					c, err := repo.CommitObject(commit)
					assert.NoError(t, err)
					tr, err := c.Tree()
					assert.NoError(t, err)
					hash := tr.Entries[0].Hash.String()
					assert.NoError(t, backing.Remove("objects/"+hash[:2]+"/"+hash[2:]))
				}
				_, _, err := New(t.Context(), backing, commit, ViewOptions{Mode: RemoteBase})
				assert.Equal(t, errors.Is(err, tc.want), true)
				for _, mode := range []ViewMode{Source, Direct} {
					input, _, err := New(t.Context(), backing, commit, ViewOptions{Mode: mode})
					assert.NoError(t, err)
					entries, err := fs.ReadDir(input, ".")
					assert.NoError(t, err)
					assert.Equal(t, len(entries), 0)
				}
			})
		}
	})
	t.Run("missing ordinary body", func(t *testing.T) {
		t.Parallel()
		repo := testutil.GitInit(t, t.TempDir())
		commit := craftedCommit(t, repo, object.TreeEntry{Name: "file", Mode: filemode.Regular})
		c, err := repo.CommitObject(commit)
		assert.NoError(t, err)
		tr, err := c.Tree()
		assert.NoError(t, err)
		hash := tr.Entries[0].Hash.String()
		backing := repo.Storer.(*filesystem.Storage).Filesystem()
		assert.NoError(t, backing.Remove("objects/"+hash[:2]+"/"+hash[2:]))
		for _, mode := range []ViewMode{Source, Direct, RemoteBase, ViewMode(255)} {
			input, _, err := New(t.Context(), backing, commit, ViewOptions{Mode: mode})
			assert.NoError(t, err)
			entries, err := fs.ReadDir(input, ".")
			assert.NoError(t, err)
			assert.Equal(t, len(entries), 1)
			assert.Equal(t, entries[0].Name(), "file")
			_, err = fs.ReadFile(input, "file")
			assert.Equal(t, errors.Is(err, plumbing.ErrObjectNotFound), true)
		}
	})
	t.Run("error classification", func(t *testing.T) {
		t.Parallel()
		for _, want := range []error{fs.ErrPermission, fs.ErrInvalid} {
			_, _, err := New(t.Context(), nil, plumbing.ZeroHash, ViewOptions{Mode: Source, Current: admissionErrorFS{err: want}})
			assert.Equal(t, errors.Is(err, want), true)
			assert.Equal(t, strings.HasPrefix(err.Error(), "sourceignore load: "), errors.Is(want, fs.ErrPermission))
		}
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		_, _, err := New(ctx, nil, plumbing.ZeroHash, ViewOptions{Mode: Source, Current: admissionErrorFS{err: fs.ErrPermission}})
		assert.Equal(t, err, context.Canceled)
	})
	t.Run("local permission diagnostic", func(t *testing.T) {
		t.Parallel()
		root := t.TempDir()
		testutil.WriteFile(t, root, ".sourceignore", "ignored/\n")
		testutil.WriteFile(t, root, "denied/file", "file")
		ignore, denied := filepath.Join(root, ".sourceignore"), filepath.Join(root, "denied")
		assert.NoError(t, os.Chmod(ignore, 0))
		assert.NoError(t, os.Chmod(denied, 0))
		t.Cleanup(func() {
			assert.NoError(t, os.Chmod(ignore, 0o644))
			assert.NoError(t, os.Chmod(denied, 0o755))
		})
		if _, err := os.ReadFile(ignore); !errors.Is(err, fs.ErrPermission) {
			t.Skipf("filesystem cannot enforce ignore-file permission fixture: %v", err)
		}
		if _, err := os.ReadDir(denied); !errors.Is(err, fs.ErrPermission) {
			t.Skipf("filesystem cannot enforce directory permission fixture: %v", err)
		}
		local, err := admitLocal(t, root)
		assert.NoError(t, err)
		_, _, err = New(t.Context(), nil, plumbing.ZeroHash, ViewOptions{Mode: Source, Current: local.FS})
		assert.Equal(t, errors.Is(err, fs.ErrPermission), true)
		pathErr, ok := errors.AsType[*fs.PathError](err)
		if !ok || pathErr.Path != ignore {
			t.Fatalf("permission diagnostic = %v; want %s", err, ignore)
		}
	})

}

func TestNew_WarningsSurviveFailure(t *testing.T) {
	t.Parallel()
	for _, failure := range []string{"traversal", "shape", "refresh"} {
		t.Run(failure, func(t *testing.T) {
			t.Parallel()
			repo := testutil.GitInit(t, t.TempDir())
			entries := []object.TreeEntry{{Name: "module", Mode: filemode.Submodule, Hash: plumbing.NewHash(strings.Repeat("a", 40))}}
			if failure == "traversal" {
				entries = append(entries, object.TreeEntry{Name: "zmissing", Mode: filemode.Dir, Hash: plumbing.NewHash(strings.Repeat("b", 40))})
			} else {
				encoded := repo.Storer.NewEncodedObject()
				assert.NoError(t, (&object.Tree{}).Encode(encoded))
				hash, err := repo.Storer.SetEncodedObject(encoded)
				assert.NoError(t, err)
				name := ".sourceignore"
				if failure == "refresh" {
					name = "zsubtree"
				}
				entries = append(entries, object.TreeEntry{Name: name, Mode: filemode.Dir, Hash: hash})
			}
			encoded := repo.Storer.NewEncodedObject()
			sort.Sort(object.TreeEntrySorter(entries))
			assert.NoError(t, (&object.Tree{Entries: entries}).Encode(encoded))
			treeHash, err := repo.Storer.SetEncodedObject(encoded)
			assert.NoError(t, err)
			encoded = repo.Storer.NewEncodedObject()
			assert.NoError(t, (&object.Commit{TreeHash: treeHash}).Encode(encoded))
			commit, err := repo.Storer.SetEncodedObject(encoded)
			assert.NoError(t, err)
			opts := ViewOptions{}
			if failure == "refresh" {
				opts = ViewOptions{Mode: Direct, Content: "captured", Current: fstest.MapFS{}}
			}
			input, identity, err := New(t.Context(), repo.Storer.(*filesystem.Storage).Filesystem(), commit, opts)
			if failure == "refresh" {
				assert.NoError(t, err)
				for _, entry := range entries {
					if entry.Name == "zsubtree" {
						oid := entry.Hash.String()
						assert.NoError(t, os.Remove(filepath.Join(repo.Storer.(*filesystem.Storage).Filesystem().Root(), "objects", oid[:2], oid[2:])))
					}
				}
				input, identity, err = New(t.Context(), nil, plumbing.ZeroHash, ViewOptions{Mode: Direct, Current: input})
			}
			if err == nil || identity != "" || input == nil || input.view != nil || len(input.Warnings) != 1 || input.Warnings[0].Category != manifest.WarnSkippedSubmodule || input.Warnings[0].Detail[0] != "module" {
				t.Fatalf("failed admission = %+v, %q, %v", input, identity, err)
			}
			if failure == "traversal" && !errors.Is(err, plumbing.ErrObjectNotFound) {
				t.Fatalf("original error lost: %v", err)
			}
		})
	}
}

type contentReadFS struct {
	fs.FS
	reads      atomic.Int64
	fail       string
	err        error
	cancel     context.CancelFunc
	readCancel context.CancelFunc
	entered    chan struct{}
	release    chan struct{}
	gate       atomic.Bool
}

func (f *contentReadFS) Open(name string) (fs.File, error) {
	if name == f.fail {
		if f.err != nil {
			return nil, f.err
		}
		return nil, fs.ErrPermission
	}
	if name == "file" {
		f.reads.Add(1)
		if f.entered != nil && f.gate.CompareAndSwap(false, true) {
			close(f.entered)
			<-f.release
		}
		if f.readCancel != nil {
			f.readCancel()
		}
	}
	return f.FS.Open(name)
}

func waitForOnceWaiter(t *testing.T, id string) {
	t.Helper()
	for {
		buf := make([]byte, 1<<20)
		n := runtime.Stack(buf, true)
		stack := string(buf[:n])
		for record := range strings.SplitSeq(stack, "\n\n") {
			if strings.HasPrefix(record, "goroutine "+id+" ") && strings.Contains(record, "sync.(*Once).doSlow") {
				return
			}
		}
		runtime.Gosched()
	}
}

func (f *contentReadFS) ReadDir(name string) ([]fs.DirEntry, error) {
	if f.cancel != nil {
		f.cancel()
	}
	return fs.ReadDir(f.FS, name)
}

type fingerprintJoinerContext struct {
	context.Context
	input      *FS
	recomputed atomic.Bool
}

func (c *fingerprintJoinerContext) Err() error {
	err := c.Context.Err()
	if err != nil && c.input.content.Load() != nil {
		c.recomputed.Store(true)
	}
	return err
}

func TestFS_ContentFingerprint(t *testing.T) {
	t.Parallel()
	for _, mode := range []string{"equal", "payload", "name", "ignored", "canceled", "error", "read_error", "walk_error", "scan_canceled", "scan_deadline"} {
		t.Run(mode, func(t *testing.T) {
			t.Parallel()
			var keys [2]string
			for i := range keys {
				files := fstest.MapFS{"file": {Data: []byte("selected")}, ".sourceignore": {Data: []byte("ignored\n")}, "ignored": {Data: []byte("ignored")}}
				if i == 1 {
					switch mode {
					case "payload":
						files["file"].Data = []byte("different")
					case "name":
						files["other"] = files["file"]
						delete(files, "file")
					case "ignored":
						files["ignored"].Data = []byte("different")
					}
				}
				reader := &contentReadFS{FS: files}
				input, key, err := New(t.Context(), nil, plumbing.ZeroHash, ViewOptions{Mode: Direct, Current: reader})
				assert.NoError(t, err)
				assert.Equal(t, key, "")
				assert.Equal(t, reader.reads.Load(), int64(0))
				assert.Equal(t, input.FileDigests() == nil, true)
				ctx, cancel := context.WithCancel(t.Context())
				if mode == "read_error" || mode == "walk_error" || mode == "scan_canceled" || mode == "scan_deadline" {
					_, err := input.SourcePolicy(t.Context())
					assert.NoError(t, err)
					want := fs.ErrPermission
					reader.fail = "file"
					if mode == "walk_error" {
						reader.fail = "."
					}
					if mode == "scan_canceled" {
						reader.fail, reader.cancel, want = "", cancel, context.Canceled
					}
					if mode == "scan_deadline" {
						reader.fail, reader.err, want = ".", context.DeadlineExceeded, context.DeadlineExceeded
					}
					_, err = ContentFingerprint(ctx, input)
					cancel()
					assert.Equal(t, input.FileDigests() == nil, true)
					if !errors.Is(err, want) {
						t.Fatalf("fingerprint error = %v, want %v", err, want)
					}
					reader.fail, reader.cancel, reader.err = "", nil, nil
					if mode == "read_error" || mode == "walk_error" {
						_, err = ContentFingerprint(t.Context(), input)
						assert.Equal(t, errors.Is(err, want), true)
						assert.Equal(t, input.FileDigests() == nil, true)
						continue
					}
				}
				if mode == "canceled" {
					cancel()
					_, err := ContentFingerprint(ctx, input)
					if !errors.Is(err, context.Canceled) {
						t.Fatalf("fingerprint error = %v", err)
					}
				}
				cancel()
				if mode == "error" {
					input.view = &directView{FS: admissionErrorFS{err: fs.ErrPermission}}
					_, err := ContentFingerprint(t.Context(), input)
					assert.Equal(t, input.FileDigests() == nil, true)
					if !errors.Is(err, fs.ErrPermission) {
						t.Fatalf("fingerprint error = %v", err)
					}
					continue
				}
				var wg sync.WaitGroup
				for range 2 {
					wg.Go(func() {
						fingerprint, err := ContentFingerprint(t.Context(), input)
						assert.NoError(t, err)
						if fingerprint == "" {
							t.Error("mutable fingerprint is unknown")
						}
					})
				}
				wg.Wait()
				reads := reader.reads.Load()
				if mode != "name" || i == 0 {
					assert.Equal(t, reads, int64(1))
				}
				for name, file := range files {
					if name != "ignored" {
						assert.Equal(t, input.FileDigests()[name], manifest.Fingerprint(plumbing.ComputeHash(plumbing.BlobObject, file.Data)))
					}
				}
				_, ignored := input.FileDigests()["ignored"]
				assert.Equal(t, ignored, false)
				keys[i], err = ContentFingerprint(t.Context(), input)
				assert.NoError(t, err)
				assert.Equal(t, reader.reads.Load(), reads)
				if mode == "canceled" {
					_, err := ContentFingerprint(ctx, input)
					if !errors.Is(err, context.Canceled) {
						t.Fatalf("cached fingerprint error = %v", err)
					}
				}
			}
			if mode != "error" && mode != "read_error" && mode != "walk_error" {
				assert.Equal(t, keys[0] == keys[1], mode != "payload" && mode != "name")
			}
		})
	}
	t.Run("native_membership", func(t *testing.T) {
		t.Parallel()
		files := fstest.MapFS{
			".sourceignore": {Data: []byte("ignored\n")},
			"file":          {Data: []byte("selected")},
			"nested/other":  {Data: []byte("included")},
			"ignored":       {Data: []byte("ignored")},
		}
		reader := &contentReadFS{FS: files}
		input, _, err := New(t.Context(), nil, plumbing.ZeroHash, ViewOptions{Mode: Source, Current: reader})
		assert.NoError(t, err)
		reader.fail = "."
		key, err := ContentFingerprint(t.Context(), input)
		assert.NoError(t, err)
		assert.Equal(t, key != "", true)
		assert.Equal(t, len(input.FileDigests()), 3)
		for _, name := range []string{".sourceignore", "file", "nested/other"} {
			assert.Equal(t, input.FileDigests()[name], manifest.Fingerprint(plumbing.ComputeHash(plumbing.BlobObject, files[name].Data)))
		}
	})

	t.Run("fresh_admission", func(t *testing.T) {
		t.Parallel()
		root := t.TempDir()
		testutil.WriteFile(t, root, "file", "before")
		first, err := admitLocal(t, root)
		assert.NoError(t, err)
		before, err := first.ContentFingerprint(t.Context(), first.FS)
		assert.NoError(t, err)
		testutil.WriteFile(t, root, "file", "after")
		second, err := admitLocal(t, root)
		assert.NoError(t, err)
		operation := first.FS.(*FS).OperationView()
		assert.Equal(t, operation.FileDigests() == nil, true)
		operationKey, err := ContentFingerprint(t.Context(), operation)
		assert.NoError(t, err)
		assert.Equal(t, operationKey != before, true)

		assert.Equal(t, second.FS.(*FS).FileDigests() == nil, true)
		after, err := second.ContentFingerprint(t.Context(), second.FS)
		assert.NoError(t, err)
		assert.Equal(t, first.Key, "")
		assert.Equal(t, second.Key, "")
		assert.Equal(t, before != after, true)
		assert.Equal(t, second.FS.(*FS).FileDigests()["file"], manifest.Fingerprint(plumbing.ComputeHash(plumbing.BlobObject, []byte("after"))))
		retained, err := first.ContentFingerprint(t.Context(), first.FS)
		assert.NoError(t, err)
		assert.Equal(t, retained, before)
	})

	t.Run("excluded_policy_file", func(t *testing.T) {
		t.Parallel()
		input, _, err := New(t.Context(), nil, plumbing.ZeroHash, ViewOptions{Mode: Source, Current: fstest.MapFS{
			".sourceignore":     {Data: []byte("*.png\nsub/.sourceignore\n")},
			"sub/.sourceignore": {Data: []byte("!*.png\n")},
			"sub/keep.png":      {Data: []byte("selected")},
			"file":              {Data: []byte("retained")},
		}})
		assert.NoError(t, err)
		_, err = fs.Stat(input, "sub/keep.png")
		assert.NoError(t, err)
		_, err = ContentFingerprint(t.Context(), input)
		assert.NoError(t, err)
		_, included := input.FileDigests()["sub/keep.png"]
		assert.Equal(t, included, false)
		assert.Equal(t, input.FileDigests()["file"] != "", true)
	})
	t.Run("content_canceled", func(t *testing.T) {
		t.Parallel()
		reader := &contentReadFS{FS: fstest.MapFS{"file": {Data: []byte("first")}, "later": {Data: []byte("later")}}}
		input, _, err := New(t.Context(), nil, plumbing.ZeroHash, ViewOptions{Mode: Direct, Current: reader})
		assert.NoError(t, err)
		_, err = input.SourcePolicy(t.Context())
		assert.NoError(t, err)
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		reader.readCancel = cancel
		_, err = ContentFingerprint(ctx, input)
		assert.Equal(t, errors.Is(err, context.Canceled), true)
		assert.Equal(t, input.FileDigests() == nil, true)
		reader.readCancel = nil
		failedReads := reader.reads.Load()
		var keys [2]string
		var wg sync.WaitGroup
		for i := range keys {
			wg.Go(func() {
				key, err := ContentFingerprint(t.Context(), input)
				assert.NoError(t, err)
				assert.Equal(t, key != "", true)
				keys[i] = key
			})
		}
		wg.Wait()
		assert.Equal(t, keys[0], keys[1])
		assert.Equal(t, reader.reads.Load(), failedReads+1)
		for name, data := range map[string]string{"file": "first", "later": "later"} {
			assert.Equal(t, input.FileDigests()[name], manifest.Fingerprint(plumbing.ComputeHash(plumbing.BlobObject, []byte(data))))
		}
		reads := reader.reads.Load()
		key, err := ContentFingerprint(t.Context(), input)
		assert.NoError(t, err)
		assert.Equal(t, key, keys[0])
		assert.Equal(t, reader.reads.Load(), reads)
	})
	for _, name := range []string{"live_joiner_retries", "canceled_joiner", "canceled_joiner_live_executor"} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			reader := &contentReadFS{
				FS:      fstest.MapFS{"file": {Data: []byte("first")}, "later": {Data: []byte("later")}},
				entered: make(chan struct{}),
				release: make(chan struct{}),
			}
			input, _, err := New(t.Context(), nil, plumbing.ZeroHash, ViewOptions{Mode: Direct, Current: reader})
			assert.NoError(t, err)
			_, err = input.SourcePolicy(t.Context())
			assert.NoError(t, err)
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			joinerCtx, cancelJoiner := context.WithCancel(t.Context())
			defer cancelJoiner()
			joiner := &fingerprintJoinerContext{Context: joinerCtx, input: input}
			first := make(chan error, 1)
			var firstKey string
			go func() {
				var err error
				firstKey, err = ContentFingerprint(ctx, input)
				first <- err
			}()
			<-reader.entered
			assert.Equal(t, input.FileDigests() == nil, true)
			second := make(chan struct {
				key string
				err error
			}, 1)
			waiterID := make(chan string, 1)
			go func() {
				buf := make([]byte, 64)
				n := runtime.Stack(buf, false)
				_, header, _ := strings.Cut(string(buf[:n]), " ")
				id, _, _ := strings.Cut(header, " ")
				waiterID <- id
				key, err := ContentFingerprint(joiner, input)
				second <- struct {
					key string
					err error
				}{key, err}
			}()
			waitForOnceWaiter(t, <-waiterID)
			if name != "canceled_joiner_live_executor" {
				cancel()
			}
			if name != "live_joiner_retries" {
				cancelJoiner()
			}
			close(reader.release)
			if name == "canceled_joiner_live_executor" {
				assert.NoError(t, <-first)
				assert.Equal(t, firstKey != "", true)
				result := <-second
				assert.Equal(t, errors.Is(result.err, context.Canceled), true)
				assert.Equal(t, result.key, "")
				assert.Equal(t, len(input.FileDigests()), 2)
				reads := reader.reads.Load()
				key, err := ContentFingerprint(t.Context(), input)
				assert.NoError(t, err)
				assert.Equal(t, key, firstKey)
				assert.Equal(t, reader.reads.Load(), reads)
				return
			}
			assert.Equal(t, errors.Is(<-first, context.Canceled), true)
			result := <-second
			assert.Equal(t, joiner.recomputed.Load(), false)
			if name == "canceled_joiner" {
				assert.Equal(t, errors.Is(result.err, context.Canceled), true)
				assert.Equal(t, result.key, "")
				assert.Equal(t, input.FileDigests() == nil, true)
				assert.Equal(t, reader.reads.Load(), int64(1))
				return
			}
			assert.NoError(t, result.err)
			assert.Equal(t, result.key != "", true)
			assert.Equal(t, len(input.FileDigests()), 2)
			for name, data := range map[string]string{"file": "first", "later": "later"} {
				assert.Equal(t, input.FileDigests()[name], manifest.Fingerprint(plumbing.ComputeHash(plumbing.BlobObject, []byte(data))))
			}
			assert.Equal(t, reader.reads.Load(), int64(2))
			key, err := ContentFingerprint(t.Context(), input)
			assert.NoError(t, err)
			assert.Equal(t, key, result.key)
			assert.Equal(t, reader.reads.Load(), int64(2))
		})
	}
}

type contentReadBilly struct {
	billy.Filesystem
	reads atomic.Int64
	all   bool
}

func (f *contentReadBilly) Open(name string) (billy.File, error) {
	if f.all || strings.HasPrefix(filepath.ToSlash(name), "dir/") {
		f.reads.Add(1)
	}
	return f.Filesystem.Open(name)
}

func TestFS_GitContentEvidence(t *testing.T) {
	t.Parallel()
	for _, mode := range []string{"clean", "dirty", "staged", "untracked", "racy", "subtree", "immutable", "sdk", "unreadable_clean", "source_unreadable", "unlocated", "routed", "scoped_local", "scoped_immutable", "direct_immutable"} {
		t.Run(mode, func(t *testing.T) {
			t.Parallel()
			root := t.TempDir()
			repo := testutil.GitInit(t, root)
			testutil.WriteFile(t, root, "dir/file", "alpha")
			testutil.WriteFile(t, root, "dir/ignored", "ignored")
			testutil.WriteFile(t, root, ".sourceignore", "dir/ignored\n")
			old := time.Unix(946684800, 0)
			assert.NoError(t, os.Chtimes(filepath.Join(root, "dir/file"), old, old))
			commit := testutil.GitCommitAll(t, repo, "fixture", &git.CommitOptions{Author: &object.Signature{Name: "t", Email: "t@e", When: old}})
			want := "alpha"
			if mode == "dirty" || mode == "staged" || mode == "racy" || mode == "routed" {
				want = "bravo"
				testutil.WriteFile(t, root, "dir/file", want)
			}
			if mode == "staged" {
				assert.NoError(t, os.Chtimes(filepath.Join(root, "dir/file"), old, old))
				wt, err := repo.Worktree()
				assert.NoError(t, err)
				_, err = wt.Add("dir/file")
				assert.NoError(t, err)
			}
			if mode == "racy" {
				assert.NoError(t, os.Chtimes(filepath.Join(root, "dir/file"), old, old))
				assert.NoError(t, os.Chtimes(filepath.Join(root, ".git/index"), old, old))
			}
			if mode == "untracked" {
				testutil.WriteFile(t, root, "dir/new", "untracked")
			}
			contentRoot, name := root, "dir/file"
			if mode == "subtree" {
				contentRoot, name = filepath.Join(root, "dir"), "file"
			}
			location, err := gitrepo.Locate(t.Context(), contentRoot, contentRoot)
			assert.NoError(t, err)
			reader := &contentReadBilly{Filesystem: osfs.New(root)}
			location.Repository, err = git.Open(repo.Storer, reader)
			assert.NoError(t, err)
			input, err := AdmitDirectory(t.Context(), location)
			assert.NoError(t, err)
			objects := &contentReadBilly{Filesystem: repo.Storer.(*filesystem.Storage).Filesystem(), all: true}
			if mode == "immutable" || mode == "scoped_immutable" || mode == "direct_immutable" {
				viewMode := Source
				if mode == "direct_immutable" {
					viewMode = Direct
				}
				input.FS, _, err = New(t.Context(), objects, commit, ViewOptions{Mode: viewMode})
				assert.NoError(t, err)
			}
			if mode == "sdk" {
				want = "sdkdata"
				input.FS, _, err = New(t.Context(), nil, plumbing.ZeroHash, ViewOptions{Mode: Direct, Location: location, Current: fstest.MapFS{"dir/file": {Data: []byte(want)}}})
				assert.NoError(t, err)
			}
			if mode == "scoped_local" || mode == "scoped_immutable" {
				current, err := Sub(input.FS, "dir")
				assert.NoError(t, err)
				input.FS, _, err = New(t.Context(), nil, plumbing.ZeroHash, ViewOptions{Mode: Source, Current: current})
				assert.NoError(t, err)
				name = "file"
			}
			if mode == "source_unreadable" || mode == "routed" {
				opts := ViewOptions{Mode: Source, Current: input.FS}
				ref := plumbing.ZeroHash
				if mode == "routed" {
					want, ref = "alpha", commit
					opts.Worktree, opts.Changes = reader, []string{"dir/file"}
				}
				input.FS, _, err = New(t.Context(), objects, ref, opts)
				assert.NoError(t, err)
			}
			selected := input.FS.(*FS)
			if mode == "unlocated" {
				selected.location = nil
			}
			_, err = selected.SourcePolicy(t.Context())
			assert.NoError(t, err)
			objects.reads.Store(0)
			if mode == "unreadable_clean" || mode == "source_unreadable" || mode == "subtree" || mode == "scoped_local" {
				file := filepath.Join(root, "dir/file")
				assert.NoError(t, os.Chmod(file, 0))
				t.Cleanup(func() { assert.NoError(t, os.Chmod(file, 0o644)) })
				if _, err := os.ReadFile(file); !errors.Is(err, fs.ErrPermission) {
					t.Skipf("filesystem cannot enforce unreadable file: %v", err)
				}
			}
			assert.Equal(t, selected.FileDigests() == nil, true)
			_, err = ContentFingerprint(t.Context(), selected)
			assert.NoError(t, err)
			assert.Equal(t, selected.FileDigests()[name], manifest.Fingerprint(plumbing.ComputeHash(plumbing.BlobObject, []byte(want))))
			if mode == "untracked" {
				assert.Equal(t, selected.FileDigests()["dir/new"], manifest.Fingerprint(plumbing.ComputeHash(plumbing.BlobObject, []byte("untracked"))))
			}
			if mode != "subtree" {
				_, ignored := selected.FileDigests()["dir/ignored"]
				assert.Equal(t, ignored, false)
			}
			reads := reader.reads.Load()
			assert.Equal(t, objects.reads.Load(), int64(0))
			_, err = ContentFingerprint(t.Context(), selected)
			assert.NoError(t, err)
			assert.Equal(t, reader.reads.Load(), reads)
		})
	}
}
