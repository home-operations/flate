package gittree

import (
	"context"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"testing/fstest"
	"time"

	"github.com/go-git/go-billy/v5/osfs"
	"github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/filemode"
	"github.com/go-git/go-git/v5/plumbing/object"
	"github.com/go-git/go-git/v5/storage/filesystem"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/tree"
)

func mustInit(t *testing.T, dir string) *git.Repository {
	t.Helper()
	r, err := git.PlainInit(dir, false)
	assert.NoError(t, err)
	return r
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

func TestNew_EntryPolicy(t *testing.T) {
	t.Run("identity", func(t *testing.T) {
		repo := mustInit(t, t.TempDir())
		commit := craftedCommit(t, repo, object.TreeEntry{Name: "file", Mode: filemode.Regular})
		backing := repo.Storer.(*filesystem.Storage).Filesystem()
		current := fstest.MapFS{"file": {Data: []byte("dirty")}}
		opts := ViewOptions{Content: struct{ Root string }{"root"}, Mode: Direct, Current: current, Changes: []string{"file"}}
		input, identity, err := New(t.Context(), backing, commit, opts)
		assert.NoError(t, err)
		if identity == "" {
			t.Fatal("committed baseline with current routing has no identity")
		}
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
			repo := mustInit(t, t.TempDir())
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
		for _, live := range []bool{false, true} {
			root := t.TempDir()
			repo := mustInit(t, root)
			commit := craftedCommit(t, repo, object.TreeEntry{Name: "committed", Mode: filemode.Regular})
			testutil.WriteFile(t, root, "old/file", "old")
			testutil.WriteFile(t, root, ".sourceignore", "ignored/\n")
			testutil.WriteFile(t, root, "ignored/file", "hidden")
			local, err := AdmitDirectory(t.Context(), root)
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
		repo := mustInit(t, t.TempDir())
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
		repo := mustInit(t, t.TempDir())
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
		repo := mustInit(t, t.TempDir())
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
		current := fstest.MapFS{"file": {Data: []byte("current")}}
		input, key, err := New(t.Context(), nil, plumbing.ZeroHash, ViewOptions{Mode: Direct, Current: current, Ignore: "file", Sparse: []string{"other"}})
		assert.NoError(t, err)
		assert.Equal(t, key, "")
		current["new"] = &fstest.MapFile{Data: []byte("new")}
		got, key, err := New(t.Context(), nil, plumbing.ZeroHash, ViewOptions{Mode: Direct, Current: input})
		assert.NoError(t, err)
		assert.Equal(t, got == input, true)
		assert.Equal(t, key, "")
		for _, name := range []string{"file", "new"} {
			data, err := fs.ReadFile(got, name)
			assert.NoError(t, err)
			assert.Equal(t, string(data), string(current[name].Data))
		}
	})
	t.Run("direct pinned worktree", func(t *testing.T) {
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
		root := t.TempDir()
		repo := mustInit(t, root)
		commit := craftedCommit(t, repo, object.TreeEntry{Name: "new", Mode: filemode.Regular})
		testutil.WriteFile(t, root, "old", "old")
		local, err := AdmitDirectory(t.Context(), root)
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
		repo := mustInit(t, t.TempDir())
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
		root := t.TempDir()
		repo := mustInit(t, root)
		for _, name := range []string{"target", ".rootfile", ".hidden/file", "node_modules/file", "dir/file"} {
			testutil.WriteFile(t, root, name, name)
		}
		for name, target := range map[string]string{
			"alias": ".hidden/link", ".hidden/link": "../target", "escape": "../outside", "missing": "absent", "directory": "dir",
		} {
			assert.NoError(t, os.Symlink(target, filepath.Join(root, name)))
		}
		wt, err := repo.Worktree()
		assert.NoError(t, err)
		assert.NoError(t, wt.AddWithOptions(&git.AddOptions{All: true}))
		commit, err := wt.Commit("fixture", &git.CommitOptions{Author: &object.Signature{Name: "t", Email: "t@e", When: time.Unix(0, 0)}})
		assert.NoError(t, err)
		for _, tc := range []struct {
			name  string
			opts  ViewOptions
			files []string
		}{
			{"source", ViewOptions{Mode: Source, Sparse: []string{"target", ".root"}, Ignore: "target/\n"}, []string{".rootfile", "target"}},
			{"direct", ViewOptions{Mode: Direct}, []string{".hidden/file", ".rootfile", "dir/file", "node_modules/file", "target"}},
			{"remote", ViewOptions{Mode: RemoteBase}, []string{".rootfile", "alias", "dir/file", "target"}},
		} {
			t.Run(tc.name, func(t *testing.T) {
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
					_, err := fs.Stat(input, name)
					if !errors.Is(err, fs.ErrNotExist) {
						t.Fatalf("omitted %s: %v", name, err)
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
	t.Run("unknown mode visibility", func(t *testing.T) {
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
		for _, tc := range []struct {
			name    string
			missing bool
			want    error
		}{
			{"loop", false, tree.ErrLoop}, {"read", true, plumbing.ErrObjectNotFound},
		} {
			t.Run(tc.name, func(t *testing.T) {
				repo := mustInit(t, t.TempDir())
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
		repo := mustInit(t, t.TempDir())
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
		for _, want := range []error{fs.ErrPermission, fs.ErrInvalid} {
			_, _, err := New(t.Context(), nil, plumbing.ZeroHash, ViewOptions{Mode: Source, Current: admissionErrorFS{err: want}})
			assert.Equal(t, errors.Is(err, want), true)
			assert.Equal(t, strings.HasPrefix(err.Error(), "sourceignore load: "), want == fs.ErrPermission)
		}
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		_, _, err := New(ctx, nil, plumbing.ZeroHash, ViewOptions{Mode: Source, Current: admissionErrorFS{err: fs.ErrPermission}})
		assert.Equal(t, err, context.Canceled)
	})
	t.Run("local permission diagnostic", func(t *testing.T) {
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
		local, err := AdmitDirectory(t.Context(), root)
		assert.NoError(t, err)
		_, _, err = New(t.Context(), nil, plumbing.ZeroHash, ViewOptions{Mode: Source, Current: local.FS})
		assert.Equal(t, errors.Is(err, fs.ErrPermission), true)
		pathErr, ok := errors.AsType[*fs.PathError](err)
		if !ok || pathErr.Path != ignore {
			t.Fatalf("permission diagnostic = %v; want %s", err, ignore)
		}
	})

}
