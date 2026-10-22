package testutil

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"

	"github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/filemode"
	"github.com/go-git/go-git/v5/plumbing/format/index"
	"github.com/go-git/go-git/v5/plumbing/object"
	"github.com/go-git/go-git/v5/storage"
	"github.com/go-git/go-git/v5/storage/filesystem"
)

// SparseGit retains the two committed snapshots of the E8 sparse workload.
type SparseGit struct {
	Root              string
	Repo              *git.Repository
	Base, Head, Large plumbing.Hash
}

// NewSparseGit writes exactly files committed files, streaming the unrelated blob.
func NewSparseGit(tb testing.TB, files int, blobBytes int64) SparseGit {
	tb.Helper()
	root := tb.TempDir()
	repo := GitInit(tb, root)
	for _, name := range []string{"app", "sibling"} {
		WriteFile(tb, root, "flux/"+name+".yaml", fmt.Sprintf("apiVersion: kustomize.toolkit.fluxcd.io/v1\nkind: Kustomization\nmetadata: {name: %s, namespace: flux-system}\nspec:\n  interval: 10m\n  path: ./%s\n  sourceRef: {kind: GitRepository, name: cluster}\n", name, name))
		WriteFile(tb, root, name+"/kustomization.yaml", "resources: [input.yaml]\n")
		WriteFile(tb, root, name+"/input.yaml", fmt.Sprintf("apiVersion: v1\nkind: ConfigMap\nmetadata: {name: %s-output}\ndata: {value: before}\n", name))
	}
	for n := range files - 7 {
		WriteFile(tb, root, fmt.Sprintf("disjoint/file-%05d.txt", n), "unconsumed\n")
	}
	WriteFile(tb, root, "unrelated/large.txt", "")
	file, err := os.OpenFile(filepath.Join(root, "unrelated/large.txt"), os.O_RDWR, 0o600) //nolint:gosec // The path is fixed under the test-owned temporary directory.
	if err != nil {
		tb.Fatal(err)
	}
	if err := file.Truncate(blobBytes); err != nil {
		tb.Fatal(err)
	}
	writer, header, err := repo.Storer.(*filesystem.Storage).LazyWriter()
	if err != nil {
		tb.Fatal(err)
	}
	if err := header(plumbing.BlobObject, blobBytes); err != nil {
		tb.Fatal(err)
	}
	hasher := plumbing.NewHasher(plumbing.BlobObject, blobBytes)
	n, err := io.Copy(io.MultiWriter(writer, hasher), file)
	if closeErr := errors.Join(writer.Close(), file.Close()); err != nil || closeErr != nil || n != blobBytes {
		tb.Fatalf("stream blob: bytes=%d want=%d error=%v", n, blobBytes, errors.Join(err, closeErr))
	}
	large := hasher.Sum()
	wt, err := repo.Worktree()
	if err != nil {
		tb.Fatal(err)
	}
	for _, dir := range []string{"flux", "app", "sibling", "disjoint"} {
		if _, err := wt.Add(dir); err != nil {
			tb.Fatal(err)
		}
	}
	idx, err := repo.Storer.Index()
	if err != nil {
		tb.Fatal(err)
	}
	info, err := os.Stat(filepath.Join(root, "unrelated/large.txt"))
	if err != nil {
		tb.Fatal(err)
	}
	idx.Entries = append(idx.Entries, &index.Entry{Name: "unrelated/large.txt", Hash: large, Mode: filemode.Regular, Size: uint32(blobBytes), ModifiedAt: info.ModTime(), CreatedAt: info.ModTime()}) //nolint:gosec // E8 fixes the blob size at 256 MiB or a bounded routine control.
	if err := repo.Storer.SetIndex(idx); err != nil {
		tb.Fatal(err)
	}
	opts := &git.CommitOptions{Author: &object.Signature{Name: "fixture", Email: "fixture@example.test"}}
	base, err := wt.Commit("before", opts)
	if err != nil {
		tb.Fatal(err)
	}
	WriteFile(tb, root, "app/input.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: app-output}\ndata: {value: after!}\n")
	if _, err := wt.Add("app/input.yaml"); err != nil {
		tb.Fatal(err)
	}
	head, err := wt.Commit("after", opts)
	if err != nil {
		tb.Fatal(err)
	}
	for _, hash := range []plumbing.Hash{base, head} {
		commit, err := repo.CommitObject(hash)
		if err != nil {
			tb.Fatal(err)
		}
		tree, err := commit.Tree()
		if err != nil {
			tb.Fatal(err)
		}
		entry, err := tree.FindEntry("unrelated/large.txt")
		if err != nil || entry.Hash != large {
			tb.Fatalf("unchanged blob: %v, %v", entry, err)
		}
		count := 0
		if err := tree.Files().ForEach(func(*object.File) error { count++; return nil }); err != nil {
			tb.Fatal(err)
		}
		if count != files {
			tb.Fatalf("committed files=%d want=%d", count, files)
		}
	}
	return SparseGit{Root: root, Repo: repo, Base: base, Head: head, Large: large}
}

// GitReads separates metadata Reader opens from blob Reader opens in one phase.
type GitReads struct {
	Metadata, Bodies, Large atomic.Int64
	LargeHash               plumbing.Hash
}

// Reset starts a new observation phase after all prior readers have stopped.
func (r *GitReads) Reset() {
	r.Metadata.Store(0)
	r.Bodies.Store(0)
	r.Large.Store(0)
}

// CountingStorer preserves storage behavior while counting object Reader opens.
type CountingStorer struct {
	storage.Storer
	Reads *GitReads
}

// EncodedObject wraps readers without changing their type, identity or bytes.
func (s *CountingStorer) EncodedObject(kind plumbing.ObjectType, hash plumbing.Hash) (plumbing.EncodedObject, error) {
	obj, err := s.Storer.EncodedObject(kind, hash)
	if err != nil {
		return nil, err
	}
	return &countedGitObject{EncodedObject: obj, reads: s.Reads}, nil
}

type countedGitObject struct {
	plumbing.EncodedObject
	reads *GitReads
}

func (o *countedGitObject) Reader() (io.ReadCloser, error) {
	if o.Type() == plumbing.BlobObject {
		o.reads.Bodies.Add(1)
		if o.Hash() == o.reads.LargeHash {
			o.reads.Large.Add(1)
		}
	} else {
		o.reads.Metadata.Add(1)
	}
	return o.EncodedObject.Reader()
}
