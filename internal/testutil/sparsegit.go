package testutil

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"

	"github.com/go-git/go-git/v6"
	"github.com/go-git/go-git/v6/plumbing"
	"github.com/go-git/go-git/v6/plumbing/filemode"
	formatcfg "github.com/go-git/go-git/v6/plumbing/format/config"
	"github.com/go-git/go-git/v6/plumbing/format/index"
	"github.com/go-git/go-git/v6/plumbing/object"
	"github.com/go-git/go-git/v6/storage"
	"github.com/go-git/go-git/v6/storage/filesystem"
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
	for _, name := range []string{fixtureAppName, "sibling"} {
		WriteFile(
			tb,
			root,
			"flux/"+name+".yaml",
			fmt.Sprintf(
				"apiVersion: kustomize.toolkit.fluxcd.io/v1\nkind: Kustomization\nmetadata: {name: %s, namespace: flux-system}\nspec:\n  interval: 10m\n  path: ./%s\n  sourceRef: {kind: GitRepository, name: cluster}\n",
				name,
				name,
			),
		)
		WriteFile(tb, root, name+"/kustomization.yaml", "resources: [input.yaml]\n")
		WriteFile(
			tb,
			root,
			name+"/input.yaml",
			fmt.Sprintf("apiVersion: v1\nkind: ConfigMap\nmetadata: {name: %s-output}\ndata: {value: before}\n", name),
		)
	}
	for n := range files - 7 {
		WriteFile(tb, root, fmt.Sprintf("disjoint/file-%05d.txt", n), "unconsumed\n")
	}
	WriteFile(tb, root, "unrelated/large.txt", "")
	large := writeSparseBlob(tb, root, repo, blobBytes)
	wt, err := repo.Worktree()
	if err != nil {
		tb.Fatal(err)
	}
	for _, dir := range []string{"flux", fixtureAppName, "sibling", "disjoint"} {
		if _, addErr := wt.Add(dir); addErr != nil {
			tb.Fatal(addErr)
		}
	}
	idx, err := repo.Storer.Index()
	if err != nil {
		tb.Fatal(err)
	}
	info, err := os.Stat(filepath.Join(root, "unrelated", "large.txt"))
	if err != nil {
		tb.Fatal(err)
	}
	idx.Entries = append(
		idx.Entries,
		&index.Entry{
			Name: "unrelated/large.txt",
			Hash: large,
			Mode: filemode.Regular,
			//nolint:gosec // Git index size intentionally retains the low 32 bits of file length.
			Size:       uint32(blobBytes),
			ModifiedAt: info.ModTime(),
			CreatedAt:  info.ModTime(),
		},
	)
	if setIndexErr := repo.Storer.SetIndex(idx); setIndexErr != nil {
		tb.Fatal(setIndexErr)
	}
	opts := &git.CommitOptions{Author: &object.Signature{Name: "fixture", Email: "fixture@example.test"}}
	base, err := wt.Commit("before", opts)
	if err != nil {
		tb.Fatal(err)
	}
	WriteFile(
		tb,
		root,
		"app/input.yaml",
		"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: app-output}\ndata: {value: after!}\n",
	)
	if _, addErr := wt.Add("app/input.yaml"); addErr != nil {
		tb.Fatal(addErr)
	}
	head, err := wt.Commit("after", opts)
	if err != nil {
		tb.Fatal(err)
	}
	fixture := SparseGit{Root: root, Repo: repo, Base: base, Head: head, Large: large}
	fixture.assertSnapshots(tb, files)
	return fixture
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
//
//nolint:ireturn,wrapcheck // The counting adapter preserves the upstream interface and sentinel identity.
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

//nolint:wrapcheck // The counting adapter preserves upstream reader errors unchanged.
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

func (s *SparseGit) assertSnapshots(tb testing.TB, files int) {
	tb.Helper()
	for _, hash := range []plumbing.Hash{s.Base, s.Head} {
		commit, err := s.Repo.CommitObject(hash)
		if err != nil {
			tb.Fatal(err)
		}
		tree, err := commit.Tree()
		if err != nil {
			tb.Fatal(err)
		}
		entry, err := tree.FindEntry("unrelated/large.txt")
		if err != nil || entry.Hash != s.Large {
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
}

func writeSparseBlob(tb testing.TB, root string, repo *git.Repository, blobBytes int64) plumbing.Hash {
	tb.Helper()
	file, err := os.OpenFile(
		filepath.Join(root, "unrelated", "large.txt"),
		os.O_RDWR,
		0o600,
	)
	if err != nil {
		tb.Fatal(err)
	}
	if truncateErr := file.Truncate(blobBytes); truncateErr != nil {
		tb.Fatal(truncateErr)
	}
	writer, header, err := repo.Storer.(*filesystem.Storage).LazyWriter() //nolint:errcheck // GitInit creates filesystem-backed storage.
	if err != nil {
		tb.Fatal(err)
	}
	if errInner := header(plumbing.BlobObject, blobBytes); errInner != nil {
		tb.Fatal(errInner)
	}
	hasher := plumbing.NewHasher(formatcfg.SHA1, plumbing.BlobObject, blobBytes)
	n, err := io.Copy(io.MultiWriter(writer, hasher), file)
	if closeErr := errors.Join(writer.Close(), file.Close()); err != nil || closeErr != nil || n != blobBytes {
		tb.Fatalf("stream blob: bytes=%d want=%d error=%v", n, blobBytes, errors.Join(err, closeErr))
	}
	return hasher.Sum()
}
