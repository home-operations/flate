package cache

import (
	"context"
	"errors"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"time"

	"github.com/go-git/go-billy/v5"
	"github.com/go-git/go-billy/v5/memfs"
	"github.com/go-git/go-billy/v5/osfs"
	"github.com/go-git/go-git/v5/plumbing"
	objectcache "github.com/go-git/go-git/v5/plumbing/cache"
	"github.com/go-git/go-git/v5/plumbing/object"
	"github.com/go-git/go-git/v5/storage"
	"github.com/go-git/go-git/v5/storage/filesystem"
	"github.com/google/uuid"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
)

// GitKey identifies a Git family, its normalized requested ref and recursion policy.
type GitKey struct {
	ID                string
	Ref               string
	RecurseSubmodules bool
}

// GitGeneration retains sealed object backing and any upstream recursive checkout.
// Every reader MUST construct its own storer; publication retains no producer handles.
type GitGeneration struct {
	Backing  billy.Filesystem
	Worktree billy.Filesystem
	Commit   plumbing.Hash
	id       string
}

type gitRequest struct {
	key     GitKey
	produce func(storage.Storer, billy.Filesystem) (plumbing.Hash, error)
}

// Git acquires a private generation through the common freshness and publication path.
func (c *Cache) Git(ctx context.Context, key GitKey, maxAge time.Duration, produce func(storage.Storer, billy.Filesystem) (plumbing.Hash, error)) (*GitGeneration, error) {
	e, err := c.acquire(ctx, acquireKey{ID: key.ID, git: &gitRequest{key, produce}}, maxAge, nil)
	if err != nil {
		return nil, err
	}
	return e.git, nil
}

func (c *Cache) produceGit(ctx context.Context, request *gitRequest) (*GitGeneration, ocispec.Descriptor, error) {
	g := &GitGeneration{id: uuid.NewString()}
	complete := false
	if c.store == nil {
		g.Backing = memfs.New()
		if request.key.RecurseSubmodules {
			g.Worktree = memfs.New()
		}
	} else {
		defer func() {
			if !complete {
				_ = os.RemoveAll(filepath.Dir(c.layout.GitObjects(g.id)))
			}
		}()
		g.Backing = osfs.New(c.layout.GitObjects(g.id))
		if err := g.Backing.MkdirAll(".", 0o750); err != nil {
			return nil, ocispec.Descriptor{}, err
		}
		if request.key.RecurseSubmodules {
			g.Worktree = osfs.New(c.layout.GitWorktree(g.id))
			if err := g.Worktree.MkdirAll(".", 0o750); err != nil {
				return nil, ocispec.Descriptor{}, err
			}
		}
	}
	storer := filesystem.NewStorageWithOptions(g.Backing, objectcache.NewObjectLRU(0), filesystem.Options{})
	hash, err := request.produce(storer, g.Worktree)
	closeErr := storer.Close()
	if err != nil {
		return nil, ocispec.Descriptor{}, &producerError{errors.Join(err, closeErr)}
	}
	if err := errors.Join(closeErr, ctx.Err()); err != nil {
		return nil, ocispec.Descriptor{}, err
	}
	complete = true
	g.Commit = hash
	return g, ocispec.Descriptor{Annotations: map[string]string{
		prefix + "revision": hash.String(), prefix + "git-ref": request.key.Ref, prefix + "git-generation": g.id,
	}}, nil
}

func (c *Cache) restoreGit(desc ocispec.Descriptor, key GitKey) (*GitGeneration, error) {
	a := desc.Annotations
	id, revision := a[prefix+"git-generation"], a[prefix+"revision"]
	parsed, err := uuid.Parse(id)
	if err != nil || parsed.String() != id || !plumbing.IsHash(revision) || a[prefix+"git-ref"] != key.Ref {
		return nil, nil
	}
	g := &GitGeneration{id: id, Commit: plumbing.NewHash(revision), Backing: osfs.New(c.layout.GitObjects(id))}
	_, err = time.Parse(time.RFC3339Nano, a[prefix+"resolved-at"])
	if err != nil {
		return nil, nil
	}
	if _, err := g.Backing.Stat("objects"); err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil, nil
		}
		return nil, err
	}
	if key.RecurseSubmodules {
		g.Worktree = osfs.New(c.layout.GitWorktree(id))
		if _, err := g.Worktree.Stat("."); err != nil {
			if errors.Is(err, fs.ErrNotExist) {
				return nil, nil
			}
			return nil, err
		}
	}
	storer := filesystem.NewStorageWithOptions(g.Backing, objectcache.NewObjectLRU(0), filesystem.Options{})
	commit, err := object.GetCommit(storer, g.Commit)
	if err == nil {
		_, err = commit.Tree()
	}
	closeErr := storer.Close()
	if errors.Is(err, plumbing.ErrObjectNotFound) || errors.Is(err, io.EOF) {
		return nil, closeErr
	}
	if err := errors.Join(err, closeErr); err != nil {
		return nil, err
	}
	return g, nil
}
