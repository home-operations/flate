package cache

import (
	"context"
	"errors"
	"io"
	"io/fs"
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

	"github.com/home-operations/flate/pkg/manifest"
)

// GitKey identifies a Git family, its normalized requested ref and recursion policy.
// Content MUST include the effective credentials used by the producer.
type GitKey struct {
	Content           any
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

type gitProducer struct {
	key GitKey
}

func (p gitProducer) identity() string {
	return manifest.Fingerprint(acquisitionContent{Git: p.key})
}

// Git acquires a private generation through the common freshness and publication path.
// Its acquisition identity is empty on failure.
func (c *Cache) Git(
	ctx context.Context,
	key GitKey,
	policy Policy,
	produce func(context.Context, storage.Storer, billy.Filesystem) (plumbing.Hash, error),
) (*GitGeneration, string, error) {
	producer := gitProducer{key: key}
	e, id, err := c.acquire(
		ctx, new(acquireKey{producer: producer}), policy,
		acquisitionCallbacks{produce: func(ctx context.Context, c *Cache, staged *payload) (*entry, error) {
			return producer.produce(ctx, c, staged, produce)
		}},
	)
	if err != nil {
		return nil, "", err
	}
	return e.git, id, nil
}

func (p gitProducer) produce(
	ctx context.Context,
	c *Cache,
	staged *payload,
	fetch func(context.Context, storage.Storer, billy.Filesystem) (plumbing.Hash, error),
) (*entry, error) {
	e, prepareErr := p.prepareGeneration(c, staged)
	if prepareErr != nil {
		return nil, prepareErr
	}
	g := e.git
	storer := filesystem.NewStorageWithOptions(g.Backing, objectcache.NewObjectLRU(0), filesystem.Options{})
	if err := ctx.Err(); err != nil {
		return nil, errors.Join(err, storer.Close())
	}
	hash, err := fetch(ctx, storer, g.Worktree)
	closeErr := storer.Close()
	if err != nil {
		if closeErr != nil {
			return nil, errors.Join(err, closeErr)
		}
		return nil, newProducerError(err)
	}
	if err := errors.Join(closeErr, ctx.Err()); err != nil {
		return nil, err
	}
	g.Commit = hash
	e.descriptor = ocispec.Descriptor{Annotations: map[string]string{
		prefix + "revision": hash.String(), prefix + "git-ref": p.key.Ref, prefix + "git-generation": g.id,
	}}
	if _, err := io.WriteString(staged.writer, g.id); err != nil {
		return nil, err //nolint:wrapcheck // The cache producer and stream adapters preserve the original error tree for replay and io sentinel handling.
	}
	return e, nil
}

func (p gitProducer) prepareGeneration(c *Cache, staged *payload) (*entry, error) {
	g := &GitGeneration{id: uuid.NewString()}
	e := &entry{git: g}
	if c.store == nil {
		g.Backing = memfs.New()
		if p.key.RecurseSubmodules {
			g.Worktree = memfs.New()
		}
		return e, nil
	}
	staged.discard = func() error {
		if e.stored.Digest == "" {
			return c.root.RemoveAll(generationPath(g.id))
		}
		return nil
	}
	g.Backing = osfs.New(filepath.Join(c.root.Name(), gitObjectsPath(g.id)))
	if err := c.root.MkdirAll(gitObjectsPath(g.id), cacheDirectoryMode); err != nil {
		return nil, err //nolint:wrapcheck // Filesystem adapters retain fs.PathError operation, path, and concrete error identity.
	}
	if !p.key.RecurseSubmodules {
		return e, nil
	}
	g.Worktree = osfs.New(filepath.Join(c.root.Name(), gitWorktreePath(g.id)))
	if err := c.root.MkdirAll(gitWorktreePath(g.id), cacheDirectoryMode); err != nil {
		return nil, err //nolint:wrapcheck // Filesystem adapters retain fs.PathError operation, path, and concrete error identity.
	}
	return e, nil
}

//nolint:gocritic // A value descriptor preserves its snapshot without allocating through the dynamic producer interface.
func (p gitProducer) restore(_ context.Context, c *Cache, desc ocispec.Descriptor, e *entry) error {
	if e.git != nil {
		return nil
	}
	resolved := e.resolved
	e.resolved = time.Time{}
	a := desc.Annotations
	id, revision := a[prefix+"git-generation"], a[prefix+"revision"]
	parsed, err := uuid.Parse(id)
	if err != nil || parsed.String() != id || !plumbing.IsHash(revision) || a[prefix+"git-ref"] != p.key.Ref {
		return nil //nolint:nilerr // This optional cache miss deliberately discards an unavailable prior result.
	}
	g := &GitGeneration{
		id:      id,
		Commit:  plumbing.NewHash(revision),
		Backing: osfs.New(filepath.Join(c.root.Name(), gitObjectsPath(id))),
	}
	if _, statErr := c.root.Stat(gitObjectDataPath(id)); statErr != nil {
		if errors.Is(statErr, fs.ErrNotExist) {
			return nil
		}
		return statErr //nolint:wrapcheck // Filesystem adapters retain fs.PathError operation, path, and concrete error identity.
	}
	if p.key.RecurseSubmodules {
		g.Worktree = osfs.New(filepath.Join(c.root.Name(), gitWorktreePath(id)))
		if _, statErr2 := c.root.Stat(gitWorktreePath(id)); statErr2 != nil {
			if errors.Is(statErr2, fs.ErrNotExist) {
				return nil
			}
			return statErr2 //nolint:wrapcheck // Filesystem adapters retain fs.PathError operation, path, and concrete error identity.
		}
	}
	storer := filesystem.NewStorageWithOptions(g.Backing, objectcache.NewObjectLRU(0), filesystem.Options{})
	commit, err := object.GetCommit(storer, g.Commit)
	if err == nil {
		_, err = commit.Tree()
	}
	closeErr := storer.Close()
	if errors.Is(err, plumbing.ErrObjectNotFound) || errors.Is(err, io.EOF) {
		return closeErr //nolint:wrapcheck // The cache producer and stream adapters preserve the original error tree for replay and io sentinel handling.
	}
	if joinErr := errors.Join(err, closeErr); joinErr != nil {
		return joinErr
	}
	e.git, e.resolved = g, resolved
	return nil
}
