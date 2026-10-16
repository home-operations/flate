package cache

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/gofrs/flock"
	"github.com/home-operations/flate/internal/keylock"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"oras.land/oras-go/v2/content/oci"
)

type owner struct {
	cache *Cache
	lock  *flock.Flock
	ready chan struct{}
	refs  int
	err   error
}

var owners = struct {
	sync.Mutex
	stores map[string]*owner
}{stores: make(map[string]*owner)}

// New selects backing once. An empty root allocates no files or directories.
func New(ctx context.Context, root string) (*Cache, error) {
	if root == "" {
		return &Cache{locks: keylock.New[string](), persistBlob: discardBlob}, nil
	}
	root, err := filepath.Abs(root)
	if err != nil {
		return nil, &CacheError{err}
	}
	if err := os.MkdirAll(root, 0o750); err != nil {
		return nil, &CacheError{err}
	}
	root, err = filepath.EvalSymlinks(root)
	if err != nil {
		return nil, &CacheError{err}
	}
	l := layout{Root: root}
	owners.Lock()
	if o := owners.stores[root]; o != nil {
		o.refs++
		owners.Unlock()
		select {
		case <-ctx.Done():
			owners.Lock()
			o.refs--
			owners.Unlock()
			return nil, ctx.Err()
		case <-o.ready:
			return o.cache, o.err
		}
	}
	o := &owner{ready: make(chan struct{}), refs: 1, lock: flock.New(l.OCILock(), flock.SetPermissions(0o600))}
	owners.stores[root] = o
	owners.Unlock()
	locked, err := o.lock.TryLock()
	if err == nil && !locked {
		slog.Info("cache: waiting for store lock", "root", root)
		locked, err = o.lock.TryLockContext(ctx, 100*time.Millisecond)
	}
	if err == nil && !locked {
		err = ctx.Err()
		if err == nil {
			err = errors.New("store lock not acquired")
		}
	}
	var store *oci.Store
	var disk *os.Root
	if err == nil {
		disk, err = os.OpenRoot(root)
	}
	if err == nil {
		store, err = oci.NewWithContext(ctx, l.OCI())
		if err != nil && ctx.Err() == nil {
			index := filepath.Join(l.OCI(), ocispec.ImageIndexFile)
			data, readErr := disk.ReadFile(filepath.Join("oci", ocispec.ImageIndexFile))
			if !errors.Is(readErr, os.ErrNotExist) && (readErr != nil || json.Unmarshal(data, new(ocispec.Index)) != nil) {
				if removeErr := disk.Remove(filepath.Join("oci", ocispec.ImageIndexFile)); removeErr != nil {
					err = errors.Join(err, removeErr)
				} else {
					slog.Warn("cache: rebuilding unreadable store index", "path", index, "error", err)
					store, err = oci.NewWithContext(ctx, l.OCI())
				}
			}
		}
	}
	owners.Lock()
	if err != nil {
		if disk != nil {
			err = errors.Join(err, disk.Close())
		}
		o.err = &CacheError{err}
		delete(owners.stores, root)
		_ = o.lock.Close()
	} else {
		o.cache = &Cache{layout: l, root: disk, store: store, locks: keylock.New[string](), persistBlob: discardBlob}
		if identity := processIdentity; identity != "" {
			o.cache.persistBlob = func(ctx context.Context, id string, policy Policy, produce func(context.Context, io.Writer) (ocispec.Descriptor, error)) (io.ReadCloser, ocispec.Descriptor, string, error) {
				return o.cache.blob(ctx, acquireKey{id: blobID(identity, id), lookupOnly: produce == nil}, policy, produce)
			}
		}
	}
	close(o.ready)
	owners.Unlock()
	return o.cache, o.err
}

// Close releases this owner's share. It MUST follow all acquired readers.
func (c *Cache) Close() error {
	if c.store == nil {
		c.entries.Clear()
		return nil
	}
	owners.Lock()
	defer owners.Unlock()
	o := owners.stores[c.layout.Root]
	if o == nil || o.cache != c {
		return nil
	}
	o.refs--
	if o.refs != 0 {
		return nil
	}
	delete(owners.stores, c.layout.Root)
	c.entries.Clear()
	if err := errors.Join(o.lock.Close(), c.root.Close()); err != nil {
		return &CacheError{err}
	}
	return nil
}
