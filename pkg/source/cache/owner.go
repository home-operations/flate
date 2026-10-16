package cache

import (
	"context"
	"encoding/json"
	"errors"
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
		return &Cache{locks: keylock.New[string]()}, nil
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
	disk, err := os.OpenRoot(root)
	if err != nil {
		return nil, &CacheError{err}
	}
	root = disk.Name()
	owners.Lock()
	if o := owners.stores[root]; o != nil {
		o.refs++
		owners.Unlock()
		_ = disk.Close()
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
	o := &owner{ready: make(chan struct{}), refs: 1, lock: flock.New(filepath.Join(disk.Name(), ociLock), flock.SetPermissions(0o600))}
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
	if err == nil {
		store, err = openStore(ctx, disk)
	}
	owners.Lock()
	if err != nil {
		err = errors.Join(err, disk.Close())
		o.err = &CacheError{err}
		delete(owners.stores, root)
		_ = o.lock.Close()
	} else {
		o.cache = &Cache{root: disk, store: store, locks: keylock.New[string](), persistBlob: processIdentity != ""}
	}
	close(o.ready)
	owners.Unlock()
	return o.cache, o.err
}

func openStore(ctx context.Context, root *os.Root) (*oci.Store, error) {
	store, err := oci.NewWithContext(ctx, filepath.Join(root.Name(), ociDir))
	if err == nil || ctx.Err() != nil {
		return store, err
	}
	data, readErr := root.ReadFile(indexPath())
	if errors.Is(readErr, os.ErrNotExist) || readErr == nil && json.Unmarshal(data, new(ocispec.Index)) == nil {
		return nil, err
	}
	if removeErr := root.Remove(indexPath()); removeErr != nil {
		return nil, errors.Join(err, removeErr)
	}
	slog.Warn("cache: rebuilding unreadable store index", "path", indexPath(), "root", root.Name(), "error", err)
	return oci.NewWithContext(ctx, filepath.Join(root.Name(), ociDir))
}

// Close releases this owner's share. It MUST follow all acquired readers.
func (c *Cache) Close() error {
	if c.store == nil {
		c.entries.Clear()
		return nil
	}
	owners.Lock()
	defer owners.Unlock()
	o := owners.stores[c.root.Name()]
	if o == nil || o.cache != c {
		return nil
	}
	o.refs--
	if o.refs != 0 {
		return nil
	}
	delete(owners.stores, c.root.Name())
	c.entries.Clear()
	if err := errors.Join(o.lock.Close(), c.root.Close()); err != nil {
		return &CacheError{err}
	}
	return nil
}
