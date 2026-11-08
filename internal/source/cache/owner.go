package cache

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"sync/atomic"
	"weak"

	"github.com/gofrs/flock"
	"github.com/opencontainers/go-digest"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"oras.land/oras-go/v2/content/oci"

	"github.com/home-operations/flate/internal/keylock"
)

type storeSnapshot struct {
	store     *oci.Store
	index     []byte
	layout    []byte
	failed    atomic.Bool
	histories map[string]*historyRecord
}

type owner struct {
	cache *Cache
	lock  *flock.Flock
	ready chan struct{}
	refs  int
	err   error
}

//nolint:gochecknoglobals // Cache owners for the same directory must share one process-wide registry.
var owners = struct {
	mu sync.Mutex

	stores  map[string]*owner
	indexes map[string]weak.Pointer[storeSnapshot]
}{stores: make(map[string]*owner), indexes: make(map[string]weak.Pointer[storeSnapshot])}

// New selects backing once. An empty root allocates no files or directories.
func New(ctx context.Context, root string) (*Cache, error) {
	if root == "" {
		return &Cache{locks: keylock.New[string]()}, nil
	}
	disk, err := openCacheRoot(root)
	if err != nil {
		return nil, err
	}
	root = disk.Name()
	owners.mu.Lock()
	if o := owners.stores[root]; o != nil {
		o.refs++
		owners.mu.Unlock()
		_ = disk.Close()
		select {
		case <-ctx.Done():
			owners.mu.Lock()
			o.refs--
			owners.mu.Unlock()
			return nil, ctx.Err() //nolint:wrapcheck // Cancellation already identifies its cause; another operation prefix would add no resource information.
		case <-o.ready:
			return o.cache, o.err
		}
	}
	o := &owner{
		ready: make(chan struct{}),
		refs:  1,
		lock:  flock.New(filepath.Join(disk.Name(), ociLock), flock.SetPermissions(privateFileMode)),
	}
	owners.stores[root] = o
	snapshot := owners.indexes[root].Value()
	owners.mu.Unlock()
	c, err := o.open(ctx, disk, snapshot)
	owners.mu.Lock()
	if err != nil {
		o.err = &CacheError{err}
		delete(owners.stores, root)
	} else {
		o.cache = c
	}
	close(o.ready)
	owners.mu.Unlock()
	return o.cache, o.err
}

func (o *owner) open(ctx context.Context, disk *os.Root, snapshot *storeSnapshot) (*Cache, error) {
	locked, err := o.lock.TryLock()
	if err == nil && !locked {
		slog.Info("cache: waiting for store lock", "root", disk.Name())
		locked, err = o.lock.TryLockContext(ctx, storeLockInterval)
	}
	if err == nil && !locked {
		err = ctx.Err()
		if err == nil {
			err = errors.New("store lock not acquired")
		}
	}
	var store *oci.Store
	if err == nil {
		store = snapshot.reopen(disk)
		if store == nil {
			store, err = openStore(ctx, disk)
			snapshot = nil
		}
	}
	c := &Cache{root: disk, store: store, locks: keylock.New[string](), persistBlob: processIdentity != ""}
	c.snapshotState.store = store
	c.snapshot.Store(snapshot)
	if err != nil {
		err = errors.Join(err, c.closeRoots(), o.lock.Close())
	}
	return c, err
}

func (s *storeSnapshot) reopen(disk *os.Root) *oci.Store {
	if s == nil || s.index == nil || s.failed.Load() {
		return nil
	}
	data, readErr := disk.ReadFile(indexPath())
	layout, layoutErr := disk.ReadFile(filepath.Join(ociDir, ocispec.ImageLayoutFile))
	if readErr == nil && layoutErr == nil && bytes.Equal(data, s.index) && bytes.Equal(layout, s.layout) {
		return s.store
	}
	return nil
}

// Directory returns the resolved disk boundary, or empty for a memory owner.
func (c *Cache) Directory() string {
	if c == nil || c.root == nil {
		return ""
	}
	return c.root.Name()
}

func openCacheRoot(root string) (*os.Root, error) {
	root, err := filepath.Abs(root)
	if err != nil {
		return nil, &CacheError{err}
	}
	resolved, err := filepath.EvalSymlinks(root)
	if errors.Is(err, os.ErrNotExist) {
		if mkdirAllErr := os.MkdirAll(root, cacheDirectoryMode); mkdirAllErr != nil {
			return nil, &CacheError{mkdirAllErr}
		}
		resolved, err = filepath.EvalSymlinks(root)
	}
	if err != nil {
		return nil, &CacheError{err}
	}
	root = resolved
	disk, err := os.OpenRoot(root)
	if err != nil {
		return nil, &CacheError{err}
	}
	return disk, nil
}

//nolint:gochecknoglobals // This immutable algorithm table fixes the ordering of the rooted blob stores.
var blobAlgorithms = [...]digest.Algorithm{digest.SHA256, digest.SHA384, digest.SHA512}

func (c *Cache) closeRoots() error {
	var err error
	for i := range c.blobRoots {
		if root := &c.blobRoots[i]; root.Root != nil {
			err = errors.Join(err, root.Close())
		}
	}
	return errors.Join(err, c.root.Close())
}

func openStore(ctx context.Context, root *os.Root) (*oci.Store, error) {
	store, err := oci.NewWithContext(ctx, filepath.Join(root.Name(), ociDir))
	if err == nil || ctx.Err() != nil {
		return store, err //nolint:wrapcheck // The cache owner classifies store-open errors once and preserves the original ORAS recovery failure.
	}
	data, readErr := root.ReadFile(indexPath())
	if errors.Is(readErr, os.ErrNotExist) || readErr == nil && json.Unmarshal(data, new(ocispec.Index)) == nil {
		return nil, err //nolint:wrapcheck // The cache owner classifies store-open errors once and preserves the original ORAS recovery failure.
	}
	if removeErr := root.Remove(indexPath()); removeErr != nil {
		return nil, errors.Join(err, removeErr)
	}
	slog.Debug("cache: rebuilding unreadable store index", "path", indexPath(), "root", root.Name(), "error", err)
	//nolint:wrapcheck // Preserve ORAS recovery error identity for cache classification.
	return oci.NewWithContext(
		ctx,
		filepath.Join(root.Name(), ociDir),
	)
}

// Close releases this owner's share. It MUST follow all acquired readers.
func (c *Cache) Close() error {
	if c.store == nil {
		c.entries.Clear()
		return nil
	}
	owners.mu.Lock()
	defer owners.mu.Unlock()
	o := owners.stores[c.root.Name()]
	if o == nil || o.cache != c {
		return nil
	}
	o.refs--
	if o.refs != 0 {
		return nil
	}
	delete(owners.stores, c.root.Name())
	if snapshot := c.snapshot.Load(); snapshot != nil && c.snapshotDirty.Load() {
		c.saveSnapshot(snapshot)
	}

	c.entries.Clear()
	if err := errors.Join(o.lock.Close(), c.closeRoots()); err != nil {
		return &CacheError{err}
	}
	return nil
}

func (c *Cache) saveSnapshot(snapshot *storeSnapshot) {
	if data, err := os.ReadFile(filepath.Join(c.root.Name(), indexPath())); err == nil {
		snapshot.layout, _ = os.ReadFile(filepath.Join(c.root.Name(), ociDir, ocispec.ImageLayoutFile))
		snapshot.index = data
		c.entries.Range(func(_, value any) bool {
			entry := value.(*entry) //nolint:errcheck // The owner stores this exact concrete type; a mismatched assertion must retain its panic.
			if entry.inventoryKey != "" {
				if snapshot.histories == nil {
					snapshot.histories = map[string]*historyRecord{}
				}
				snapshot.histories[entry.inventoryKey] = entry.history
			}
			return true
		})
	} else {
		snapshot.index = nil
	}
	c.retainSnapshot(snapshot)
}

func (c *Cache) retainSnapshot(snapshot *storeSnapshot) {
	root := c.root.Name()
	if owners.indexes[root].Value() != snapshot {
		pointer := weak.Make(snapshot)
		owners.indexes[root] = pointer
		runtime.AddCleanup(snapshot, func(root string) {
			owners.mu.Lock()
			if owners.indexes[root] == pointer {
				delete(owners.indexes, root)
			}
			owners.mu.Unlock()
		}, root)
	}
}

func (c *Cache) prepareSnapshot() {
	if c.store == nil || c.snapshot.Load() != nil {
		return
	}
	c.snapshot.CompareAndSwap(nil, &c.snapshotState)
}
