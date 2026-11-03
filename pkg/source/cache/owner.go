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
	"time"
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

var owners = struct {
	sync.Mutex
	stores  map[string]*owner
	indexes map[string]weak.Pointer[storeSnapshot]
}{stores: make(map[string]*owner), indexes: make(map[string]weak.Pointer[storeSnapshot])}

// New selects backing once. An empty root allocates no files or directories.
func New(ctx context.Context, root string) (*Cache, error) {
	if root == "" {
		return &Cache{locks: keylock.New[string]()}, nil
	}
	root, err := filepath.Abs(root)
	if err != nil {
		return nil, &CacheError{err}
	}
	resolved, err := filepath.EvalSymlinks(root)
	if errors.Is(err, os.ErrNotExist) {
		if err := os.MkdirAll(root, 0o750); err != nil {
			return nil, &CacheError{err}
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
	o := &owner{
		ready: make(chan struct{}),
		refs:  1,
		lock:  flock.New(filepath.Join(disk.Name(), ociLock), flock.SetPermissions(0o600)),
	}
	owners.stores[root] = o
	snapshot := owners.indexes[root].Value()
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
		if snapshot != nil && snapshot.index != nil && !snapshot.failed.Load() {
			data, readErr := disk.ReadFile(indexPath())
			layout, layoutErr := disk.ReadFile(filepath.Join(ociDir, ocispec.ImageLayoutFile))
			if readErr == nil && layoutErr == nil && bytes.Equal(data, snapshot.index) &&
				bytes.Equal(layout, snapshot.layout) {
				store = snapshot.store
			}
		}
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
	owners.Lock()
	if err != nil {
		o.err = &CacheError{err}
		delete(owners.stores, root)
	} else {
		o.cache = c
	}
	close(o.ready)
	owners.Unlock()
	return o.cache, o.err
}

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
		return store, err
	}
	data, readErr := root.ReadFile(indexPath())
	if errors.Is(readErr, os.ErrNotExist) || readErr == nil && json.Unmarshal(data, new(ocispec.Index)) == nil {
		return nil, err
	}
	if removeErr := root.Remove(indexPath()); removeErr != nil {
		return nil, errors.Join(err, removeErr)
	}
	slog.Debug("cache: rebuilding unreadable store index", "path", indexPath(), "root", root.Name(), "error", err)
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
	if snapshot := c.snapshot.Load(); snapshot != nil && c.snapshotDirty.Load() {
		if data, err := os.ReadFile(filepath.Join(c.root.Name(), indexPath())); err == nil {
			snapshot.layout, _ = os.ReadFile(filepath.Join(c.root.Name(), ociDir, ocispec.ImageLayoutFile))
			snapshot.index = data
			c.entries.Range(func(_, value any) bool {
				entry := value.(*entry)
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
		root := c.root.Name()
		if owners.indexes[root].Value() != snapshot {
			pointer := weak.Make(snapshot)
			owners.indexes[root] = pointer
			runtime.AddCleanup(snapshot, func(root string) {
				owners.Lock()
				if owners.indexes[root] == pointer {
					delete(owners.indexes, root)
				}
				owners.Unlock()
			}, root)
		}
	}

	c.entries.Clear()
	if err := errors.Join(o.lock.Close(), c.closeRoots()); err != nil {
		return &CacheError{err}
	}
	return nil
}

func (c *Cache) prepareSnapshot() {
	if c.store == nil || c.snapshot.Load() != nil {
		return
	}
	c.snapshot.CompareAndSwap(nil, &c.snapshotState)
}
