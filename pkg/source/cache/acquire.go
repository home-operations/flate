// Package cache acquires verified source bytes into immutable memory or OCI-backed filesystems.
package cache

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"maps"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/home-operations/flate/internal/keylock"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source/safepath"
	"github.com/opencontainers/go-digest"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"oras.land/oras-go/v2/content/oci"
	"oras.land/oras-go/v2/errdef"
)

const prefix = "flate."

const maxTrailingBytes = 1 << 20

const accessInterval = time.Second

// ArchiveKey identifies an archive and its source-ignore policy.
// Content MUST include the effective credentials used by the producer.
type ArchiveKey struct {
	// Content describes the producer inputs.
	Content      any
	Ignore       string
	WithDefaults bool
	// Required lists unprefixed descriptor annotation names; it does not affect identity.
	Required []string
}

// BlobKey identifies single-file content.
// Content MUST include the effective credentials used by the producer.
type BlobKey struct {
	// Content describes the producer inputs, or a render identity with PersistOnly.
	Content any
	// PersistOnly records only with identified disk backing. Otherwise lookup misses and
	// publication returns a closeable empty reader without invoking the producer.
	PersistOnly bool
	// Required lists unprefixed descriptor annotation names; it does not affect identity.
	Required []string
}

type blobRoot struct {
	*os.Root
	once sync.Once
	path string
	info fs.FileInfo
	err  error
}

// Cache owns acquired generations until Close. Readers MUST finish before Close.
type Cache struct {
	root        *os.Root
	blobRoots   [len(blobAlgorithms)]blobRoot
	store       *oci.Store
	locks       *keylock.KeyMap[string]
	entries     sync.Map
	persistBlob bool
}

type entry struct {
	fs         fs.FS
	descriptor ocispec.Descriptor
	stored     ocispec.Descriptor
	resolved   time.Time
	touched    time.Time
	git        *GitGeneration
	outcome    *outcome
	unloaded   bool
	waveResult *entry
	waveErr    error
}

// CacheError identifies a cache operation failure, including failed persistence.
type CacheError struct{ Err error } //nolint:revive // The public acquisition contract names CacheError.

func (e *CacheError) Error() string   { return "cache: " + e.Err.Error() }
func (e *CacheError) Unwrap() []error { return []error{manifest.ErrFlux, e.Err} }

type acquireKey struct {
	producer   acquisitionProducer
	id         string
	required   []string
	lookupOnly bool
}

type producerError struct{ error }

// readerError marks a failure of the archive consumer that the producer only observes as a failed write.
type readerError struct{ error }

func (e *readerError) Unwrap() error     { return e.error }
func (e *producerError) Unwrap() []error { return []error{manifest.ErrFlux, e.error} }

// Acquire publishes a complete archive or revalidates its previous generation after produce succeeds.
// Its acquisition identity is empty on failure.
func (c *Cache) Acquire(ctx context.Context, key ArchiveKey, policy Policy, produce func(context.Context, ProducerInput) (ProducerResult, error)) (fs.FS, ocispec.Descriptor, string, error) {
	e, id, err := c.acquire(ctx, acquireKey{producer: archiveProducer{key: key, write: produce}, required: key.Required}, policy)
	if err != nil {
		return nil, ocispec.Descriptor{}, "", err
	}
	return e.fs, e.descriptor, id, nil
}

// Blob returns an independent reader over a complete generation. A nil producer only looks up
// a fresh generation; zero Policy.MaxAge with a producer replaces the generation. Disabled
// PersistOnly publication returns a closeable empty reader without running produce.
// Unchanged production retains the previous payload and replaces its upstream metadata.
// Its acquisition identity is empty on failure.
func (c *Cache) Blob(ctx context.Context, key BlobKey, policy Policy, produce func(context.Context, ProducerInput) (ProducerResult, error)) (io.ReadCloser, ocispec.Descriptor, string, error) {
	if key.PersistOnly {
		id, ok := key.Content.(string)
		if !ok || id == "" {
			return nil, ocispec.Descriptor{}, "", &CacheError{fmt.Errorf("%w: render blob content must be a nonempty identity", manifest.ErrInput)}
		}
		if !c.persistBlob {
			if err := ctx.Err(); err != nil {
				return nil, ocispec.Descriptor{}, "", &CacheError{err}
			}
			if produce == nil {
				return nil, ocispec.Descriptor{}, "", &CacheError{errdef.ErrNotFound}
			}
			return io.NopCloser(strings.NewReader("")), ocispec.Descriptor{}, id, nil
		}
		key.Required = nil
	}
	request := acquireKey{producer: blobProducer{key: key, write: produce}, required: key.Required, lookupOnly: produce == nil}
	e, id, err := c.acquire(ctx, request, policy)
	if err != nil {
		return nil, ocispec.Descriptor{}, "", err
	}
	input := e.fs.(fileFS)
	r, err := input.Open("content")
	if err != nil {
		return nil, ocispec.Descriptor{}, "", &CacheError{err}
	}
	return &blobHandle{ReadCloser: r, input: input}, e.descriptor, id, nil
}

func (c *Cache) acquireOnce(ctx context.Context, key acquireKey, policy Policy) (*entry, error) {
	var observed *entry
	if v, ok := c.entries.Load(key.id); ok {
		observed = v.(*entry)
	} else if c.store != nil {
		v, _ := c.entries.LoadOrStore(key.id, &entry{unloaded: true})
		observed = v.(*entry)
	}
	release, err := c.locks.Acquire(ctx, key.id)
	if err != nil {
		return nil, &CacheError{err}
	}
	defer release()
	if observed != nil && !key.lookupOnly {
		if observed.waveErr != nil {
			return nil, &TerminalError{Err: observed.waveErr, Attempted: false}
		}
		if observed.waveResult != nil {
			return observed.waveResult, nil
		}
	}
	var e *entry
	if v, ok := c.entries.Load(key.id); ok {
		e = v.(*entry)
	}
	if e != nil && e.unloaded {
		if err := c.loadEntry(ctx, key, policy.MaxAge, e); err != nil {
			return nil, err
		}
	}
	if e != nil && e.outcome != nil {
		isFresh, _ := e.outcome.observe(time.Now().UTC())
		if isFresh {
			if err := c.touch(e); err != nil {
				return nil, err
			}
			return nil, e.outcome.replay()
		}
	}
	if e != nil && fresh(e.resolved, policy.MaxAge) {
		if err := c.touch(e); err != nil {
			if !key.lookupOnly {
				c.entries.Store(key.id, &entry{stored: e.stored})
			}
			return nil, err
		}
		return e, nil
	}
	if key.lookupOnly {
		if e != nil && e.stored.Digest == "" && e.resolved.IsZero() {
			c.entries.CompareAndDelete(key.id, e)
		}
		return nil, &CacheError{errdef.ErrNotFound}
	}
	return c.attempt(ctx, key, policy, e)
}

func (c *Cache) attempt(ctx context.Context, key acquireKey, policy Policy, e *entry) (*entry, error) {
	if err := ctx.Err(); err != nil {
		return nil, &CacheError{err}
	}
	var lastGood ocispec.Descriptor
	var count uint32
	due := false
	if e != nil {
		lastGood = e.stored
		if e.outcome != nil {
			// acquireOnce MUST observe this outcome under the same key lock.
			due = !e.outcome.invalid
			lastGood = e.outcome.lastGood
			if due {
				count = e.outcome.record.Count
			}
		}
	}
	if due {
		policy.Retry.Attempts = 1
		ctx = context.WithValue(ctx, attemptCeilingKey{}, true)
	}
	result, err := c.fill(ctx, key, e)
	if err != nil {
		// A joined cleanup failure MUST retain cache failure attribution.
		if producer, ok := errors.AsType[*producerError](err); !ok || !errors.Is(producer, err) {
			err = &CacheError{err}
		} else if _, terminal := errors.AsType[*TerminalError](err); !terminal && retryable(err) && policy.Retry.Attempts <= 1 {
			err = &TerminalError{Err: err, Attempted: true}
		}
		// Caller cancellation MUST retain the previous generation and its freshness.
		if ctx.Err() != nil || due && (errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)) {
			return nil, err
		}
		next := &entry{stored: lastGood}
		if due {
			next.stored, next.outcome = e.stored, e.outcome
		}
		if c.store != nil && ctx.Err() == nil {
			if _, producer := errors.AsType[*producerError](err); producer {
				if o := newOutcome(err, time.Now().UTC(), nextOutcomeCount(count), lastGood); o != nil {
					stored, saveErr := c.publishOutcome(ctx, key.id, o, err)
					if saveErr != nil {
						err = saveErr
					} else if stored.Digest != "" {
						next.stored, next.outcome = stored, o
					}
				}
			}
		}
		if due {
			e.waveErr = err
		}
		c.entries.Store(key.id, next)
		return nil, err
	}
	if due {
		e.waveResult = result
	}
	c.entries.Store(key.id, result)
	return result, nil
}

func (c *Cache) loadEntry(ctx context.Context, key acquireKey, maxAge time.Duration, e *entry) error {
	desc, err := c.store.Resolve(ctx, key.id)
	if errors.Is(err, errdef.ErrNotFound) {
		e.unloaded = false
		return nil
	}
	if err != nil {
		return &CacheError{err}
	}
	if desc.MediaType == ocispec.MediaTypeImageManifest {
		o, err := c.readOutcome(ctx, desc)
		if err != nil {
			return err
		}
		e.stored, e.outcome, e.unloaded = desc, o, false
		if o == nil {
			e.stored = ocispec.Descriptor{}
		}
		return nil
	}
	upstream, at, valid := decode(desc, key.required)
	if valid {
		e.stored = desc
	}
	if valid && fresh(at, maxAge) {
		e.descriptor, e.resolved = upstream, at
		if err := key.producer.restore(ctx, c, desc, e); err != nil {
			return &CacheError{err}
		}
	}
	e.unloaded = valid && e.resolved.IsZero()
	return nil
}

func (c *Cache) touch(e *entry) error {
	if c.store != nil {
		if e.touched.IsZero() {
			info, err := c.root.Stat(blobPath(e.stored.Digest))
			if err != nil {
				return &CacheError{err}
			}
			e.touched = info.ModTime()
		}
		now := time.Now()
		// Sweep allows the same interval so coalescing never retires a recent access early.
		if now.Sub(e.touched) < accessInterval {
			return nil
		}
		if err := c.root.Chtimes(blobPath(e.stored.Digest), now, now); err != nil {
			return &CacheError{err}
		}
		e.touched = now
	}
	return nil
}

// Forever retains a successfully resolved pin regardless of elapsed time.
const Forever time.Duration = -1

func fresh(at time.Time, maxAge time.Duration) bool {
	return !at.IsZero() && (maxAge < 0 || maxAge > 0 && time.Since(at) < maxAge)
}

func (c *Cache) fill(ctx context.Context, key acquireKey, previous *entry) (result *entry, resultErr error) {
	staged := payload{key: key, previous: previous}
	staged.writer = &staged.memory
	var tmp *os.File
	var tmpName, repairName string
	defer func() {
		var cleanup error
		if staged.discard != nil {
			cleanup = staged.discard()
		}
		if tmp != nil {
			cleanup = errors.Join(cleanup, tmp.Close())
		}
		for _, name := range [2]string{tmpName, repairName} {
			if name != "" {
				cleanup = errors.Join(cleanup, c.root.Remove(name))
			}
		}
		if cleanup != nil {
			result = nil
			resultErr = errors.Join(resultErr, cleanup)
		}
	}()
	if c.store != nil {
		var err error
		name := acquirePath(uuid.NewString())
		tmp, err = c.root.OpenFile(name, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o600)
		if err != nil {
			return nil, err
		}
		tmpName = name
		staged.hash = digest.SHA256.Digester()
		staged.writer = io.MultiWriter(tmp, staged.hash.Hash())
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	e, err := key.producer.produce(ctx, c, &staged)
	if err != nil {
		return nil, err
	}
	e.resolved = time.Now().UTC()
	if c.store == nil {
		return e, nil
	}
	var stored ocispec.Descriptor
	if staged.unchanged {
		stored = e.storedDescriptor(e.stored.Digest, e.stored.Size)
	} else {
		stored, repairName, err = c.publish(ctx, tmp, staged.hash.Digest(), e)
		if err != nil {
			return nil, err
		}
		if err := key.producer.restore(ctx, c, stored, e); err != nil {
			return nil, err
		}
	}
	closeErr := tmp.Close()
	tmp = nil
	removeErr := c.root.Remove(tmpName)
	if removeErr == nil {
		tmpName = ""
	}
	if err := errors.Join(closeErr, removeErr); err != nil {
		return nil, err
	}
	if err := c.store.Tag(ctx, stored, key.id); err != nil {
		return nil, err
	}
	e.stored = stored
	return e, nil
}

func (c *Cache) publish(ctx context.Context, tmp *os.File, payload digest.Digest, e *entry) (ocispec.Descriptor, string, error) {
	if _, err := tmp.Seek(0, io.SeekStart); err != nil {
		return ocispec.Descriptor{}, "", err
	}
	info, err := tmp.Stat()
	if err != nil {
		return ocispec.Descriptor{}, "", err
	}
	stored := e.storedDescriptor(payload, info.Size())
	if err := c.store.Push(ctx, stored, tmp); err != nil {
		if !errors.Is(err, errdef.ErrAlreadyExists) {
			return stored, "", err
		}
		info, err := os.Stat(filepath.Join(c.root.Name(), blobPath(stored.Digest)))
		if err != nil {
			return stored, "", err
		}
		if info.Size() != stored.Size {
			name, err := filepath.Rel(c.root.Name(), tmp.Name())
			if err != nil {
				return stored, "", err
			}
			name, err = safepath.ArchiveName(name)
			if err != nil {
				return stored, "", err
			}
			if err := tmp.Chmod(0o444); err != nil {
				return stored, "", err
			}
			repair := acquirePath(uuid.NewString())
			if err := c.root.Link(name, repair); err != nil {
				return stored, "", err
			}
			if err := c.root.Rename(repair, blobPath(stored.Digest)); err != nil {
				return stored, repair, err
			}
		}
	}
	return stored, "", nil
}

func (e *entry) storedDescriptor(payload digest.Digest, size int64) ocispec.Descriptor {
	stored := ocispec.Descriptor{MediaType: "application/octet-stream", Digest: payload, Size: size, Annotations: maps.Clone(e.descriptor.Annotations)}
	if stored.Annotations == nil {
		stored.Annotations = make(map[string]string)
	}
	for k, v := range map[string]string{
		"resolved-at":         e.resolved.Format(time.RFC3339Nano),
		"upstream-digest":     e.descriptor.Digest.String(),
		"upstream-size":       strconv.FormatInt(e.descriptor.Size, 10),
		"upstream-media-type": e.descriptor.MediaType,
	} {
		stored.Annotations[prefix+k] = v
	}
	return stored
}

func decode(stored ocispec.Descriptor, required []string) (ocispec.Descriptor, time.Time, bool) {
	a := maps.Clone(stored.Annotations)
	delete(a, ocispec.AnnotationRefName)
	at, err := time.Parse(time.RFC3339Nano, a[prefix+"resolved-at"])
	size, sizeErr := strconv.ParseInt(a[prefix+"upstream-size"], 10, 64)
	d := digest.Digest(a[prefix+"upstream-digest"])
	valid := stored.Digest.Validate() == nil && err == nil && sizeErr == nil && size >= 0 && (d == "" || d.Validate() == nil)
	for _, field := range required {
		valid = valid && a[prefix+field] != ""
		if field == "object-count" {
			count, err := strconv.Atoi(a[prefix+field])
			valid = valid && err == nil && count >= 0
		}
	}
	mediaType := a[prefix+"upstream-media-type"]
	for _, k := range []string{"resolved-at", "upstream-digest", "upstream-size", "upstream-media-type"} {
		delete(a, prefix+k)
	}
	return ocispec.Descriptor{Digest: d, Size: size, MediaType: mediaType, Annotations: a}, at, valid
}
