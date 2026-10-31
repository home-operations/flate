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
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
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
	Content any
	Ignore  string
	Policy  string
	// Required lists unprefixed descriptor annotation names; it does not affect identity.
	Required []string
}

// RenderMode identifies the acquisition phase of rendered content.
type RenderMode uint8

const (
	// RenderPending validates accepted history, then coalesces immutable work until Close.
	RenderPending RenderMode = iota + 1
	// RenderAccepted persists only complete accepted render history with identified backing.
	RenderAccepted
)

// BlobKey identifies single-file content.
// Content MUST include the effective credentials used by the producer.
type BlobKey struct {
	// Content describes the producer inputs, or a nonempty identity for rendered content.
	Content any
	// Render selects pending work or accepted history; zero denotes source content.
	Render RenderMode
	// Validate checks a candidate against this caller's inputs under the acquisition lock.
	// Streams belong to the callback; the callback never affects identity.
	Validate func(context.Context, fs.FS) (bool, error)
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

func (r *blobRoot) current(owner *os.Root) (*os.Root, bool, error) {
	info, err := owner.Stat(r.path)
	if err != nil {
		return nil, false, err
	}
	if os.SameFile(info, r.info) {
		return r.Root, false, nil
	}
	current, err := owner.OpenRoot(r.path)
	return current, true, err
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
	// Descriptor fields MUST remain immutable after ready publishes them.
	ready      atomic.Bool
	fs         fs.FS
	descriptor ocispec.Descriptor
	stored     ocispec.Descriptor
	resolved   time.Time
	touched    atomic.Int64
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
	accepted   string
	memoryOnly bool
	render     bool
	policy     Policy
}

type producerError struct {
	error
	causes [2]error
}

func newProducerError(err error) *producerError {
	return &producerError{error: err, causes: [2]error{manifest.ErrFlux, err}}
}

// readerError marks a failure of the archive consumer that the producer only observes as a failed write.
type readerError struct{ error }

func (e *readerError) Unwrap() error     { return e.error }
func (e *producerError) Unwrap() []error { return e.causes[:] }

// Acquire publishes a complete archive or revalidates its previous generation after produce succeeds.
// Its acquisition identity is empty on failure.
func (c *Cache) Acquire(ctx context.Context, key ArchiveKey, policy Policy, produce func(context.Context, ProducerInput) (ProducerResult, error)) (fs.FS, ocispec.Descriptor, string, error) {
	switch key.Policy {
	case "", "source", "fixed", "extract":
	default:
		return nil, ocispec.Descriptor{}, "", &CacheError{manifest.NewError(manifest.ErrInput, nil, fmt.Sprintf("unknown archive policy %q", key.Policy))}
	}
	producer := archiveProducer{key: key}
	e, id, err := c.acquire(ctx, acquireKey{producer: producer, required: key.Required}, policy, acquisitionCallbacks{produce: func(ctx context.Context, c *Cache, staged *payload) (*entry, error) {
		return producer.produce(ctx, c, staged, produce)
	}})
	if err != nil {
		return nil, ocispec.Descriptor{}, "", err
	}
	descriptor := e.descriptor
	descriptor.Annotations = maps.Clone(descriptor.Annotations)
	if key.Policy == "fixed" {
		fingerprint, err := e.fs.(*tarFS).treeIdentity(ctx)
		if err != nil {
			return nil, ocispec.Descriptor{}, "", &CacheError{err}
		}
		if descriptor.Annotations == nil {
			descriptor.Annotations = make(map[string]string)
		}
		descriptor.Annotations["flate.tree-digest"] = "sha256:" + fingerprint
	}
	return e.fs, descriptor, id, nil
}

// CanPersistRenders reports whether the cache owner can persist accepted renders.
func (c *Cache) CanPersistRenders() bool { return c.persistBlob }

// Blob returns an immutable filesystem over a complete generation. A nil producer only looks up
// a fresh generation; zero Policy.MaxAge with a producer replaces the generation.
// Unchanged production retains the previous payload and replaces its upstream metadata.
// A successful generation rejected by Validate is returned once without being retained for reuse.
// Its acquisition identity is empty on failure.
func (c *Cache) Blob(ctx context.Context, key BlobKey, policy Policy, produce func(context.Context, ProducerInput) (ProducerResult, error)) (fs.FS, ocispec.Descriptor, string, error) {
	switch key.Render {
	case 0, RenderPending, RenderAccepted:
	default:
		return nil, ocispec.Descriptor{}, "", &CacheError{manifest.NewError(manifest.ErrInput, nil, fmt.Sprintf("unknown render mode %d", key.Render))}
	}
	if key.Render != 0 {
		id, ok := key.Content.(string)
		if !ok || id == "" {
			return nil, ocispec.Descriptor{}, "", &CacheError{manifest.NewError(manifest.ErrInput, nil, "render blob content must be a nonempty identity")}
		}
		if key.Render == RenderAccepted {
			enabled, err := c.renderEnabled(ctx, id)
			if err != nil {
				return nil, ocispec.Descriptor{}, "", err
			}
			if !enabled {
				if produce == nil {
					return nil, ocispec.Descriptor{}, "", &CacheError{errdef.ErrNotFound}
				}
				return fileFS{name: "content", open: func() (io.ReadCloser, error) { return io.NopCloser(strings.NewReader("")), nil }}, ocispec.Descriptor{}, id, nil
			}
		}
		key.Required = nil
	}
	producer := blobProducer{content: key.Content, render: key.Render}
	request := acquireKey{producer: producer, required: key.Required, lookupOnly: produce == nil, memoryOnly: key.Render == RenderPending, render: key.Render != 0}
	if key.Render == RenderPending && c.CanPersistRenders() && policy.MaxAge != 0 {
		request.accepted = key.Content.(string)
	}
	e, id, err := c.acquire(ctx, request, policy, acquisitionCallbacks{validate: key.Validate, produce: func(ctx context.Context, c *Cache, staged *payload) (*entry, error) {
		return producer.produce(ctx, c, staged, produce)
	}})
	if err != nil {
		return nil, ocispec.Descriptor{}, "", err
	}
	if c.store != nil && !request.memoryOnly {
		if _, err := c.root.Stat(e.fs.(fileFS).backingPath); err != nil {
			return nil, ocispec.Descriptor{}, "", &CacheError{err}
		}
	}
	return e.fs, e.descriptor, id, nil
}

// RenderMetadata returns the current descriptor and generation without opening the payload.
// The boolean reports whether this owner can persist renders. A nonempty lookup
// MUST identify Blob accepted history for this content; an empty lookup derives it.
func (c *Cache) RenderMetadata(ctx context.Context, content, lookup string) (ocispec.Descriptor, string, bool, error) {
	enabled, err := c.renderEnabled(ctx, content)
	if err != nil || !enabled {
		return ocispec.Descriptor{}, "", enabled, err
	}
	if err := ctx.Err(); err != nil {
		return ocispec.Descriptor{}, "", true, &CacheError{err}
	}
	var e *entry
	if lookup != "" {
		if current, ok := c.entries.Load(lookup); ok && current.(*entry).ready.Load() && time.Now().UnixNano()-current.(*entry).touched.Load() < int64(accessInterval) {
			e = current.(*entry)
		}
	}
	if e == nil {
		key := BlobKey{Content: content, Render: RenderAccepted}
		request := acquireKey{producer: blobProducer{content: key.Content, render: key.Render}, lookupOnly: true, render: true, policy: Policy{MaxAge: Forever}}
		if lookup == "" {
			e, _, err = c.acquire(ctx, request, Policy{MaxAge: Forever}, acquisitionCallbacks{})
		} else {
			request.id = lookup
			e, err = c.acquireOnce(ctx, request, acquisitionCallbacks{})
		}
		if err == nil && !e.ready.Load() {
			e.ready.Store(true)
		}
		if err != nil {
			return ocispec.Descriptor{}, "", true, err
		}
	}
	root := &c.blobRoots[slices.Index(blobAlgorithms[:], e.stored.Digest.Algorithm())]
	current, temporary, err := root.current(c.root)
	if err != nil {
		return ocispec.Descriptor{}, "", true, &CacheError{err}
	}
	if temporary {
		defer func() { _ = current.Close() }()
	}
	info, err := current.Stat(e.stored.Digest.Encoded())
	if err != nil {
		return ocispec.Descriptor{}, "", true, &CacheError{err}
	}
	if !info.Mode().IsRegular() {
		return ocispec.Descriptor{}, "", true, &CacheError{fmt.Errorf("%w: render blob is not a regular file", fs.ErrInvalid)}
	}
	return e.descriptor, e.stored.Digest.String(), true, nil
}

func (c *Cache) renderEnabled(ctx context.Context, content string) (bool, error) {
	if content == "" {
		return false, &CacheError{manifest.NewError(manifest.ErrInput, nil, "render blob content must be a nonempty identity")}
	}
	if !c.CanPersistRenders() {
		if err := ctx.Err(); err != nil {
			return false, &CacheError{err}
		}
	}
	return c.CanPersistRenders(), nil
}

func (c *Cache) acquireOnce(ctx context.Context, key acquireKey, callbacks acquisitionCallbacks) (*entry, error) {
	var observed, rejected *entry
	if v, ok := c.entries.Load(key.id); ok {
		observed = v.(*entry)
	} else {
		v, _ := c.entries.LoadOrStore(key.id, &entry{unloaded: c.store != nil && !key.memoryOnly})
		observed = v.(*entry)
	}
	release, err := c.locks.Acquire(ctx, key.id)
	if err != nil {
		return nil, &CacheError{err}
	}
	defer release()
	if key.accepted != "" {
		generation, descriptor, _, err := c.Blob(ctx, BlobKey{Content: key.accepted, Render: RenderAccepted}, Policy{MaxAge: Forever}, nil)
		if err != nil && !errors.Is(err, errdef.ErrNotFound) {
			return nil, err
		}
		if err == nil {
			accepted := &entry{fs: generation, descriptor: descriptor}
			valid, err := c.validCandidate(ctx, callbacks.validate, accepted)
			if err != nil || valid {
				return accepted, err
			}
		}
	}
	if observed != nil && !key.lookupOnly {
		if observed.waveErr != nil {
			if observed.outcome != nil {
				return nil, &TerminalError{Err: observed.waveErr, Attempted: false}
			}
			return nil, observed.waveErr
		}
		if observed.waveResult != nil {
			valid, err := c.validCandidate(ctx, callbacks.validate, observed.waveResult)
			if err != nil || valid {
				return observed.waveResult, err
			}
			rejected = observed.waveResult
		}
	}
	var e *entry
	if v, ok := c.entries.Load(key.id); ok {
		e = v.(*entry)
	} else {
		e = observed
		c.entries.Store(key.id, e)
	}
	if e != nil && e.unloaded && !key.memoryOnly {
		if err := c.loadEntry(ctx, key, key.policy.MaxAge, e); err != nil {
			return nil, err
		}
	}
	if e != nil && e.outcome != nil {
		isFresh, _ := e.outcome.observe(time.Now().UTC())
		if isFresh {
			if err := c.touch(key, e); err != nil {
				return nil, err
			}
			return nil, e.outcome.replay()
		}
	}
	if e != nil && e != rejected && fresh(e.resolved, key.policy.MaxAge) {
		if err := c.touch(key, e); err != nil {
			if !key.lookupOnly {
				c.entries.Store(key.id, &entry{stored: e.stored})
			}
			return nil, err
		}
		valid, err := c.validCandidate(ctx, callbacks.validate, e)
		if err != nil || valid {
			return e, err
		}
	}
	if key.lookupOnly {
		if e != nil && e.stored.Digest == "" && e.resolved.IsZero() {
			c.entries.CompareAndDelete(key.id, e)
		}
		return nil, &CacheError{errdef.ErrNotFound}
	}
	return c.attempt(ctx, key, e, callbacks)
}

func (c *Cache) validCandidate(ctx context.Context, validate func(context.Context, fs.FS) (bool, error), e *entry) (bool, error) {
	if validate == nil {
		return true, nil
	}
	valid, err := validate(ctx, e.fs)
	if err != nil {
		return false, &CacheError{err}
	}
	return valid, nil
}

func (c *Cache) attempt(ctx context.Context, key acquireKey, e *entry, callbacks acquisitionCallbacks) (*entry, error) {
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
		key.policy.Retry.Attempts = 1
		ctx = context.WithValue(ctx, attemptCeilingKey{}, true)
	}
	result, err := c.fill(ctx, key, e, callbacks)
	if result != nil && result.resolved.IsZero() {
		return result, err
	}
	if err != nil {
		// A joined cleanup failure MUST retain cache failure attribution.
		if producer, ok := errors.AsType[*producerError](err); !ok || !errors.Is(producer, err) {
			err = &CacheError{err}
		} else if _, terminal := errors.AsType[*TerminalError](err); !terminal && retryable(err) && key.policy.Retry.Attempts <= 1 {
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
		if c.store != nil && !key.memoryOnly && ctx.Err() == nil {
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
		// Retry and deadline failures MUST retain each caller's policy.
		shareFailure := !retryable(err) && !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded)
		if _, terminal := errors.AsType[*TerminalError](err); due || shareFailure && !terminal {
			e.waveErr = err
		}
		c.entries.Store(key.id, next)
		return nil, err
	}
	e.waveResult = result
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

func (c *Cache) touch(key acquireKey, e *entry) error {
	if c.store != nil && !key.memoryOnly {
		touched := e.touched.Load()
		if touched == 0 {
			info, err := c.root.Stat(blobPath(e.stored.Digest))
			if err != nil {
				return &CacheError{err}
			}
			touched = info.ModTime().UnixNano()
			e.touched.Store(touched)
		}
		now := time.Now()
		// Sweep allows the same interval so coalescing never retires a recent access early.
		if now.UnixNano()-touched < int64(accessInterval) {
			return nil
		}
		if err := c.root.Chtimes(blobPath(e.stored.Digest), now, now); err != nil {
			return &CacheError{err}
		}
		e.touched.Store(now.UnixNano())
	}
	return nil
}

// Forever retains a successfully resolved pin regardless of elapsed time.
const Forever time.Duration = -1

func fresh(at time.Time, maxAge time.Duration) bool {
	return !at.IsZero() && (maxAge < 0 || maxAge > 0 && time.Since(at) < maxAge)
}

func (c *Cache) fill(ctx context.Context, key acquireKey, previous *entry, callbacks acquisitionCallbacks) (result *entry, resultErr error) {
	if key.policy.Admit != nil {
		release, err := key.policy.Admit(ctx)
		if err != nil {
			return nil, err
		}
		defer release()
	}
	if key.policy.Timeout != nil {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, *key.policy.Timeout)
		defer cancel()
	}
	staged := payload{key: key, previous: previous}
	staged.writer = &staged.memory
	var repairName string
	defer func() {
		var cleanup error
		if staged.discard != nil {
			cleanup = staged.discard()
		}
		if staged.file != nil {
			cleanup = errors.Join(cleanup, staged.file.Close())
		}
		for _, name := range [2]string{staged.name, repairName} {
			if name != "" {
				cleanup = errors.Join(cleanup, c.root.Remove(name))
			}
		}
		if cleanup != nil {
			result = nil
			resultErr = errors.Join(resultErr, cleanup)
		}
	}()
	if c.store != nil && !key.memoryOnly {
		staged.disk, staged.writer = c, nil
		if !key.render {
			if err := staged.stage(); err != nil {
				return nil, err
			}
		}
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	e, err := callbacks.produce(ctx, c, &staged)
	if err != nil {
		if producer, ok := errors.AsType[*producerError](err); ok && errors.Is(producer, err) && (key.policy.Machine || credentialsUnavailable(producer.error)) {
			err = newProducerError(MachineCredentialErr(producer.error))
		}
		return nil, err
	}
	if key.policy.Timeout != nil && ctx.Err() != nil {
		return nil, ctx.Err()
	}
	e.resolved = time.Now().UTC()
	var stored ocispec.Descriptor
	if c.store != nil && !key.memoryOnly {
		if staged.writer == nil {
			if err := staged.stage(); err != nil {
				return nil, err
			}
		}
		if staged.unchanged {
			stored = e.storedDescriptor(e.stored.Digest, e.stored.Size)
		} else {
			stored, repairName, err = c.publish(ctx, staged.file, staged.hash.Digest(), e)
			if err != nil {
				return nil, err
			}
			if err := key.producer.restore(ctx, c, stored, e); err != nil {
				return nil, err
			}
		}
		closeErr := staged.file.Close()
		staged.file = nil
		removeErr := c.root.Remove(staged.name)
		if removeErr == nil {
			staged.name = ""
		}
		if err := errors.Join(closeErr, removeErr); err != nil {
			return nil, err
		}
	}
	valid, err := c.validCandidate(ctx, callbacks.validate, e)
	if key.policy.Timeout != nil && ctx.Err() != nil {
		return nil, ctx.Err()
	}
	if !valid {
		e.resolved = time.Time{}
		return e, err
	}
	if c.store != nil && !key.memoryOnly {
		if err := c.store.Tag(ctx, stored, key.id); err != nil {
			return nil, err
		}
		e.stored = stored
	}
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
