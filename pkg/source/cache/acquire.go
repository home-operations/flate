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
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/opencontainers/go-digest"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"oras.land/oras-go/v2/content/oci"
	"oras.land/oras-go/v2/errdef"

	"github.com/home-operations/flate/internal/keylock"
	"github.com/home-operations/flate/pkg/manifest"
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
		return nil, false, err //nolint:wrapcheck // Filesystem adapters retain fs.PathError operation, path, and concrete error identity.
	}
	if os.SameFile(info, r.info) {
		return r.Root, false, nil
	}
	current, err := owner.OpenRoot(r.path)
	return current, true, err //nolint:wrapcheck // Filesystem adapters retain fs.PathError operation, path, and concrete error identity.
}

// Cache owns acquired generations until Close. Readers MUST finish before Close.
type Cache struct {
	root          *os.Root
	blobRoots     [len(blobAlgorithms)]blobRoot
	store         *oci.Store
	locks         *keylock.KeyMap[string]
	entries       sync.Map
	persistBlob   bool
	snapshot      atomic.Pointer[storeSnapshot]
	snapshotState storeSnapshot
	snapshotDirty atomic.Bool
}

type entry struct {
	inventoryKey string
	history      *historyRecord
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
func (c *Cache) Acquire(
	ctx context.Context,
	key ArchiveKey,
	policy Policy,
	produce func(context.Context, ProducerInput) (ProducerResult, error),
) (fs.FS, ocispec.Descriptor, string, error) {
	switch key.Policy {
	case "", "source", fixedPolicy, extractPolicy:
	default:
		return nil, ocispec.Descriptor{}, "", &CacheError{
			manifest.NewError(manifest.ErrInput, nil, fmt.Sprintf("unknown archive policy %q", key.Policy)),
		}
	}
	producer := archiveProducer{key: key}
	e, id, err := c.acquire(
		ctx, new(acquireKey{producer: producer, required: key.Required}), policy,
		acquisitionCallbacks{produce: func(ctx context.Context, c *Cache, staged *payload) (*entry, error) {
			return producer.produce(ctx, c, staged, produce)
		}},
	)
	if err != nil {
		return nil, ocispec.Descriptor{}, "", err
	}
	descriptor := e.descriptor
	descriptor.Annotations = maps.Clone(descriptor.Annotations)
	if key.Policy == fixedPolicy {
		fingerprint, err := e.fs.(*tarFS).treeIdentity( //nolint:errcheck // The owner stores this exact concrete type; a mismatched assertion must retain its panic.
			ctx,
		)
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
func (c *Cache) Blob(
	ctx context.Context,
	key BlobKey,
	policy Policy,
	produce func(context.Context, ProducerInput) (ProducerResult, error),
) (fs.FS, ocispec.Descriptor, string, error) {
	switch key.Render {
	case 0, RenderPending, RenderAccepted:
	default:
		return nil, ocispec.Descriptor{}, "", &CacheError{
			manifest.NewError(manifest.ErrInput, nil, fmt.Sprintf("unknown render mode %d", key.Render)),
		}
	}
	if key.Render != 0 {
		stub, identity, err := c.prepareRenderBlob(ctx, &key, produce == nil)
		if err != nil || stub != nil {
			return stub, ocispec.Descriptor{}, identity, err
		}
		key.Required = nil
	}
	producer := blobProducer{content: key.Content, render: key.Render}
	request := acquireKey{
		producer:   producer,
		required:   key.Required,
		lookupOnly: produce == nil,
		memoryOnly: key.Render == RenderPending,
		render:     key.Render != 0,
	}
	if key.Render == RenderPending && c.CanPersistRenders() && policy.MaxAge != 0 {
		request.accepted = key.Content.(string) //nolint:errcheck // The owner stores this exact concrete type; a mismatched assertion must retain its panic.
	}
	var validate func(context.Context, *entry) (bool, error)
	if key.Validate != nil {
		validate = func(ctx context.Context, e *entry) (bool, error) { return key.Validate(ctx, e.fs) }
	}
	e, id, err := c.acquire(
		ctx, new(request), policy,
		acquisitionCallbacks{
			validate: validate,
			produce: func(ctx context.Context, c *Cache, staged *payload) (*entry, error) {
				return producer.produce(ctx, c, staged, produce)
			},
		},
	)
	if err != nil {
		return nil, ocispec.Descriptor{}, "", err
	}
	if c.store != nil && !request.memoryOnly {
		if _, err := c.root.Stat(
			e.fs.(fileFS).backingPath, //nolint:errcheck // The owner stores this exact concrete type; a mismatched assertion must retain its panic.
		); err != nil {
			return nil, ocispec.Descriptor{}, "", &CacheError{err}
		}
	}
	return e.fs, e.descriptor, id, nil
}

// RenderMetadata returns the current descriptor and generation without opening the payload.
// Enabled reports whether this owner can persist renders. A nonempty lookup
// MUST identify Blob accepted history for this content; an empty lookup derives it.
func (c *Cache) RenderMetadata(ctx context.Context, content, lookup string) (RenderMetadata, error) {
	enabled, err := c.renderEnabled(ctx, content)
	if err != nil || !enabled {
		return RenderMetadata{Enabled: enabled}, err
	}
	if errErr := ctx.Err(); errErr != nil {
		return RenderMetadata{Enabled: true}, &CacheError{errErr}
	}
	var e *entry
	if lookup != "" {
		if current, ok := c.entries.Load(
			lookup,
		); ok && current.(*entry).ready.Load() && //nolint:errcheck // The owner stores this exact concrete type; a mismatched assertion must retain its panic.
			time.Now().
				UnixNano()-
				current.(*entry).touched.Load() < int64( //nolint:errcheck // The owner stores this exact concrete type; a mismatched assertion must retain its panic.
				accessInterval,
			) {
			e = current.(*entry) //nolint:errcheck // The owner stores this exact concrete type; a mismatched assertion must retain its panic.
		}
	}
	if e == nil {
		key := BlobKey{Content: content, Render: RenderAccepted}
		request := acquireKey{
			producer:   blobProducer{content: key.Content, render: key.Render},
			lookupOnly: true,
			render:     true,
			policy:     Policy{MaxAge: Forever},
		}
		if lookup == "" {
			e, _, err = c.acquire(ctx, new(request), Policy{MaxAge: Forever}, acquisitionCallbacks{})
		} else {
			request.id = lookup
			e, err = c.acquireOnce(ctx, &request, acquisitionCallbacks{})
		}
		if err == nil && !e.ready.Load() {
			e.ready.Store(true)
		}
		if err != nil {
			return RenderMetadata{Enabled: true}, err
		}
	}
	root := &c.blobRoots[slices.Index(blobAlgorithms[:], e.stored.Digest.Algorithm())]
	current, temporary, err := root.current(c.root)
	if err != nil {
		return RenderMetadata{Enabled: true}, &CacheError{err}
	}
	if temporary {
		defer func() { _ = current.Close() }()
	}
	info, err := current.Stat(e.stored.Digest.Encoded())
	if err != nil {
		return RenderMetadata{Enabled: true}, &CacheError{err}
	}
	if !info.Mode().IsRegular() {
		return RenderMetadata{Enabled: true}, &CacheError{
			fmt.Errorf("%w: render blob is not a regular file", fs.ErrInvalid),
		}
	}
	return RenderMetadata{Descriptor: e.descriptor, Identity: e.stored.Digest.String(), Enabled: true}, nil
}

func (c *Cache) renderEnabled(ctx context.Context, content string) (bool, error) {
	if content == "" {
		return false, &CacheError{
			manifest.NewError(manifest.ErrInput, nil, "render blob content must be a nonempty identity"),
		}
	}
	if !c.CanPersistRenders() {
		if err := ctx.Err(); err != nil {
			return false, &CacheError{err}
		}
	}
	return c.CanPersistRenders(), nil
}

func (c *Cache) acquireOnce(ctx context.Context, keyValue *acquireKey, callbacks acquisitionCallbacks) (*entry, error) {
	key := *keyValue
	if !key.memoryOnly {
		c.prepareSnapshot()
	}
	var observed *entry
	if v, ok := c.entries.Load(key.id); ok {
		observed = v.(*entry) //nolint:errcheck // The owner stores this exact concrete type; a mismatched assertion must retain its panic.
	} else {
		v, _ := c.entries.LoadOrStore(key.id, &entry{unloaded: c.store != nil && !key.memoryOnly})
		observed = v.(*entry) //nolint:errcheck // The owner stores this exact concrete type; a mismatched assertion must retain its panic.
	}
	release, err := c.locks.Acquire(ctx, key.id)
	if err != nil {
		return nil, &CacheError{err}
	}
	defer release()
	candidate, err := c.acceptedCandidate(ctx, &key, callbacks.validate)
	if candidate != nil || err != nil {
		return candidate, err
	}
	candidates, err := c.waveCandidate(ctx, &key, observed, callbacks.validate)
	candidate, rejected := candidates.candidate, candidates.rejected
	if candidate != nil || err != nil {
		return candidate, err
	}

	var e *entry
	if v, ok := c.entries.Load(key.id); ok {
		e = v.(*entry) //nolint:errcheck // The owner stores this exact concrete type; a mismatched assertion must retain its panic.
	} else {
		e = observed
		c.entries.Store(key.id, e)
	}
	if err := c.restoreCandidate(ctx, &key, e); err != nil {
		return nil, err
	}

	if err := c.replayOutcome(&key, e); err != nil {
		return nil, err
	}
	if e != nil && e != rejected && fresh(e.resolved, key.policy.MaxAge) {
		candidate, candidateErr := c.freshCandidate(ctx, &key, e, callbacks.validate)
		if candidate != nil || candidateErr != nil {
			return candidate, candidateErr
		}
	}

	if key.lookupOnly {
		return nil, c.missingGeneration(&key, e)
	}
	return c.attempt(ctx, &key, e, callbacks)
}

func (c *Cache) validCandidate(
	ctx context.Context,
	validate func(context.Context, *entry) (bool, error),
	e *entry,
) (bool, error) {
	if validate == nil {
		return true, nil
	}
	valid, err := validate(ctx, e)
	if err != nil {
		return false, &CacheError{err}
	}
	return valid, nil
}

func (c *Cache) loadEntry(ctx context.Context, keyValue *acquireKey, maxAge time.Duration, e *entry) error {
	key := *keyValue
	desc, err := c.store.Resolve(ctx, key.id)
	if errors.Is(err, errdef.ErrNotFound) {
		e.unloaded = false
		return nil
	}
	if err != nil {
		return &CacheError{err}
	}
	if desc.MediaType == ocispec.MediaTypeImageManifest {
		o, err := c.readOutcome(ctx, &desc)
		if err != nil {
			return err
		}
		e.stored, e.outcome, e.unloaded = desc, o, false
		if o == nil {
			e.stored = ocispec.Descriptor{}
		}
		return nil
	}
	upstream, at, valid := decode(&desc, key.required)
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

func (c *Cache) touch(keyValue *acquireKey, e *entry) error {
	key := *keyValue
	if c.store == nil || key.memoryOnly {
		return nil
	}
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
	return nil
}

// Forever retains a successfully resolved pin regardless of elapsed time.
const Forever time.Duration = -1

func fresh(at time.Time, maxAge time.Duration) bool {
	return !at.IsZero() && (maxAge < 0 || maxAge > 0 && time.Since(at) < maxAge)
}

//nolint:nonamedreturns // Deferred cleanup must clear successful results and join cleanup errors, including during panic unwinding.
func (c *Cache) fill(
	ctx context.Context, keyValue *acquireKey,
	previous *entry,
	callbacks acquisitionCallbacks,
) (result *entry, resultErr error) {
	key := *keyValue
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
		cleanup := staged.cleanup(c.root, repairName)
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
	e, repair, err := c.producePayload(ctx, &staged, callbacks)
	repairName = repair
	return e, err
}

func (c *Cache) producePayload(
	ctx context.Context, staged *payload, callbacks acquisitionCallbacks,
) (*entry, string, error) {
	key := &staged.key
	var repairName string
	if err := ctx.Err(); err != nil {
		return nil, repairName, err //nolint:wrapcheck // Cancellation already identifies its cause; another operation prefix would add no resource information.
	}
	e, err := callbacks.produce(ctx, c, staged)
	if err != nil {
		if producer, ok := errors.AsType[*producerError](
			err,
		); ok && errors.Is(producer, err) &&
			(key.policy.Machine || credentialsUnavailable(producer.error)) {
			err = newProducerError(MachineCredentialErr(producer.error))
		}
		return nil, repairName, err
	}
	if key.policy.Timeout != nil && ctx.Err() != nil {
		return nil, repairName, ctx.Err() //nolint:wrapcheck // Cancellation already identifies its cause; another operation prefix would add no resource information.
	}
	e.resolved = time.Now().UTC()
	var stored ocispec.Descriptor
	if c.store != nil && !key.memoryOnly {
		stored, repairName, err = c.persistPayload(ctx, key, staged, e)
		if err != nil {
			return nil, repairName, err
		}
	}

	valid, err := c.validCandidate(ctx, callbacks.validate, e)
	if key.policy.Timeout != nil && ctx.Err() != nil {
		return nil, repairName, ctx.Err() //nolint:wrapcheck // Cancellation already identifies its cause; another operation prefix would add no resource information.
	}
	if !valid {
		e.resolved = time.Time{}
		return e, repairName, err
	}
	if err := c.tagPayload(ctx, key, e, &stored); err != nil {
		return nil, repairName, err
	}

	return e, repairName, nil
}

func (c *Cache) tagPayload(ctx context.Context, key *acquireKey, e *entry, stored *ocispec.Descriptor) error {
	if c.store != nil && !key.memoryOnly {
		c.snapshotDirty.Store(true)
		if err := c.store.Tag(ctx, *stored, key.id); err != nil {
			if snapshot := c.snapshot.Load(); snapshot != nil {
				snapshot.failed.Store(true)
			}
			return fmt.Errorf("tag cache generation: %w", err)
		}
		e.stored = *stored
	}
	return nil
}

func decode(storedValue *ocispec.Descriptor, required []string) (ocispec.Descriptor, time.Time, bool) {
	stored := *storedValue
	a := maps.Clone(stored.Annotations)
	delete(a, ocispec.AnnotationRefName)
	at, err := time.Parse(time.RFC3339Nano, a[prefix+"resolved-at"])
	size, sizeErr := strconv.ParseInt(a[prefix+"upstream-size"], 10, 64)
	d := digest.Digest(a[prefix+"upstream-digest"])
	valid := stored.Digest.Validate() == nil && err == nil && sizeErr == nil && size >= 0 &&
		(d == "" || d.Validate() == nil)
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

func (c *Cache) restoreCandidate(ctx context.Context, key *acquireKey, e *entry) error {
	if e != nil && e.unloaded && !key.memoryOnly {
		if err := c.loadEntry(ctx, key, key.policy.MaxAge, e); err != nil {
			return err
		}
	}
	return nil
}

func (c *Cache) prepareRenderBlob(ctx context.Context, key *BlobKey, lookupOnly bool) (fs.FS, string, error) {
	id, ok := key.Content.(string)
	if !ok || id == "" {
		return nil, "", &CacheError{
			manifest.NewError(manifest.ErrInput, nil, "render blob content must be a nonempty identity"),
		}
	}
	if key.Render == RenderAccepted {
		enabled, err := c.renderEnabled(ctx, id)
		if err != nil {
			return nil, "", err
		}
		if !enabled {
			if lookupOnly {
				return nil, "", &CacheError{errdef.ErrNotFound}
			}
			return fileFS{
				name: payloadName,
				open: func() (io.ReadCloser, error) { return io.NopCloser(strings.NewReader("")), nil },
			}, id, nil
		}
	}
	return nil, "", nil
}

func (c *Cache) replayOutcome(key *acquireKey, e *entry) error {
	if e != nil && e.outcome != nil {
		freshness := e.outcome.observe(time.Now().UTC())

		if freshness.fresh {
			if err := c.touch(key, e); err != nil {
				return err
			}
			return e.outcome.replay()
		}
	}
	return nil
}

func (c *Cache) missingGeneration(key *acquireKey, e *entry) error {
	if e != nil && e.stored.Digest == "" && e.resolved.IsZero() {
		c.entries.CompareAndDelete(key.id, e)
	}
	return &CacheError{errdef.ErrNotFound}
}

// RenderMetadata identifies the accepted generation without opening its payload.
type RenderMetadata struct {
	Descriptor ocispec.Descriptor
	Identity   string
	Enabled    bool
}
