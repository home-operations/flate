// Package cache acquires verified source bytes into immutable memory or OCI-backed filesystems.
package cache

import (
	"bytes"
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
	"github.com/opencontainers/go-digest"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"golang.org/x/sync/errgroup"
	"oras.land/oras-go/v2/content/oci"
	"oras.land/oras-go/v2/errdef"
)

const prefix = "flate."

const maxTrailingBytes = 1 << 20

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

// Cache owns acquired generations until Close. Readers MUST finish before Close.
type Cache struct {
	layout      layout
	root        *os.Root
	store       *oci.Store
	locks       *keylock.KeyMap[string]
	entries     sync.Map
	persistBlob func(context.Context, string, Policy, func(context.Context, io.Writer) (ocispec.Descriptor, error)) (io.ReadCloser, ocispec.Descriptor, string, error)
}

type entry struct {
	fs         fs.FS
	descriptor ocispec.Descriptor
	stored     ocispec.Descriptor
	resolved   time.Time
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
	ArchiveKey
	id         string
	archive    bool
	git        *gitRequest
	lookupOnly bool
}

type producerError struct{ error }

// readerError marks a failure of the archive consumer that the producer only observes as a failed write.
type readerError struct{ error }

func (e *readerError) Unwrap() error     { return e.error }
func (e *producerError) Unwrap() []error { return []error{manifest.ErrFlux, e.error} }

// Acquire publishes a complete archive only after produce succeeds.
// Its acquisition identity is empty on failure.
func (c *Cache) Acquire(ctx context.Context, key ArchiveKey, policy Policy, produce func(context.Context, io.Writer) (ocispec.Descriptor, error)) (fs.FS, ocispec.Descriptor, string, error) {
	e, id, err := c.acquire(ctx, acquireKey{ArchiveKey: key, archive: true}, policy, produce)
	if err != nil {
		return nil, ocispec.Descriptor{}, "", err
	}
	return e.fs, e.descriptor, id, nil
}

// Blob returns an independent reader over a complete generation. A nil producer only looks up
// a fresh generation; zero Policy.MaxAge with a producer replaces the generation. Disabled
// PersistOnly publication returns a closeable empty reader without running produce.
// Its acquisition identity is empty on failure.
func (c *Cache) Blob(ctx context.Context, key BlobKey, policy Policy, produce func(context.Context, io.Writer) (ocispec.Descriptor, error)) (io.ReadCloser, ocispec.Descriptor, string, error) {
	if key.PersistOnly {
		id, ok := key.Content.(string)
		if !ok || id == "" {
			return nil, ocispec.Descriptor{}, "", &CacheError{fmt.Errorf("%w: render blob content must be a nonempty identity", manifest.ErrInput)}
		}
		return c.persistBlob(ctx, id, policy, produce)
	}
	return c.blob(ctx, acquireKey{Content: key.Content, Required: key.Required, lookupOnly: produce == nil}, policy, produce)
}

func discardBlob(ctx context.Context, id string, _ Policy, produce func(context.Context, io.Writer) (ocispec.Descriptor, error)) (io.ReadCloser, ocispec.Descriptor, string, error) {
	if err := ctx.Err(); err != nil {
		return nil, ocispec.Descriptor{}, "", &CacheError{err}
	}
	if produce == nil {
		return nil, ocispec.Descriptor{}, "", &CacheError{errdef.ErrNotFound}
	}
	return io.NopCloser(strings.NewReader("")), ocispec.Descriptor{}, id, nil
}

func (c *Cache) blob(ctx context.Context, key acquireKey, policy Policy, produce func(context.Context, io.Writer) (ocispec.Descriptor, error)) (io.ReadCloser, ocispec.Descriptor, string, error) {
	e, id, err := c.acquire(ctx, key, policy, produce)
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

func (c *Cache) acquireOnce(ctx context.Context, key acquireKey, policy Policy, produce func(context.Context, io.Writer) (ocispec.Descriptor, error)) (*entry, error) {
	id := key.id
	var observed *entry
	if v, ok := c.entries.Load(id); ok {
		observed = v.(*entry)
	} else if c.store != nil {
		v, _ := c.entries.LoadOrStore(id, &entry{unloaded: true})
		observed = v.(*entry)
	}
	release, err := c.locks.Acquire(ctx, id)
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
	if v, ok := c.entries.Load(id); ok {
		e = v.(*entry)
	}
	if e != nil && e.unloaded {
		if err := c.loadEntry(ctx, key, policy.MaxAge, e); err != nil {
			return nil, err
		}
	}
	due := false
	if e != nil && e.outcome != nil {
		isFresh, valid := e.outcome.observe(time.Now().UTC())
		if isFresh {
			if err := c.touch(e); err != nil {
				return nil, err
			}
			return nil, e.outcome.replay()
		}
		due = valid
	}
	if e != nil && fresh(e.resolved, policy.MaxAge) {
		if err := c.touch(e); err != nil {
			if !key.lookupOnly {
				c.entries.Store(id, &entry{stored: e.stored})
			}
			return nil, err
		}
		return e, nil
	}
	if key.lookupOnly {
		if e != nil && e.stored.Digest == "" && e.resolved.IsZero() {
			c.entries.CompareAndDelete(id, e)
		}
		return nil, &CacheError{errdef.ErrNotFound}
	}
	if err := ctx.Err(); err != nil {
		return nil, &CacheError{err}
	}
	var lastGood ocispec.Descriptor
	var count uint32
	if e != nil {
		lastGood = e.stored
		if e.outcome != nil {
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
	result, err := c.fill(ctx, key, produce)
	if err != nil {
		if _, ok := err.(*producerError); !ok {
			err = &CacheError{err}
		} else if _, terminal := errors.AsType[*TerminalError](err); !terminal && retryable(err) && policy.Retry.Attempts <= 1 {
			err = &TerminalError{Err: err, Attempted: true}
		}
		// A canceled winner leaves the due state available to the next live waiter.
		if due && (ctx.Err() != nil || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)) {
			return nil, err
		}
		next := &entry{stored: lastGood}
		if due {
			next.stored, next.outcome = e.stored, e.outcome
		}
		if c.store != nil && ctx.Err() == nil {
			if _, producer := errors.AsType[*producerError](err); producer {
				if o := newOutcome(err, time.Now().UTC(), nextOutcomeCount(count), lastGood); o != nil {
					stored, saveErr := c.publishOutcome(ctx, id, o, err)
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
		c.entries.Store(id, next)
		return nil, err
	}
	if due {
		e.waveResult = result
	}
	c.entries.Store(id, result)
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
	upstream, at, valid := decode(desc, key.Required)
	if valid {
		e.stored = desc
	}
	if valid && fresh(at, maxAge) {
		e.descriptor, e.resolved = upstream, at
		if key.git != nil {
			e.git, err = c.restoreGit(desc, key.git.key)
			if err != nil {
				return &CacheError{err}
			}
			if e.git == nil {
				e.resolved = time.Time{}
			}
		} else if key.archive {
			index := &tarFS{root: c.root, digest: desc.Digest}
			index.identify(desc.Digest, key.ArchiveKey)
			file, err := c.root.Open(blobPath(desc.Digest))
			if err != nil {
				return &CacheError{err}
			}
			e.fs, err = readArchive(file, key.ArchiveKey, index)
			if err := errors.Join(err, file.Close()); err != nil {
				return &CacheError{err}
			}
		} else {
			e.fs = fileFS{name: "content", identity: desc.Digest.String(), size: desc.Size, open: func() (io.ReadCloser, error) {
				return c.root.Open(blobPath(desc.Digest))
			}}
		}
	}
	e.unloaded = valid && e.resolved.IsZero()
	return nil
}

func (c *Cache) touch(e *entry) error {
	if c.store != nil {
		now := time.Now()
		if err := c.root.Chtimes(blobPath(e.stored.Digest), now, now); err != nil {
			return &CacheError{err}
		}
	}
	return nil
}

// Forever retains a successfully resolved pin regardless of elapsed time.
const Forever time.Duration = -1

func fresh(at time.Time, maxAge time.Duration) bool {
	return !at.IsZero() && (maxAge < 0 || maxAge > 0 && time.Since(at) < maxAge)
}

func (c *Cache) fill(ctx context.Context, key acquireKey, produce func(context.Context, io.Writer) (ocispec.Descriptor, error)) (result *entry, resultErr error) {
	var buf bytes.Buffer
	var sink io.Writer = &buf
	var tmp *os.File
	var tmpName string
	var index *tarFS
	var h digest.Digester
	if key.archive {
		h = digest.SHA256.Digester()
		sink = h.Hash()
	}
	if c.store != nil {
		var err error
		tmpName = filepath.Join("oci", "acquire-"+uuid.NewString())
		tmp, err = c.root.OpenFile(tmpName, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o600)
		if err != nil {
			return nil, err
		}
		defer func() {
			var cleanup error
			if tmp != nil {
				cleanup = tmp.Close()
			}
			if tmpName != "" {
				cleanup = errors.Join(cleanup, c.root.Remove(tmpName))
			}
			if cleanup != nil {
				result = nil
				resultErr = errors.Join(resultErr, cleanup)
			}
		}()
		if h == nil {
			h = digest.SHA256.Digester()
		}
		sink = io.MultiWriter(tmp, h.Hash())
		if key.archive {
			index = &tarFS{root: c.root}
		}
	}
	e := &entry{}
	if c.store != nil && key.git != nil {
		defer func() {
			if e.git != nil && e.stored.Digest == "" {
				if cleanup := c.root.RemoveAll(generationPath(e.git.id)); cleanup != nil {
					result = nil
					resultErr = errors.Join(resultErr, cleanup)
				}
			}
		}()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	var err error
	if key.git != nil {
		e.git, e.descriptor, err = c.produceGit(ctx, key.git)
		if err == nil {
			_, err = io.WriteString(sink, e.git.id)
		}
	} else if key.archive {
		e.fs, e.descriptor, err = produceArchive(ctx, key.ArchiveKey, index, func(ctx context.Context, w io.Writer) (ocispec.Descriptor, error) {
			w = io.MultiWriter(w, sink)
			return produce(ctx, w)
		})
	} else {
		e.descriptor, err = produce(ctx, sink)
		if err != nil {
			err = &producerError{err}
		}
	}
	if err != nil {
		return nil, err
	}
	if key.archive {
		e.fs.(*tarFS).identify(h.Digest(), key.ArchiveKey)
	}
	e.resolved = time.Now().UTC()
	if c.store == nil {
		if !key.archive {
			data := buf.Bytes()
			e.fs = fileFS{name: "content", identity: manifest.SHA256Hex(data), size: int64(len(data)), open: func() (io.ReadCloser, error) {
				return io.NopCloser(bytes.NewReader(data)), nil
			}}
		}
		return e, nil
	}
	stored, err := c.publish(ctx, tmp, h.Digest(), e)
	if err != nil {
		return nil, err
	}
	if key.archive {
		index.digest = stored.Digest
		if err := pruneArchive(index, index.nodes, key.ArchiveKey); err != nil {
			return nil, err
		}
		index.list()
	} else {
		e.fs = fileFS{name: "content", identity: stored.Digest.String(), size: stored.Size, open: func() (io.ReadCloser, error) {
			return c.root.Open(blobPath(stored.Digest))
		}}
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

func produceArchive(ctx context.Context, key ArchiveKey, index *tarFS, produce func(context.Context, io.Writer) (ocispec.Descriptor, error)) (fs.FS, ocispec.Descriptor, error) {
	pr, pw := io.Pipe()
	g, gctx := errgroup.WithContext(ctx)
	cancelDone := make(chan struct{})
	stop := context.AfterFunc(ctx, func() {
		_ = pr.CloseWithError(ctx.Err())
		_ = pw.CloseWithError(ctx.Err())
		close(cancelDone)
	})
	var upstream ocispec.Descriptor
	var input fs.FS
	var producerFailure, readerFailure error
	g.Go(func() error {
		if err := gctx.Err(); err != nil {
			return err
		}
		desc, err := produce(gctx, pw)
		upstream = desc
		if re, ok := errors.AsType[*readerError](err); ok {
			err = re.error
		} else if err != nil {
			err = &producerError{err}
			producerFailure = err
		}
		_ = pw.CloseWithError(err)
		return err
	})
	g.Go(func() error {
		var err error
		input, err = readArchive(pr, key, index)
		if err == nil {
			if n, copyErr := io.CopyN(io.Discard, pr, maxTrailingBytes+1); n > maxTrailingBytes {
				err = fmt.Errorf("archive has more than %d bytes after its end marker", maxTrailingBytes)
			} else if !errors.Is(copyErr, io.EOF) {
				err = copyErr
			}
		}
		if err != nil {
			if _, inherited := errors.AsType[*producerError](err); !inherited {
				readerFailure = err
			}
			_ = pr.CloseWithError(&readerError{err})
		} else {
			_ = pr.Close()
		}
		return err
	})
	err := g.Wait()
	if !stop() {
		<-cancelDone
	}
	if err != nil && ctx.Err() != nil {
		return input, upstream, ctx.Err()
	}
	if readerFailure != nil {
		return input, upstream, readerFailure
	}
	if producerFailure != nil {
		return input, upstream, producerFailure
	}
	return input, upstream, err
}

func (c *Cache) publish(ctx context.Context, tmp *os.File, payload digest.Digest, e *entry) (ocispec.Descriptor, error) {
	if _, err := tmp.Seek(0, io.SeekStart); err != nil {
		return ocispec.Descriptor{}, err
	}
	info, err := tmp.Stat()
	if err != nil {
		return ocispec.Descriptor{}, err
	}
	stored := ocispec.Descriptor{MediaType: "application/octet-stream", Digest: payload, Size: info.Size(), Annotations: maps.Clone(e.descriptor.Annotations)}
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
	if err := c.store.Push(ctx, stored, tmp); err != nil {
		if !errors.Is(err, errdef.ErrAlreadyExists) {
			return stored, err
		}
		exists, checkErr := c.store.Exists(ctx, stored)
		if checkErr != nil {
			return stored, checkErr
		}
		if !exists {
			return stored, err
		}
	}
	return stored, nil
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
