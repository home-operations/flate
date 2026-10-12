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

	"github.com/home-operations/flate/internal/keylock"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source/cacheroot"
	"github.com/opencontainers/go-digest"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"golang.org/x/sync/errgroup"
	"oras.land/oras-go/v2/content/oci"
	"oras.land/oras-go/v2/errdef"
)

const prefix = "flate."

const maxTrailingBytes = 1 << 20

// ArchiveKey identifies an archive and its source-ignore policy.
type ArchiveKey struct {
	ID           string
	Ignore       string
	WithDefaults bool
}

// BlobKey identifies single-file content.
type BlobKey struct{ ID string }

// Cache owns acquired generations until Close. Readers MUST finish before Close.
type Cache struct {
	layout  cacheroot.Layout
	store   *oci.Store
	locks   *keylock.KeyMap[string]
	entries sync.Map
}

type entry struct {
	fs         fs.FS
	descriptor ocispec.Descriptor
	stored     ocispec.Descriptor
	resolved   time.Time
	git        *GitGeneration
}

// CacheError identifies an acquisition failure, including failed persistence.
type CacheError struct{ Err error } //nolint:revive // The public acquisition contract names CacheError.

func (e *CacheError) Error() string   { return "cache: " + e.Err.Error() }
func (e *CacheError) Unwrap() []error { return []error{manifest.ErrFlux, e.Err} }

type acquireKey struct {
	ArchiveKey
	archive bool
	git     *gitRequest
}

type producerError struct{ error }

// readerError marks a failure of the archive consumer that the producer only observes as a failed write.
type readerError struct{ error }

func (e *readerError) Unwrap() error     { return e.error }
func (e *producerError) Unwrap() []error { return []error{manifest.ErrFlux, e.error} }

// Acquire publishes a complete archive only after produce succeeds.
func (c *Cache) Acquire(ctx context.Context, key ArchiveKey, maxAge time.Duration, produce func(io.Writer) (ocispec.Descriptor, error)) (fs.FS, ocispec.Descriptor, error) {
	e, err := c.acquire(ctx, acquireKey{ArchiveKey: key, archive: true}, maxAge, produce)
	if err != nil {
		return nil, ocispec.Descriptor{}, err
	}
	return e.fs, e.descriptor, nil
}

// Blob returns an independent reader over a complete generation.
func (c *Cache) Blob(ctx context.Context, key BlobKey, maxAge time.Duration, produce func(io.Writer) (ocispec.Descriptor, error)) (io.ReadCloser, ocispec.Descriptor, error) {
	e, err := c.acquire(ctx, acquireKey{ID: key.ID}, maxAge, produce)
	if err != nil {
		return nil, ocispec.Descriptor{}, err
	}
	input := e.fs.(fileFS)
	r, err := input.Open("content")
	if err != nil {
		return nil, ocispec.Descriptor{}, &CacheError{err}
	}
	return &blobHandle{ReadCloser: r, input: input}, e.descriptor, nil
}

func (c *Cache) acquire(ctx context.Context, key acquireKey, maxAge time.Duration, produce func(io.Writer) (ocispec.Descriptor, error)) (*entry, error) {
	id := key.ID
	release, err := c.locks.Acquire(ctx, id)
	if err != nil {
		return nil, &CacheError{err}
	}
	defer release()
	var e *entry
	if v, ok := c.entries.Load(id); ok {
		e = v.(*entry)
	}
	if e == nil && c.store != nil {
		desc, err := c.store.Resolve(ctx, id)
		if err != nil && !errors.Is(err, errdef.ErrNotFound) {
			return nil, &CacheError{err}
		}
		if err == nil {
			upstream, at, valid := decode(desc, id)
			if valid && fresh(at, maxAge) {
				e = &entry{descriptor: upstream, stored: desc, resolved: at}
				if key.git != nil {
					e.git, err = c.restoreGit(desc, key.git.key)
					if err != nil {
						return nil, &CacheError{err}
					}
					if e.git == nil {
						e = nil
					}
				} else if key.archive {
					index := &tarFS{layout: c.layout, digest: desc.Digest}
					file, err := os.Open(c.layout.OCIBlob(desc.Digest))
					if err != nil {
						return nil, &CacheError{err}
					}
					e.fs, err = readArchive(file, key.ArchiveKey, index)
					closeErr := file.Close()
					if err := errors.Join(err, closeErr); err != nil {
						return nil, &CacheError{err}
					}
				} else {
					e.fs = fileFS{name: "content", size: desc.Size, open: func() (io.ReadCloser, error) {
						return os.Open(c.layout.OCIBlob(desc.Digest))
					}}
				}
			}
		}
	}
	if e != nil && fresh(e.resolved, maxAge) {
		if c.store != nil {
			now := time.Now()
			if err := os.Chtimes(c.layout.OCIBlob(e.stored.Digest), now, now); err != nil {
				c.entries.Store(id, &entry{})
				return nil, &CacheError{err}
			}
		}
		c.entries.Store(id, e)
		return e, nil
	}
	e, err = c.fill(ctx, key, produce)
	if err != nil {
		c.entries.Store(id, &entry{})
		if failure, ok := errors.AsType[*producerError](err); ok {
			return nil, failure
		}
		return nil, &CacheError{err}
	}
	c.entries.Store(id, e)
	return e, nil
}

// Forever retains a successfully resolved pin regardless of elapsed time.
const Forever time.Duration = -1

func fresh(at time.Time, maxAge time.Duration) bool {
	return !at.IsZero() && (maxAge < 0 || maxAge > 0 && time.Since(at) < maxAge)
}

func (c *Cache) fill(ctx context.Context, key acquireKey, produce func(io.Writer) (ocispec.Descriptor, error)) (*entry, error) {
	var buf bytes.Buffer
	var sink io.Writer = &buf
	var tmp *os.File
	var index *tarFS
	var h digest.Digester
	if c.store != nil {
		var err error
		tmp, err = os.CreateTemp(c.layout.OCI(), "acquire-*")
		if err != nil {
			return nil, err
		}
		defer func() { _ = tmp.Close(); _ = os.Remove(tmp.Name()) }()
		h = digest.SHA256.Digester()
		sink = io.MultiWriter(tmp, h.Hash())
		if key.archive {
			index = &tarFS{layout: c.layout}
		}
	}
	e := &entry{}
	if c.store != nil && key.git != nil {
		defer func() {
			if e.git != nil && e.stored.Digest == "" {
				_ = os.RemoveAll(filepath.Dir(c.layout.GitObjects(e.git.id)))
			}
		}()
	}
	var err error
	if key.git != nil {
		e.git, e.descriptor, err = c.produceGit(ctx, key.git)
		if err == nil {
			_, err = io.WriteString(sink, e.git.id)
		}
	} else if key.archive {
		e.fs, e.descriptor, err = produceArchive(ctx, key.ArchiveKey, index, func(w io.Writer) (ocispec.Descriptor, error) {
			if tmp != nil {
				w = io.MultiWriter(w, sink)
			}
			return produce(w)
		})
	} else {
		e.descriptor, err = produce(sink)
		if err != nil {
			err = &producerError{err}
		}
	}
	if err != nil {
		return nil, err
	}
	e.resolved = time.Now().UTC()
	if c.store == nil {
		if !key.archive {
			data := buf.Bytes()
			e.fs = fileFS{name: "content", size: int64(len(data)), open: func() (io.ReadCloser, error) {
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
		e.fs = fileFS{name: "content", size: stored.Size, open: func() (io.ReadCloser, error) {
			return os.Open(c.layout.OCIBlob(stored.Digest))
		}}
	}
	if err := c.store.Tag(ctx, stored, key.ID); err != nil {
		return nil, err
	}
	e.stored = stored
	return e, nil
}

func produceArchive(ctx context.Context, key ArchiveKey, index *tarFS, produce func(io.Writer) (ocispec.Descriptor, error)) (fs.FS, ocispec.Descriptor, error) {
	pr, pw := io.Pipe()
	g, gctx := errgroup.WithContext(ctx)
	stop := context.AfterFunc(gctx, func() { _ = pr.CloseWithError(gctx.Err()); _ = pw.CloseWithError(gctx.Err()) })
	defer stop()
	var upstream ocispec.Descriptor
	var input fs.FS
	g.Go(func() error {
		desc, err := produce(pw)
		upstream = desc
		if re, ok := errors.AsType[*readerError](err); ok {
			err = re.error
		} else if err != nil {
			err = &producerError{err}
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
			_ = pr.CloseWithError(&readerError{err})
		} else {
			_ = pr.Close()
		}
		return err
	})
	err := g.Wait()
	if err != nil && ctx.Err() != nil {
		err = ctx.Err()
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
	return stored, tmp.Close()
}

func decode(stored ocispec.Descriptor, id string) (ocispec.Descriptor, time.Time, bool) {
	a := maps.Clone(stored.Annotations)
	delete(a, ocispec.AnnotationRefName)
	at, err := time.Parse(time.RFC3339Nano, a[prefix+"resolved-at"])
	size, sizeErr := strconv.ParseInt(a[prefix+"upstream-size"], 10, 64)
	d := digest.Digest(a[prefix+"upstream-digest"])
	valid := stored.Digest.Validate() == nil && err == nil && sizeErr == nil && size >= 0 && (d == "" || d.Validate() == nil)
	switch {
	case strings.HasPrefix(id, "source-oci-"), strings.HasPrefix(id, "blob-oci-"):
		valid = valid && d != "" && a[prefix+"revision"] != ""
	case strings.HasPrefix(id, "source-bucket-"):
		count, err := strconv.Atoi(a[prefix+"object-count"])
		valid = valid && err == nil && count >= 0 && a[prefix+"revision"] != ""
	case strings.HasPrefix(id, "blob-chart-"):
		valid = valid && a[prefix+"chart-version"] != "" && a[prefix+"chart-url"] != ""
	}
	mediaType := a[prefix+"upstream-media-type"]
	for _, k := range []string{"resolved-at", "upstream-digest", "upstream-size", "upstream-media-type"} {
		delete(a, prefix+k)
	}
	return ocispec.Descriptor{Digest: d, Size: size, MediaType: mediaType, Annotations: a}, at, valid
}
