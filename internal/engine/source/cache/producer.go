package cache

import (
	"bytes"
	"cmp"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"maps"
	"os"
	"path/filepath"
	"slices"

	"github.com/google/uuid"
	"github.com/opencontainers/go-digest"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"golang.org/x/sync/errgroup"

	"github.com/home-operations/flate/pkg/manifest"
)

type acquisitionCallbacks struct {
	produce  func(context.Context, *Cache, *payload) (*entry, error)
	validate func(context.Context, *entry) (bool, error)
	observe  func(context.Context, *entry) (bool, error)
}

type acquisitionProducer interface {
	identity() string
	restore(context.Context, *Cache, ocispec.Descriptor, *entry) error
}

const renderSpoolLimit = 32 << 10

type payload struct {
	writer    io.Writer
	disk      *Cache
	file      *os.File
	name      string
	memory    bytes.Buffer
	hash      digest.Digester
	discard   func() error
	key       acquireKey
	previous  *entry
	attempted bool
	verified  bool
	unchanged bool
	written   int64
}

// ProducerInput writes new content or exposes the owner's last complete generation.
type ProducerInput struct {
	io.Writer

	cache   *Cache
	payload *payload
	work    *manifest.AcquisitionWork
}

// Previous returns independent upstream metadata, or nil when no usable payload exists.
// It retains no open reader and MUST only be called during production.
func (p ProducerInput) Previous(ctx context.Context) (*ocispec.Descriptor, error) {
	if err := ctx.Err(); err != nil {
		return nil, &Error{err}
	}
	s := p.payload
	if s == nil {
		return nil, nil //nolint:nilnil // Without a payload, Previous supplies no prior metadata to the producer.
	}
	e := s.previous
	if e == nil || e.outcome != nil {
		return nil, nil //nolint:nilnil // Absent or failed prior content requires fresh production.
	}
	if !s.verified {
		prior, err := p.restorePrevious(ctx)
		if err != nil {
			return nil, err
		}
		if prior == nil || prior.fs == nil {
			s.previous = nil
			return nil, nil //nolint:nilnil // Missing prior content requires a fresh producer result.
		}
		s.previous, s.verified = prior, true
		e = prior
	}
	desc := e.descriptor
	desc.Annotations = maps.Clone(desc.Annotations)
	return &desc, nil
}

func (p ProducerInput) restorePrevious(ctx context.Context) (*entry, error) {
	s := p.payload
	e := s.previous
	prior := &entry{fs: e.fs, descriptor: e.descriptor, stored: e.stored, resolved: e.resolved}
	if p.cache.store == nil || s.key.memoryOnly {
		return prior, nil
	}
	upstream, at, valid := decode(&e.stored, s.key.required)
	if !valid {
		s.previous = nil
		return nil, nil //nolint:nilnil // Invalid stored metadata clears the prior payload and requires fresh production.
	}
	info, err := p.cache.root.Stat(blobPath(e.stored.Digest))
	if errors.Is(err, fs.ErrNotExist) || err == nil && info.Size() != e.stored.Size {
		s.previous = nil
		return nil, nil //nolint:nilnil // Missing or truncated prior content requires fresh production.
	}
	if err != nil {
		return nil, &Error{err}
	}
	prior.descriptor, prior.resolved = upstream, at
	if prior.fs == nil {
		if err := s.key.producer.restore(ctx, p.cache, e.stored, prior); err != nil {
			return nil, &Error{err}
		}
	}
	return prior, nil
}

// ProducerResult reports new bytes or successful validation of the previous payload.
type ProducerResult struct {
	ocispec.Descriptor

	// Unchanged requires a usable previous payload and forbids writing new bytes.
	Unchanged bool
}

func (s *payload) Write(data []byte) (int, error) {
	buffering := s.disk != nil && s.key.render && s.file == nil
	if buffering && len(data) == 0 {
		return 0, nil
	}
	if buffering && len(data) > renderSpoolLimit-s.memory.Len() {
		if err := s.stage(); err != nil {
			return 0, &Error{err}
		}
	}
	if buffering && s.file == nil {
		buffered := s.memory.Bytes()
		needed := len(buffered) + len(data)
		if needed > cap(buffered) {
			grown := make([]byte, len(buffered), min(renderSpoolLimit, max(needed, cap(buffered)+cap(buffered))))
			copy(grown, buffered)
			s.memory = *bytes.NewBuffer(grown)
		}
		if s.writer == nil {
			s.hash = digest.SHA256.Digester()
			s.writer = io.MultiWriter(&s.memory, s.hash.Hash())
		}
	}
	if s.writer == nil {
		if err := s.stage(); err != nil {
			return 0, &Error{err}
		}
	}
	n, err := s.writer.Write(data)
	s.written += int64(n)
	return n, err //nolint:wrapcheck // payload.Write forwards the staging writer error and byte count to its producer.
}

func (s *payload) stage() error {
	name := acquirePath(uuid.NewString())
	file, err := s.disk.root.OpenFile(name, os.O_RDWR|os.O_CREATE|os.O_EXCL, privateFileMode)
	if err != nil {
		return err //nolint:wrapcheck // payload.stage forwards the staging OpenFile PathError.
	}
	s.file, s.name = file, name
	if s.hash == nil {
		s.hash = digest.SHA256.Digester()
	}
	s.writer = io.MultiWriter(file, s.hash.Hash())
	if _, err := bytes.NewReader(s.memory.Bytes()).WriteTo(file); err != nil {
		return err //nolint:wrapcheck // payload.stage preserves the buffered prefix writer error.
	}
	s.memory = bytes.Buffer{}
	return nil
}

func (s *payload) complete(ctx context.Context, c *Cache, resultValue *ProducerResult) (*entry, error) {
	result := *resultValue
	e := &entry{descriptor: result.Descriptor}
	if !result.Unchanged {
		return e, nil
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if !s.verified {
		if _, err := (ProducerInput{cache: c, payload: s}).Previous(ctx); err != nil {
			return nil, err
		}
	}
	if !s.verified || s.previous == nil || s.written != 0 {
		return nil, manifest.NewError(
			manifest.ErrInput,
			nil,
			"unchanged production requires a previous payload and no new bytes",
		)
	}
	e.fs, e.stored = s.previous.fs, s.previous.stored
	s.unchanged = true
	return e, nil
}

type acquisitionContent struct{ Archive, Blob, Git any }

type archiveProducer struct {
	key ArchiveKey
}

func (p archiveProducer) identity() string {
	return manifest.Fingerprint(acquisitionContent{Archive: struct {
		Content        any
		Ignore, Policy string
		Version        int
	}{p.key.Content, p.key.Ignore, p.key.Policy, p.key.shapeVersion()}})
}

func (p archiveProducer) produce(
	ctx context.Context,
	c *Cache,
	staged *payload,
	write func(context.Context, ProducerInput) (ProducerResult, error),
) (*entry, error) {
	if staged.hash == nil {
		staged.hash = digest.SHA256.Digester()
		staged.writer = staged.hash.Hash()
	}
	index := &tarFS{root: c.root}
	pr, pw := io.Pipe()
	g, gctx := errgroup.WithContext(ctx)
	cancelDone := make(chan struct{})
	stop := context.AfterFunc(ctx, func() {
		_ = pr.CloseWithError(ctx.Err())
		_ = pw.CloseWithError(ctx.Err())
		close(cancelDone)
	})
	var result ProducerResult
	var producerFailure, readerFailure error
	g.Go(func() error {
		if err := gctx.Err(); err != nil {
			return err
		}
		produced, err := write(
			gctx,
			ProducerInput{Writer: io.MultiWriter(staged, pw), cache: c, payload: staged, work: staged.key.policy.Work},
		)
		result = produced
		if re, ok := errors.AsType[*readerError](err); ok {
			err = re.error
		} else if err != nil {
			err = newProducerError(err)
			producerFailure = err
		}
		_ = pw.CloseWithError(err)
		return err
	})
	g.Go(func() error {
		failures := p.readPayload(pr, index)

		readerFailure = failures.reader
		return failures.archive
	})
	err := g.Wait()
	if !stop() {
		<-cancelDone
	}
	if errErr := ctx.Err(); errErr != nil {
		if readerFailure != nil && !errors.Is(readerFailure, errErr) {
			return nil, errors.Join(errErr, readerFailure)
		}
		return nil, errErr
	}
	if result.Unchanged && producerFailure == nil {
		return staged.complete(ctx, c, &result)
	}
	if err != nil {
		//nolint:wrapcheck // Preserve producer and stream errors for replay and I/O sentinel handling.
		return nil, cmp.Or(
			ctx.Err(),
			readerFailure,
			producerFailure,
			err,
		)
	}
	index.identify(staged.hash.Digest(), p.key)
	return &entry{fs: index, descriptor: result.Descriptor}, nil
}

//nolint:gocritic // A value descriptor preserves its snapshot without allocating through the dynamic producer interface.
func (p archiveProducer) restore(_ context.Context, c *Cache, stored ocispec.Descriptor, e *entry) error {
	if e.fs != nil {
		index := e.fs.(*tarFS) //nolint:errcheck // archiveProducer restores *tarFS generations; corruption must panic.
		index.digest = stored.Digest
		if err := pruneArchive(index, index.nodes, p.key); err != nil {
			return err
		}
		index.list()
		return nil
	}
	index := &tarFS{root: c.root, digest: stored.Digest}
	index.identify(stored.Digest, p.key)
	file, err := c.root.Open(blobPath(stored.Digest))
	if err != nil {
		return err //nolint:wrapcheck // archiveProducer.restore forwards the rooted blob Open PathError.
	}
	loaded, err := readArchive(file, p.key, index)
	err = errors.Join(err, file.Close())
	if err == nil {
		e.fs = loaded
	}
	return err
}

type blobProducer struct {
	content any
	render  RenderMode
}

func (p blobProducer) identity() string {
	if p.render != 0 {
		return blobID(
			processIdentity,
			p.content.(string), //nolint:errcheck // A mismatched cache content type must retain its invariant panic.
			p.render,
		)
	}
	return manifest.Fingerprint(acquisitionContent{Blob: struct{ Content any }{p.content}})
}

func (p blobProducer) produce(
	ctx context.Context,
	c *Cache,
	staged *payload,
	write func(context.Context, ProducerInput) (ProducerResult, error),
) (*entry, error) {
	result, err := write(ctx, ProducerInput{Writer: staged, cache: c, payload: staged, work: staged.key.policy.Work})
	if err != nil {
		return nil, newProducerError(err)
	}
	e, err := staged.complete(ctx, c, &result)
	if err != nil {
		return nil, err
	}
	if staged.unchanged {
		return e, nil
	}
	if c.store == nil || staged.key.memoryOnly {
		data := staged.memory.Bytes()
		e.fs = fileFS{
			name:     payloadName,
			identity: digest.FromBytes(data).String(),
			size:     int64(len(data)),
			open: func() (io.ReadCloser, error) {
				return memoryBlob{bytes.NewReader(data)}, nil
			},
		}
	}
	return e, nil
}

//nolint:gocritic // A value descriptor preserves its snapshot without allocating through the dynamic producer interface.
func (p blobProducer) restore(_ context.Context, c *Cache, stored ocispec.Descriptor, e *entry) error {
	algorithm := stored.Digest.Algorithm()
	root := &c.blobRoots[slices.Index(blobAlgorithms[:], algorithm)]
	root.once.Do(func() {
		root.path = filepath.Join(blobDir(), algorithm.String())
		root.Root, root.err = c.root.OpenRoot(root.path)
		if root.err == nil {
			root.info, root.err = root.Stat(".")
		}
	})
	if root.err != nil {
		return root.err
	}
	name := stored.Digest.Encoded()
	e.fs = fileFS{
		backingPath: blobPath(stored.Digest),
		name:        payloadName,
		identity:    stored.Digest.String(),
		size:        stored.Size,
		open: func() (io.ReadCloser, error) {
			current, temporary, err := root.current(c.root)
			if err != nil {
				return nil, err
			}
			if temporary {
				defer func() { _ = current.Close() }()
			}
			return current.Open(name)
		},
	}
	return nil
}

func (p archiveProducer) readPayload(pr *io.PipeReader, index *tarFS) payloadErrors {
	_, err := readArchive(pr, p.key, index)
	if err == nil && p.key.Policy == extractPolicy {
		_, err = io.Copy(io.Discard, pr)
	} else if err == nil {
		if n, copyErr := io.CopyN(io.Discard, pr, maxTrailingBytes+1); n > maxTrailingBytes {
			err = fmt.Errorf("archive has more than %d bytes after its end marker", maxTrailingBytes)
		} else if !errors.Is(copyErr, io.EOF) {
			err = copyErr
		}
	}
	var readerFailure error
	if err != nil {
		if _, inherited := errors.AsType[*producerError](err); !inherited {
			readerFailure = err
		}
		_ = pr.CloseWithError(&readerError{err})
	} else {
		_ = pr.Close()
	}
	return payloadErrors{reader: readerFailure, archive: err}
}

type payloadErrors struct{ reader, archive error }

// Work returns the physical acquisition counters borrowed from the caller.
func (p ProducerInput) Work() *manifest.AcquisitionWork { return p.work }
