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

	"github.com/home-operations/flate/pkg/manifest"
	"github.com/opencontainers/go-digest"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"golang.org/x/sync/errgroup"
)

type acquisitionProducer interface {
	identity() string
	produce(context.Context, *Cache, *payload) (*entry, error)
	restore(context.Context, *Cache, ocispec.Descriptor, *entry) error
}

type payload struct {
	writer    io.Writer
	memory    bytes.Buffer
	hash      digest.Digester
	discard   func() error
	key       acquireKey
	previous  *entry
	verified  bool
	unchanged bool
	written   int64
}

// ProducerInput writes new content or exposes the owner's last complete generation.
type ProducerInput struct {
	io.Writer
	cache   *Cache
	payload *payload
}

// Previous returns independent upstream metadata, or nil when no usable payload exists.
// It retains no open reader and MUST only be called during production.
func (p ProducerInput) Previous(ctx context.Context) (*ocispec.Descriptor, error) {
	if err := ctx.Err(); err != nil {
		return nil, &CacheError{err}
	}
	s := p.payload
	if s == nil {
		return nil, nil
	}
	e := s.previous
	if e == nil || e.outcome != nil {
		return nil, nil
	}
	if !s.verified {
		prior := &entry{fs: e.fs, descriptor: e.descriptor, stored: e.stored, resolved: e.resolved}
		if p.cache.store != nil {
			upstream, at, valid := decode(e.stored, s.key.required)
			if !valid {
				s.previous = nil
				return nil, nil
			}
			info, err := p.cache.root.Stat(blobPath(e.stored.Digest))
			if errors.Is(err, fs.ErrNotExist) || err == nil && info.Size() != e.stored.Size {
				s.previous = nil
				return nil, nil
			}
			if err != nil {
				return nil, &CacheError{err}
			}
			prior.descriptor, prior.resolved = upstream, at
			if prior.fs == nil {
				if err := s.key.producer.restore(ctx, p.cache, e.stored, prior); err != nil {
					return nil, &CacheError{err}
				}
			}
		}
		if prior.fs == nil {
			s.previous = nil
			return nil, nil
		}
		s.previous, s.verified = prior, true
		e = prior
	}
	desc := e.descriptor
	desc.Annotations = maps.Clone(desc.Annotations)
	return &desc, nil
}

// ProducerResult reports new bytes or successful validation of the previous payload.
type ProducerResult struct {
	ocispec.Descriptor
	// Unchanged requires a usable previous payload and forbids writing new bytes.
	Unchanged bool
}

func (s *payload) Write(data []byte) (int, error) {
	n, err := s.writer.Write(data)
	s.written += int64(n)
	return n, err
}

func (s *payload) complete(ctx context.Context, c *Cache, result ProducerResult) (*entry, error) {
	e := &entry{descriptor: result.Descriptor}
	if result.Unchanged {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if !s.verified {
			if _, err := (ProducerInput{cache: c, payload: s}).Previous(ctx); err != nil {
				return nil, err
			}
		}
		if !s.verified || s.previous == nil || s.written != 0 {
			return nil, fmt.Errorf("%w: unchanged production requires a previous payload and no new bytes", manifest.ErrInput)
		}
		e.fs, e.stored = s.previous.fs, s.previous.stored
		s.unchanged = true
	}
	return e, nil
}

type acquisitionContent struct{ Archive, Blob, Git any }

type archiveProducer struct {
	key   ArchiveKey
	write func(context.Context, ProducerInput) (ProducerResult, error)
}

func (p archiveProducer) identity() string {
	return manifest.Fingerprint(acquisitionContent{Archive: struct {
		Content        any
		Ignore, Policy string
		Version        int
	}{p.key.Content, p.key.Ignore, p.key.Policy, 1}})
}

func (p archiveProducer) produce(ctx context.Context, c *Cache, staged *payload) (*entry, error) {
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
		produced, err := p.write(gctx, ProducerInput{Writer: io.MultiWriter(staged, pw), cache: c, payload: staged})
		result = produced
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
		_, err := readArchive(pr, p.key, index)
		if err == nil && p.key.Policy == "extract" {
			_, err = io.Copy(io.Discard, pr)
		} else if err == nil {
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
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if result.Unchanged && producerFailure == nil {
		return staged.complete(ctx, c, result)
	}
	if err != nil {
		return nil, cmp.Or(ctx.Err(), readerFailure, producerFailure, err)
	}
	index.identify(staged.hash.Digest(), p.key)
	return &entry{fs: index, descriptor: result.Descriptor}, nil
}

func (p archiveProducer) restore(_ context.Context, c *Cache, stored ocispec.Descriptor, e *entry) error {
	if e.fs != nil {
		index := e.fs.(*tarFS)
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
		return err
	}
	loaded, err := readArchive(file, p.key, index)
	err = errors.Join(err, file.Close())
	if err == nil {
		e.fs = loaded
	}
	return err
}

type blobProducer struct {
	key   BlobKey
	write func(context.Context, ProducerInput) (ProducerResult, error)
}

func (p blobProducer) identity() string {
	if p.key.PersistOnly {
		return blobID(processIdentity, p.key.Content.(string))
	}
	return manifest.Fingerprint(acquisitionContent{Blob: struct{ Content any }{p.key.Content}})
}

func (p blobProducer) produce(ctx context.Context, c *Cache, staged *payload) (*entry, error) {
	result, err := p.write(ctx, ProducerInput{Writer: staged, cache: c, payload: staged})
	if err != nil {
		return nil, &producerError{err}
	}
	e, err := staged.complete(ctx, c, result)
	if err != nil {
		return nil, err
	}
	if staged.unchanged {
		return e, nil
	}
	if c.store == nil {
		data := staged.memory.Bytes()
		e.fs = fileFS{name: "content", identity: digest.FromBytes(data).String(), size: int64(len(data)), open: func() (io.ReadCloser, error) {
			return io.NopCloser(bytes.NewReader(data)), nil
		}}
	}
	return e, nil
}

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
	e.fs = fileFS{name: "content", identity: stored.Digest.String(), size: stored.Size, open: func() (io.ReadCloser, error) {
		info, err := c.root.Stat(root.path)
		if err != nil {
			return nil, err
		}
		if os.SameFile(info, root.info) {
			return root.Open(name)
		}
		current, err := c.root.OpenRoot(root.path)
		if err != nil {
			return nil, err
		}
		defer func() { _ = current.Close() }()
		return current.Open(name)
	}}
	return nil
}
