package cache

import (
	"bytes"
	"cmp"
	"context"
	"errors"
	"fmt"
	"io"
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
	writer  io.Writer
	memory  bytes.Buffer
	hash    digest.Digester
	discard func() error
}

type acquisitionContent struct{ Archive, Blob, Git any }

type archiveProducer struct {
	key   ArchiveKey
	write func(context.Context, io.Writer) (ocispec.Descriptor, error)
}

func (p archiveProducer) identity() string {
	key := p.key
	key.Required = nil
	return manifest.Fingerprint(acquisitionContent{Archive: key})
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
	var upstream ocispec.Descriptor
	var producerFailure, readerFailure error
	g.Go(func() error {
		if err := gctx.Err(); err != nil {
			return err
		}
		desc, err := p.write(gctx, io.MultiWriter(pw, staged.writer))
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
		_, err := readArchive(pr, p.key, index)
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
	if err != nil {
		return nil, cmp.Or(ctx.Err(), readerFailure, producerFailure, err)
	}
	index.identify(staged.hash.Digest(), p.key)
	return &entry{fs: index, descriptor: upstream}, nil
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
	write func(context.Context, io.Writer) (ocispec.Descriptor, error)
}

func (p blobProducer) identity() string {
	if p.key.PersistOnly {
		return blobID(processIdentity, p.key.Content.(string))
	}
	return manifest.Fingerprint(acquisitionContent{Blob: struct{ Content any }{p.key.Content}})
}

func (p blobProducer) produce(ctx context.Context, c *Cache, staged *payload) (*entry, error) {
	descriptor, err := p.write(ctx, staged.writer)
	if err != nil {
		return nil, &producerError{err}
	}
	e := &entry{descriptor: descriptor}
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
