package bucket

import (
	"archive/tar"
	"bytes"
	"cmp"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"path"
	"slices"
	"strconv"
	"strings"

	"github.com/home-operations/flate/internal/fetcherr"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source/safepath"
	"github.com/minio/minio-go/v7"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/semaphore"
)

const (
	chunkBytes int64 = 8 << 20
	downloads        = 8
)

type bucketChunk struct {
	object      *minio.ObjectInfo
	name        string
	start, size int64
	ready       chan io.Reader
	done        chan struct{}
}

func walkBucket(ctx context.Context, client *minio.Client, b *manifest.Bucket, w io.Writer) (ocispec.Descriptor, error) {
	var entries []minio.ObjectInfo
	for obj := range client.ListObjects(ctx, b.BucketName, minio.ListObjectsOptions{Prefix: b.Prefix, Recursive: true}) {
		if obj.Err != nil {
			return ocispec.Descriptor{}, bucketResponseError(obj.Err)
		}
		entries = append(entries, obj)
	}
	slices.SortFunc(entries, func(a, b minio.ObjectInfo) int { return cmp.Compare(a.Key, b.Key) })
	names := make([]string, 0, len(entries))
	h := sha256.New()
	for _, e := range entries {
		// Validate both coordinates before prefix stripping or marker omission.
		if _, err := safepath.ArchiveName(e.Key); err != nil {
			return ocispec.Descriptor{}, fmt.Errorf("bucket key %q: %w", e.Key, err)
		}
		rel, matched := strings.CutPrefix(e.Key, b.Prefix)
		if !matched {
			return ocispec.Descriptor{}, fmt.Errorf("bucket key %q does not match prefix %q: %w", e.Key, b.Prefix, fs.ErrInvalid)
		}
		if b.Prefix != "" && !strings.HasSuffix(b.Prefix, "/") {
			rel, _ = strings.CutPrefix(rel, "/")
		}
		if rel == "" {
			rel = path.Base(e.Key)
		}
		name, err := safepath.ArchiveName(rel)
		if err != nil {
			return ocispec.Descriptor{}, fmt.Errorf("bucket key %q: %w", e.Key, err)
		}
		if strings.HasSuffix(e.Key, "/") {
			continue
		}
		if name == "." {
			return ocispec.Descriptor{}, fmt.Errorf("bucket key %q is a non-directory root: %w", e.Key, fs.ErrInvalid)
		}
		if e.Size < 0 {
			return ocispec.Descriptor{}, fmt.Errorf("bucket object %q has negative size", e.Key)
		}
		entries[len(names)] = e
		names = append(names, name)
		_, _ = fmt.Fprintf(h, "%s %s\n", e.Key, e.ETag)
	}
	entries = entries[:len(names)]
	ranges := true
	if i := slices.IndexFunc(entries, func(e minio.ObjectInfo) bool { return e.Size > chunkBytes }); i >= 0 {
		probe := &bucketChunk{object: &entries[i], size: 1}
		body, header, err := probe.get(ctx, client, b.BucketName)
		if err != nil {
			return ocispec.Descriptor{}, err
		}
		_ = body.Close()
		ranges = header.Get("Content-Range") != ""
	}
	var chunks []*bucketChunk
	for i := range entries {
		step := chunkBytes
		if !ranges {
			step = max(int64(1), entries[i].Size)
		}
		for start := int64(0); start == 0 || start < entries[i].Size; start += step {
			c := &bucketChunk{
				object: &entries[i], name: names[i],
				start: start, size: min(step, entries[i].Size-start),
				ready: make(chan io.Reader, 1),
			}
			if c.size > chunkBytes {
				c.done = make(chan struct{})
			}
			chunks = append(chunks, c)
		}
	}
	budget := semaphore.NewWeighted(downloads * chunkBytes)
	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(downloads + 2)
	g.Go(func() error {
		tw := tar.NewWriter(w)
		for _, c := range chunks {
			var body io.Reader
			select {
			case body = <-c.ready:
			case <-gctx.Done():
				return gctx.Err()
			}
			if err := c.emit(tw, body); err != nil {
				return cmp.Or(ctx.Err(), err)
			}
			budget.Release(c.weight())
			if c.done != nil {
				close(c.done)
			}
		}
		return tw.Close()
	})
	g.Go(func() error {
		for _, c := range chunks {
			if err := budget.Acquire(gctx, c.weight()); err != nil {
				return err
			}
			g.Go(func() error { return c.fetch(gctx, client, b.BucketName) })
		}
		return nil
	})
	if err := g.Wait(); err != nil {
		return ocispec.Descriptor{}, err
	}
	return ocispec.Descriptor{Annotations: map[string]string{
		"flate.revision":     fmt.Sprintf("sha256:%x", h.Sum(nil)),
		"flate.object-count": strconv.Itoa(len(entries)),
	}}, nil
}

func (c *bucketChunk) weight() int64 { return min(c.size, chunkBytes) }

func (c *bucketChunk) get(ctx context.Context, client *minio.Client, bucket string) (io.ReadCloser, http.Header, error) {
	opts := minio.GetObjectOptions{}
	if err := opts.SetMatchETag(c.object.ETag); err != nil {
		return nil, nil, err
	}
	if c.size < c.object.Size {
		_ = opts.SetRange(c.start, c.start+c.size-1)
	}
	body, _, header, err := (minio.Core{Client: client}).GetObject(ctx, bucket, c.object.Key, opts)
	return body, header, bucketResponseError(err)
}

func bucketResponseError(err error) error {
	status, ok := errors.AsType[minio.ErrorResponse](err)
	if !ok {
		return err
	}
	response := &fetcherr.ResponseError{StatusCode: status.StatusCode, Op: http.MethodGet, Err: err}
	if fetcherr.Classify(status.StatusCode) == fetcherr.Transient {
		// The outcome codec must never persist MinIO messages or unsupported causes.
		response.Message, response.Err = err.Error(), nil
	}
	return response
}

func (c *bucketChunk) fetch(ctx context.Context, client *minio.Client, bucket string) error {
	body, header, err := c.get(ctx, client, bucket)
	if err != nil {
		return err
	}
	if want := fmt.Sprintf("bytes %d-%d/%d", c.start, c.start+c.size-1, c.object.Size); c.size < c.object.Size && header.Get("Content-Range") != want {
		_ = body.Close()
		return fmt.Errorf("bucket object %q has Content-Range %q, want %q", c.object.Key, header.Get("Content-Range"), want)
	}
	defer func() { _ = body.Close() }()
	if c.done != nil {
		// The download context MUST stay alive until the writer finishes the streamed body.
		c.ready <- body
		select {
		case <-c.done:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	data := make([]byte, c.size)
	if _, err := io.ReadFull(body, data); err != nil {
		return err
	}
	if n, err := io.CopyN(io.Discard, body, 1); n != 0 {
		return fmt.Errorf("bucket object %q exceeds chunk size", c.object.Key)
	} else if !errors.Is(err, io.EOF) {
		return err
	}
	c.ready <- bytes.NewReader(data)
	return nil
}

func (c *bucketChunk) emit(tw *tar.Writer, body io.Reader) error {
	if c.start == 0 {
		if err := tw.WriteHeader(&tar.Header{Name: c.name, Mode: 0o600, Size: c.object.Size}); err != nil {
			return err
		}
	}
	_, err := io.Copy(tw, body)
	return err
}
