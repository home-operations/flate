package bucket

import (
	"archive/tar"
	"bytes"
	"cmp"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"slices"
	"strconv"
	"strings"

	"github.com/minio/minio-go/v7"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/semaphore"

	"github.com/home-operations/flate/internal/engine/source/cache"
	"github.com/home-operations/flate/internal/engine/source/fetcherr"
	"github.com/home-operations/flate/internal/engine/source/safepath"
	"github.com/home-operations/flate/internal/engine/source/sourceignore"
	"github.com/home-operations/flate/pkg/manifest"
)

const (
	chunkBytes       int64 = 8 << 20
	downloads              = 8
	bucketPolicyFile       = ".sourceignore"
)

type bucketOutput struct {
	io.Writer

	work     *manifest.WorkRecord
	previous *ocispec.Descriptor
}

type bucketChunk struct {
	work        *manifest.WorkRecord
	object      *minio.ObjectInfo
	name        string
	start, size int64
	ready       chan io.Reader
	done        chan struct{}
}

func walkBucket(
	ctx context.Context,
	client *minio.Client,
	b *manifest.Bucket,
	w bucketOutput,
) (cache.ProducerResult, error) {
	var entries []minio.ObjectInfo
	for obj := range client.ListObjects(ctx, b.BucketName, minio.ListObjectsOptions{Prefix: b.Prefix, Recursive: true}) {
		if obj.Err != nil {
			return cache.ProducerResult{}, bucketResponseError(obj.Err)
		}
		entries = append(entries, obj)
	}
	ignore, err := readBucketPolicy(ctx, client, b.BucketName, w.work)
	if err != nil {
		return cache.ProducerResult{}, err
	}
	policy := sourceignore.NewRoot(string(ignore), *cmp.Or(b.Ignore, new(string)))
	slices.SortFunc(entries, func(a, b minio.ObjectInfo) int { return cmp.Compare(a.Key, b.Key) })
	names := make([]string, 0, len(entries))
	pairs := make([][2]string, 0, len(entries))
	allETags := true
	for i := range entries {
		e := &entries[i]
		name, err := bucketEntryName(e, b.Prefix)
		switch {
		case err != nil:
			return cache.ProducerResult{}, err
		case name == "" || name == bucketPolicyFile || policy.Match(name, false):
			continue
		}
		entries[len(names)] = *e
		names = append(names, name)
		allETags = allETags && e.ETag != ""
		pairs = append(pairs, [2]string{e.Key, e.ETag})
	}
	entries = entries[:len(names)]
	result := cache.ProducerResult{Annotations: map[string]string{
		"flate.revision":     "sha256:" + manifest.Fingerprint(pairs),
		"flate.object-count": strconv.Itoa(len(entries)),
		"flate.sourceignore": manifest.Fingerprint(ignore),
	}}
	result.Unchanged = allETags && w.previous != nil &&
		w.previous.Annotations["flate.revision"] == result.Annotations["flate.revision"] &&
		w.previous.Annotations["flate.object-count"] == result.Annotations["flate.object-count"] &&
		w.previous.Annotations["flate.sourceignore"] == result.Annotations["flate.sourceignore"]
	if result.Unchanged {
		return result, nil
	}
	ranges := true
	if i := slices.IndexFunc(entries, func(e minio.ObjectInfo) bool { return bucketRangeEligible(&e) }); i >= 0 {
		probe := &bucketChunk{work: w.work, object: &entries[i], size: 1}
		body, header, err := probe.get(ctx, client, b.BucketName)
		if err != nil {
			return cache.ProducerResult{}, err
		}
		_ = body.Close()
		ranges = header.Get("Content-Range") != ""
	}
	chunks := bucketChunks(entries, names, w.work, ranges)
	budget := semaphore.NewWeighted(downloads * chunkBytes)
	g, gctx := errgroup.WithContext(ctx)
	const archiveWorkers = 2
	g.SetLimit(downloads + archiveWorkers)
	g.Go(func() error {
		return cmp.Or(ctx.Err(), w.writeArchive(gctx, ignore, chunks, budget))
	})
	g.Go(func() error {
		for _, c := range chunks {
			if err := budget.Acquire(gctx, c.weight()); err != nil {
				return fmt.Errorf("admit bucket download: %w", err)
			}
			g.Go(func() error { return c.fetch(gctx, client, b.BucketName) })
		}
		return nil
	})
	if err := g.Wait(); err != nil {
		//nolint:wrapcheck // Retain cancellation authority or the producer's HTTP failure classification.
		return cache.ProducerResult{}, cmp.Or(ctx.Err(), err)
	}
	return result, nil
}

func readBucketPolicy(
	ctx context.Context, client *minio.Client, bucket string, work *manifest.WorkRecord,
) ([]byte, error) {
	body, _, _, err := (minio.Core{Client: client}).GetObject(ctx, bucket, bucketPolicyFile, minio.GetObjectOptions{})
	if err != nil {
		if minio.ToErrorResponse(err).Code == "NoSuchKey" {
			return nil, nil
		}
		return nil, bucketResponseError(err)
	}
	data, readErr := io.ReadAll(body)
	if readErr = errors.Join(readErr, body.Close()); readErr != nil {
		return nil, fmt.Errorf("read bucket policy: %w", readErr)
	}
	work.Add(&manifest.WorkCounts{RemoteTransfers: 1})
	return data, nil
}

func (w bucketOutput) writeArchive(
	ctx context.Context, policy []byte, chunks []*bucketChunk, budget *semaphore.Weighted,
) error {
	tw := tar.NewWriter(w.Writer)
	if policy != nil {
		const policyMode = 0o600
		if err := tw.WriteHeader(
			&tar.Header{Name: bucketPolicyFile, Mode: policyMode, Size: int64(len(policy))},
		); err != nil {
			return fmt.Errorf("write bucket policy header: %w", err)
		}
		if _, err := tw.Write(policy); err != nil {
			return fmt.Errorf("write bucket policy: %w", err)
		}
	}
	if err := writeBucketChunks(ctx, tw, chunks, budget); err != nil {
		return err
	}
	return tw.Close() //nolint:wrapcheck // Preserve the failure from the cache-owned archive stream.
}

func (c *bucketChunk) weight() int64 { return min(c.size, chunkBytes) }

func (c *bucketChunk) get(
	ctx context.Context,
	client *minio.Client,
	bucket string,
) (io.ReadCloser, http.Header, error) {
	opts := minio.GetObjectOptions{}
	if c.object.ETag != "" {
		if err := opts.SetMatchETag(c.object.ETag); err != nil {
			return nil, nil, fmt.Errorf("set bucket object etag condition: %w", err)
		}
	}
	if c.size < c.object.Size {
		_ = opts.SetRange(c.start, c.start+c.size-1)
	}
	body, _, header, err := (minio.Core{Client: client}).GetObject(ctx, bucket, c.object.Key, opts)
	if err == nil {
		c.work.Add(&manifest.WorkCounts{RemoteTransfers: 1})
	}
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
	if want := fmt.Sprintf(
		"bytes %d-%d/%d",
		c.start,
		c.start+c.size-1,
		c.object.Size,
	); c.size < c.object.Size &&
		header.Get("Content-Range") != want {
		_ = body.Close()
		return fmt.Errorf(
			"bucket object %q has Content-Range %q, want %q",
			c.object.Key,
			header.Get("Content-Range"),
			want,
		)
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
		return fmt.Errorf("read bucket object chunk: %w", err)
	}
	if n, err := io.CopyN(io.Discard, body, 1); n != 0 {
		return fmt.Errorf("bucket object %q exceeds chunk size", c.object.Key)
	} else if !errors.Is(err, io.EOF) {
		if err != nil {
			return fmt.Errorf("check bucket object chunk length: %w", err)
		}
		return nil
	}
	c.ready <- bytes.NewReader(data)
	return nil
}

func (c *bucketChunk) emit(tw *tar.Writer, body io.Reader) error {
	if c.start == 0 {
		if err := tw.WriteHeader(&tar.Header{Name: c.name, Mode: bucketObjectMode, Size: c.object.Size}); err != nil {
			return fmt.Errorf("write bucket archive header: %w", err)
		}
	}
	_, err := io.Copy(tw, body)
	if err != nil {
		return fmt.Errorf("write bucket archive content: %w", err)
	}
	return nil
}

const bucketObjectMode = 0o600

func bucketEntryName(e *minio.ObjectInfo, prefix string) (string, error) {
	name, err := safepath.ArchiveName(e.Key)
	if err != nil {
		return "", fmt.Errorf("bucket key %q: %w", e.Key, err)
	}
	if !strings.HasPrefix(e.Key, prefix) {
		return "", fmt.Errorf("bucket key %q does not match prefix %q: %w", e.Key, prefix, fs.ErrInvalid)
	}
	if strings.HasSuffix(e.Key, "/") {
		return "", nil
	}
	if name == "." {
		return "", fmt.Errorf("bucket key %q is a non-directory root: %w", e.Key, fs.ErrInvalid)
	}
	if e.Size < 0 {
		return "", fmt.Errorf("bucket object %q has negative size", e.Key)
	}
	return name, nil
}

func bucketRangeEligible(e *minio.ObjectInfo) bool { return e.Size > chunkBytes && e.ETag != "" }

func bucketChunks(entries []minio.ObjectInfo, names []string, work *manifest.WorkRecord, ranges bool) []*bucketChunk {
	var chunks []*bucketChunk
	for i := range entries {
		step := chunkBytes
		if !ranges || !bucketRangeEligible(&entries[i]) {
			step = max(int64(1), entries[i].Size)
		}
		for start := int64(0); start == 0 || start < entries[i].Size; start += step {
			c := &bucketChunk{
				work:   work,
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
	return chunks
}

func writeBucketChunks(ctx context.Context, tw *tar.Writer, chunks []*bucketChunk, budget *semaphore.Weighted) error {
	for _, c := range chunks {
		var body io.Reader
		select {
		case body = <-c.ready:
		case <-ctx.Done():
			return ctx.Err()
		}
		if err := c.emit(tw, body); err != nil {
			return err
		}
		budget.Release(c.weight())
		if c.done != nil {
			close(c.done)
		}
	}
	return nil
}
