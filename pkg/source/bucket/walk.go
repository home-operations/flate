package bucket

import (
	"archive/tar"
	"cmp"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"slices"
	"strconv"
	"strings"

	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source/safepath"
	"github.com/minio/minio-go/v7"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/semaphore"
)

var readAheadBytes int64 = 64 << 20

func walkBucket(ctx context.Context, client *minio.Client, b *manifest.Bucket, w io.Writer) (ocispec.Descriptor, error) {
	var entries []minio.ObjectInfo
	for obj := range client.ListObjects(ctx, b.BucketName, minio.ListObjectsOptions{Prefix: b.Prefix, Recursive: true}) {
		if obj.Err != nil {
			return ocispec.Descriptor{}, obj.Err
		}
		if !strings.HasSuffix(obj.Key, "/") {
			entries = append(entries, obj)
		}
	}
	slices.SortFunc(entries, func(a, b minio.ObjectInfo) int { return cmp.Compare(a.Key, b.Key) })
	bodies := make([]chan []byte, len(entries))
	names := make([]string, len(entries))
	for i, e := range entries {
		if e.Size < 0 || e.Size > 1<<30 {
			return ocispec.Descriptor{}, fmt.Errorf("bucket object %q exceeds 1 GiB extract limit", e.Key)
		}
		rel := strings.TrimPrefix(strings.TrimPrefix(e.Key, b.Prefix), "/")
		if rel == "" {
			rel = filepath.Base(e.Key)
		}
		safe, err := safepath.SafeJoin("/flate-archive", rel, false)
		if err != nil {
			return ocispec.Descriptor{}, fmt.Errorf("bucket key %q: %w", e.Key, err)
		}
		names[i] = strings.TrimPrefix(filepath.ToSlash(safe), "/flate-archive/")
		bodies[i] = make(chan []byte, 1)
	}
	// Oversized objects reserve the whole window, keeping the bound at
	// max(readAheadBytes, largest object), within the archive's 1 GiB cap.
	budget := semaphore.NewWeighted(readAheadBytes)
	g, gctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		downloads, dctx := errgroup.WithContext(gctx)
		downloads.SetLimit(8)
		for i, e := range entries {
			if budget.Acquire(dctx, min(e.Size, readAheadBytes)) != nil {
				break
			}
			downloads.Go(func() error {
				body, err := getObject(dctx, client, b.BucketName, e)
				if err == nil {
					bodies[i] <- body
				}
				return err
			})
		}
		return downloads.Wait()
	})
	h := sha256.New()
	g.Go(func() error {
		tw := tar.NewWriter(w)
		for i, e := range entries {
			var body []byte
			select {
			case body = <-bodies[i]:
			case <-gctx.Done():
				return gctx.Err()
			}
			if err := tw.WriteHeader(&tar.Header{Name: names[i], Mode: 0o600, Size: e.Size}); err != nil {
				return err
			}
			if _, err := tw.Write(body); err != nil {
				return err
			}
			budget.Release(min(e.Size, readAheadBytes))
			_, _ = fmt.Fprintf(h, "%s %s\n", e.Key, e.ETag)
		}
		return tw.Close()
	})
	if err := g.Wait(); err != nil {
		return ocispec.Descriptor{}, err
	}

	return ocispec.Descriptor{Annotations: map[string]string{
		"flate.revision":     fmt.Sprintf("sha256:%x", h.Sum(nil)),
		"flate.object-count": strconv.Itoa(len(entries)),
	}}, nil
}

func getObject(ctx context.Context, client *minio.Client, bucket string, e minio.ObjectInfo) ([]byte, error) {
	obj, err := client.GetObject(ctx, bucket, e.Key, minio.GetObjectOptions{})
	if err != nil {
		return nil, err
	}
	data := make([]byte, e.Size)
	if _, err = io.ReadFull(obj, data); err == nil {
		if n, readErr := io.CopyN(io.Discard, obj, 1); n != 0 {
			err = fmt.Errorf("bucket object %q exceeds listed size", e.Key)
		} else if !errors.Is(readErr, io.EOF) {
			err = readErr
		}
	}
	return data, errors.Join(err, obj.Close())
}
