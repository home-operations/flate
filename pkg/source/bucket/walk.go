package bucket

import (
	"archive/tar"
	"cmp"
	"context"
	"crypto/sha256"
	"encoding/hex"
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
	results := make([]struct {
		name string
		data []byte
		done chan struct{}
	}, len(entries))
	for i, e := range entries {
		if e.Size < 0 || e.Size > 1<<30 {
			return ocispec.Descriptor{}, fmt.Errorf("bucket object %q exceeds 1 GiB extract limit", e.Key)
		}
		rel := strings.TrimPrefix(strings.TrimPrefix(e.Key, b.Prefix), "/")
		if rel == "" {
			rel = filepath.Base(e.Key)
		}
		safe, err := safepath.SafeJoin(filepath.Join(string(filepath.Separator), "flate-archive"), rel, false)
		if err != nil {
			return ocispec.Descriptor{}, fmt.Errorf("bucket key %q: %w", e.Key, err)
		}
		results[i].name = strings.TrimPrefix(filepath.ToSlash(safe), "/flate-archive/")
		results[i].done = make(chan struct{})
	}
	// Oversized objects reserve the whole window, keeping the bound at
	// max(readAheadBytes, largest object), within the archive's 1 GiB cap.
	budget := semaphore.NewWeighted(readAheadBytes)
	g, gctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		downloads, dctx := errgroup.WithContext(gctx)
		downloads.SetLimit(8)
		var admissionErr error
		for i, e := range entries {
			if admissionErr = budget.Acquire(dctx, min(e.Size, readAheadBytes)); admissionErr != nil {
				break
			}
			downloads.Go(func() error {
				obj, err := client.GetObject(dctx, b.BucketName, e.Key, minio.GetObjectOptions{})
				if err != nil {
					return err
				}
				data := make([]byte, e.Size)
				_, err = io.ReadFull(obj, data)
				if err == nil {
					if n, readErr := io.CopyN(io.Discard, obj, 1); n != 0 {
						err = fmt.Errorf("bucket object %q exceeds listed size", e.Key)
					} else if !errors.Is(readErr, io.EOF) {
						err = readErr
					}
				}
				if err := errors.Join(err, obj.Close()); err != nil {
					return err
				}
				results[i].data = data
				close(results[i].done)
				return nil
			})
		}
		if err := downloads.Wait(); err != nil {
			return err
		}
		return admissionErr
	})
	h := sha256.New()
	g.Go(func() error {
		tw := tar.NewWriter(w)
		for i, e := range entries {
			select {
			case <-results[i].done:
			case <-gctx.Done():
				return gctx.Err()
			}
			if err := tw.WriteHeader(&tar.Header{Name: results[i].name, Mode: 0o600, Size: e.Size}); err != nil {
				return err
			}
			if _, err := tw.Write(results[i].data); err != nil {
				return err
			}
			results[i].data = nil
			budget.Release(min(e.Size, readAheadBytes))
			_, _ = fmt.Fprintf(h, "%s %s\n", e.Key, e.ETag)
		}
		return tw.Close()
	})
	if err := g.Wait(); err != nil {
		if ctx.Err() != nil {
			err = ctx.Err()
		}
		return ocispec.Descriptor{}, err
	}

	return ocispec.Descriptor{Annotations: map[string]string{
		"flate.revision":     "sha256:" + hex.EncodeToString(h.Sum(nil)),
		"flate.object-count": strconv.Itoa(len(entries)),
	}}, nil
}
