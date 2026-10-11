package bucket

import (
	"archive/tar"
	"cmp"
	"context"
	"crypto/sha256"
	"encoding/hex"
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
)

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
	h := sha256.New()
	tw := tar.NewWriter(w)
	for batch := range slices.Chunk(entries, 8) {
		g, gctx := errgroup.WithContext(ctx)
		readers := make([]*io.PipeReader, len(batch))
		writers := make([]*io.PipeWriter, len(batch))
		for i := range batch {
			readers[i], writers[i] = io.Pipe()
		}
		stop := context.AfterFunc(gctx, func() {
			for i := range readers {
				_ = readers[i].CloseWithError(gctx.Err())
				_ = writers[i].CloseWithError(gctx.Err())
			}
		})
		for i, e := range batch {
			g.Go(func() error {
				obj, err := client.GetObject(gctx, b.BucketName, e.Key, minio.GetObjectOptions{})
				if err == nil {
					_, err = io.Copy(writers[i], obj)
					closeErr := obj.Close()
					if err == nil {
						err = closeErr
					}
				}
				_ = writers[i].CloseWithError(err)
				return err
			})
		}
		g.Go(func() error {
			for i, e := range batch {
				rel := strings.TrimPrefix(strings.TrimPrefix(e.Key, b.Prefix), "/")
				if rel == "" {
					rel = filepath.Base(e.Key)
				}
				safe, err := safepath.SafeJoin(filepath.Join(string(filepath.Separator), "flate-archive"), rel, false)
				if err != nil {
					return fmt.Errorf("bucket key %q: %w", e.Key, err)
				}
				name := strings.TrimPrefix(filepath.ToSlash(safe), "/flate-archive/")
				if err := tw.WriteHeader(&tar.Header{Name: name, Mode: 0o600, Size: e.Size}); err != nil {
					return err
				}
				if _, err := io.Copy(tw, readers[i]); err != nil {
					return err
				}
				_ = readers[i].Close()
				_, _ = fmt.Fprintf(h, "%s %s\n", e.Key, e.ETag)
			}
			return nil
		})
		err := g.Wait()
		stop()
		if err != nil {
			return ocispec.Descriptor{}, err
		}
	}

	if err := tw.Close(); err != nil {
		return ocispec.Descriptor{}, err
	}
	return ocispec.Descriptor{Annotations: map[string]string{
		"flate.revision":     "sha256:" + hex.EncodeToString(h.Sum(nil)),
		"flate.object-count": strconv.Itoa(len(entries)),
	}}, nil
}
