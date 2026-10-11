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
	for _, e := range entries {
		rel := strings.TrimPrefix(strings.TrimPrefix(e.Key, b.Prefix), "/")
		if rel == "" {
			rel = filepath.Base(e.Key)
		}
		safe, err := safepath.SafeJoin(filepath.Join(string(filepath.Separator), "flate-archive"), rel, false)
		if err != nil {
			return ocispec.Descriptor{}, fmt.Errorf("bucket key %q: %w", e.Key, err)
		}
		name := strings.TrimPrefix(filepath.ToSlash(safe), "/flate-archive/")
		if err := tw.WriteHeader(&tar.Header{Name: name, Mode: 0o600, Size: e.Size}); err != nil {
			return ocispec.Descriptor{}, err
		}
		obj, err := client.GetObject(ctx, b.BucketName, e.Key, minio.GetObjectOptions{})
		if err != nil {
			return ocispec.Descriptor{}, err
		}
		_, copyErr := io.Copy(tw, obj)
		closeErr := obj.Close()
		if copyErr != nil {
			return ocispec.Descriptor{}, copyErr
		}
		if closeErr != nil {
			return ocispec.Descriptor{}, closeErr
		}
		_, _ = fmt.Fprintf(h, "%s %s\n", e.Key, e.ETag)
	}
	if err := tw.Close(); err != nil {
		return ocispec.Descriptor{}, err
	}
	return ocispec.Descriptor{Annotations: map[string]string{
		"flate.revision":     "sha256:" + hex.EncodeToString(h.Sum(nil)),
		"flate.object-count": strconv.Itoa(len(entries)),
	}}, nil
}
