package bucket

import (
	"cmp"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"

	"github.com/minio/minio-go/v7"
	"golang.org/x/sync/errgroup"
)

// walkBucket lists the bucket under prefix, downloads each object
// into slot preserving the prefix-relative path, and returns the
// sorted key list + a content-addressed revision (sha256 of
// "<key> <etag>\n" pairs, sorted).
func walkBucket(ctx context.Context, client *minio.Client, bucket, prefix, slot string) ([]string, string, error) {
	type entry struct{ key, etag string }
	var entries []entry

	for obj := range client.ListObjects(ctx, bucket, minio.ListObjectsOptions{
		Prefix: prefix, Recursive: true,
	}) {
		if obj.Err != nil {
			return nil, "", obj.Err
		}
		if strings.HasSuffix(obj.Key, "/") {
			// "Directory" placeholder — skip.
			continue
		}
		entries = append(entries, entry{key: obj.Key, etag: obj.ETag})
	}
	slices.SortFunc(entries, func(a, b entry) int { return cmp.Compare(a.key, b.key) })

	// Download objects in parallel. Bucket fetches are network-bound
	// (GetObject + Copy per key) and minio-go is concurrency-safe.
	// The 8-way limit handles typical S3-style providers without
	// hitting per-account rate caps; tuned narrow because the
	// fetcher's job is one bucket at a time, not bulk transfer.
	// Writes go to distinct paths under slot so no inter-goroutine
	// coordination beyond the errgroup is needed.
	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(8)
	for _, e := range entries {
		g.Go(func() error {
			rel := strings.TrimPrefix(strings.TrimPrefix(e.key, prefix), "/")
			if rel == "" {
				rel = filepath.Base(e.key)
			}
			dst, err := safeJoinUnderSlot(slot, rel)
			if err != nil {
				return fmt.Errorf("bucket key %q: %w", e.key, err)
			}
			if err := os.MkdirAll(filepath.Dir(dst), 0o750); err != nil {
				return err
			}
			return downloadObject(gctx, client, bucket, e.key, dst)
		})
	}
	if err := g.Wait(); err != nil {
		return nil, "", err
	}

	// Format matches source-controller's internal/index/digest.go:
	// `"%s %s\n"` (single space delimiter). Using a tab made flate's
	// revision diverge silently from what a cluster Bucket reports
	// for identical contents — change detection across runs and any
	// readyExpr keyed off status.artifact.revision would never align.
	h := sha256.New()
	keys := make([]string, 0, len(entries))
	for _, e := range entries {
		_, _ = fmt.Fprintf(h, "%s %s\n", e.key, e.etag)
		keys = append(keys, e.key)
	}
	return keys, "sha256:" + hex.EncodeToString(h.Sum(nil)), nil
}

func downloadObject(ctx context.Context, client *minio.Client, bucket, key, dst string) error {
	obj, err := client.GetObject(ctx, bucket, key, minio.GetObjectOptions{})
	if err != nil {
		return err
	}
	defer func() { _ = obj.Close() }()
	f, err := os.Create(dst) //nolint:gosec // dst is composed from cache slot + bucket key
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()
	if _, err := io.Copy(f, obj); err != nil {
		return fmt.Errorf("copy %s: %w", key, err)
	}
	return nil
}
