// Package bucket implements the source.Fetcher for KindBucket
// (S3-compatible object storage via minio-go).
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

	sourcev1 "github.com/fluxcd/source-controller/api/v1"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"

	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source"
	"github.com/home-operations/flate/pkg/store"
)

// Fetcher pulls a Flux Bucket CR into the on-disk cache. Only the
// "generic" provider (any S3-API-compatible storage) is wired up
// today via minio-go. The aws/gcp/azure providers parse and route
// here but return a clear "not implemented" error rather than silently
// falling back.
type Fetcher struct {
	Cache   *source.Cache
	Secrets source.SecretGetter
}

// Fetch implements source.Fetcher for *manifest.Bucket.
func (f *Fetcher) Fetch(ctx context.Context, obj manifest.BaseManifest) (*store.SourceArtifact, error) {
	b, ok := obj.(*manifest.Bucket)
	if !ok {
		return nil, fmt.Errorf("%w: Fetcher: unexpected payload %T", manifest.ErrInput, obj)
	}
	if b.Provider != "" && b.Provider != sourcev1.BucketProviderGeneric {
		return nil, fmt.Errorf(
			"bucket %s/%s provider %q is not implemented; flate currently supports only %q (S3-compatible)",
			b.Namespace, b.Name, b.Provider, sourcev1.BucketProviderGeneric,
		)
	}

	creds, err := f.resolveCredentials(b)
	if err != nil {
		return nil, err
	}

	endpoint, secure, err := normalizeEndpoint(b.Endpoint, b.Insecure)
	if err != nil {
		return nil, err
	}

	transport, err := f.resolveTransport(b)
	if err != nil {
		return nil, err
	}

	client, err := minio.New(endpoint, &minio.Options{
		Creds:     creds,
		Secure:    secure,
		Region:    b.Region,
		Transport: transport,
	})
	if err != nil {
		return nil, fmt.Errorf("bucket %s/%s: minio client: %w", b.Namespace, b.Name, err)
	}

	// Cache key: bucket+prefix; reset on first error so retries start
	// clean. The revision identifier (sha256 over sorted etags) is
	// recomputed after listing.
	slot, _, release, err := f.Cache.Slot(endpoint+"/"+b.BucketName, b.Prefix)
	if err != nil {
		return nil, fmt.Errorf("bucket %s/%s cache slot: %w", b.Namespace, b.Name, err)
	}
	defer release()

	keys, revHash, err := walkBucket(ctx, client, b.BucketName, b.Prefix, slot)
	if err != nil {
		_ = f.Cache.Reset(slot)
		return nil, fmt.Errorf("bucket %s/%s walk: %w", b.Namespace, b.Name, err)
	}
	// Bucket uses the no-defaults ignore variant — matches upstream
	// source-controller's bucket_controller.go, which constructs an
	// ignore Matcher without VCS / extension defaults. Buckets are
	// object stores with no VCS semantics; .jpg / .flux.yaml / etc.
	// are legitimate content and must reach the artifact.
	if err := source.ApplyIgnoreNoDefaults(slot, b.Ignore); err != nil {
		_ = f.Cache.Reset(slot)
		return nil, fmt.Errorf("bucket %s/%s: %w", b.Namespace, b.Name, err)
	}

	return &store.SourceArtifact{
		Kind:      manifest.KindBucket,
		URL:       fmt.Sprintf("%s://%s/%s", schemeFor(secure), endpoint, b.BucketName),
		LocalPath: slot,
		Revision:  revHash,
		Metadata: map[string]string{
			"objectCount": fmt.Sprintf("%d", len(keys)),
		},
	}, nil
}

// resolveTransport lives in transport.go (paired with
// transport_test.go).

// resolveCredentials picks up accesskey/secretkey from the SecretRef
// or falls back to anonymous (which is valid for public buckets).
func (f *Fetcher) resolveCredentials(b *manifest.Bucket) (*credentials.Credentials, error) {
	if b.SecretRef == nil {
		return credentials.NewStaticV4("", "", ""), nil
	}
	if f.Secrets == nil {
		return nil, fmt.Errorf("bucket %s/%s references secretRef but no SecretGetter is wired",
			b.Namespace, b.Name)
	}
	sec := f.Secrets(b.Namespace, b.SecretRef.Name)
	if sec == nil {
		return nil, fmt.Errorf("%w: bucket %s/%s: secret %s/%s not found",
			manifest.ErrMissingSecret, b.Namespace, b.Name, b.Namespace, b.SecretRef.Name)
	}
	access := source.StringFromSecret(sec, "accesskey")
	secret := source.StringFromSecret(sec, "secretkey")
	if access == "" || secret == "" {
		// Empty covers both missing-key and PLACEHOLDER-wiped values
		// (the ExternalSecret case). Same sentinel so
		// --allow-missing-secrets covers both shapes.
		return nil, fmt.Errorf("%w: bucket %s/%s: secret %s/%s missing accesskey/secretkey",
			manifest.ErrMissingSecret, b.Namespace, b.Name, b.Namespace, b.SecretRef.Name)
	}
	return credentials.NewStaticV4(access, secret, ""), nil
}

// normalizeEndpoint / schemeFor live in endpoint.go (paired with
// endpoint_test.go).

// walkBucket lists the bucket under prefix, downloads each object
// into slot preserving the prefix-relative path, and returns the
// sorted key list + a content-addressed revision (sha256 of
// "<key>\t<etag>\n" pairs, sorted).
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

	for _, e := range entries {
		rel := strings.TrimPrefix(strings.TrimPrefix(e.key, prefix), "/")
		if rel == "" {
			rel = filepath.Base(e.key)
		}
		dst, err := safeJoinUnderSlot(slot, rel)
		if err != nil {
			return nil, "", fmt.Errorf("bucket key %q: %w", e.key, err)
		}
		if err := os.MkdirAll(filepath.Dir(dst), 0o750); err != nil {
			return nil, "", err
		}
		if err := downloadObject(ctx, client, bucket, e.key, dst); err != nil {
			return nil, "", err
		}
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

// safeJoinUnderSlot lives in traversal.go (paired with
// traversal_test.go).

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
