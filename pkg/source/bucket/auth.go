package bucket

import (
	"fmt"

	"github.com/minio/minio-go/v7/pkg/credentials"

	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source"
)

// resolveCredentials picks up accesskey/secretkey from the SecretRef
// or falls back to anonymous (which is valid for public buckets).
func (f *Fetcher) resolveCredentials(b *manifest.Bucket) (*credentials.Credentials, error) {
	if b.SecretRef == nil {
		return credentials.NewStaticV4("", "", ""), nil
	}
	if f.Secrets == nil {
		return nil, fmt.Errorf("%s references secretRef but no SecretGetter is wired", source.QualifiedName(manifest.KindBucket, b.Namespace, b.Name))
	}
	sec := f.Secrets(b.Namespace, b.SecretRef.Name)
	if sec == nil {
		return nil, source.MissingSecretErr(b.Named(), b.SecretRef.Name, "not found")
	}
	access, _, err := sec.Value("accesskey")
	if err != nil {
		return nil, fmt.Errorf("%s: secret %s/%s: %w", source.QualifiedName(manifest.KindBucket, b.Namespace, b.Name), b.Namespace, b.SecretRef.Name, err)
	}
	secret, _, err := sec.Value("secretkey")
	if err != nil {
		return nil, fmt.Errorf("%s: secret %s/%s: %w", source.QualifiedName(manifest.KindBucket, b.Namespace, b.Name), b.Namespace, b.SecretRef.Name, err)
	}
	if access == "" || secret == "" || manifest.HasValuePlaceholder(access) || manifest.HasValuePlaceholder(secret) {
		return nil, source.MissingSecretErr(b.Named(), b.SecretRef.Name, "missing accesskey/secretkey")
	}
	return credentials.NewStaticV4(access, secret, ""), nil
}
