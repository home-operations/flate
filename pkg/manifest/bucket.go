package manifest

import (
	sourcev1 "github.com/fluxcd/source-controller/api/v1"
)

// Bucket is the Flux Bucket CRD (source.toolkit.fluxcd.io/v1). It
// represents an object-storage bucket that source-controller (or
// flate, when "generic" provider) lists and downloads into a local
// artifact directory.
//
// flate currently implements the "generic" provider only (S3-compatible
// via minio-go). The aws/gcp/azure providers parse correctly but the
// Fetcher returns a clear "provider X not implemented" error.
type Bucket struct {
	Name      string `json:"name"                yaml:"name"`
	Namespace string `json:"namespace,omitempty" yaml:"namespace,omitempty"`
	// BucketSpec is the embedded upstream sourcev1.BucketSpec — all
	// fields (Provider, BucketName, Endpoint, Region, Prefix, Insecure,
	// SecretRef, CertSecretRef, ProxySecretRef, Suspend, ...) are
	// promoted to the top level for ergonomic access (b.Endpoint, etc.).
	sourcev1.BucketSpec `json:",inline" yaml:",inline"`
}

// Named identifies the Bucket.
func (b *Bucket) Named() NamedResource {
	return NamedResource{Kind: KindBucket, Namespace: b.Namespace, Name: b.Name}
}

// Suspended reports whether reconciliation is paused on this resource.
func (b *Bucket) Suspended() bool { return b.Suspend }

// ParseBucket decodes a Bucket CR via the source-controller typed
// schema.
func ParseBucket(doc map[string]any) (*Bucket, error) {
	if err := checkAPIVersion(doc, SourceDomain); err != nil {
		return nil, err
	}
	var cr sourcev1.Bucket
	if err := decodeTyped(doc, &cr); err != nil {
		return nil, inputf("Bucket decode: %w", err)
	}
	if cr.Name == "" {
		return nil, inputf("Bucket missing metadata.name")
	}
	if cr.Spec.BucketName == "" {
		return nil, inputf("Bucket %s/%s missing spec.bucketName", cr.Namespace, cr.Name)
	}
	if cr.Spec.Endpoint == "" {
		return nil, inputf("Bucket %s/%s missing spec.endpoint", cr.Namespace, cr.Name)
	}
	if cr.Spec.Provider == "" {
		cr.Spec.Provider = sourcev1.BucketProviderGeneric
	}
	owner := cr.Namespace + "/" + cr.Name
	if r := cr.Spec.SecretRef; r != nil {
		if err := validateSecretRefName("Bucket", owner, "spec.secretRef", r.Name); err != nil {
			return nil, err
		}
	}
	if r := cr.Spec.CertSecretRef; r != nil {
		if err := validateSecretRefName("Bucket", owner, "spec.certSecretRef", r.Name); err != nil {
			return nil, err
		}
	}
	if r := cr.Spec.ProxySecretRef; r != nil {
		if err := validateSecretRefName("Bucket", owner, "spec.proxySecretRef", r.Name); err != nil {
			return nil, err
		}
	}
	return &Bucket{
		Name:       cr.Name,
		Namespace:  cr.Namespace,
		BucketSpec: cr.Spec,
	}, nil
}
