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
	Name       string `json:"name" yaml:"name"`
	Namespace  string `json:"namespace,omitempty" yaml:"namespace,omitempty"`
	Provider   string `json:"provider,omitempty" yaml:"provider,omitempty"`
	BucketName string `json:"bucketName" yaml:"bucketName"`
	Endpoint   string `json:"endpoint" yaml:"endpoint"`
	Region     string `json:"region,omitempty" yaml:"region,omitempty"`
	Prefix     string `json:"prefix,omitempty" yaml:"prefix,omitempty"`
	Insecure   bool   `json:"insecure,omitempty" yaml:"insecure,omitempty"`
	// SecretRef points at a Secret carrying accesskey / secretkey
	// (generic provider) or the provider-specific credential bag.
	SecretRef *LocalObjectReference `json:"secretRef,omitempty" yaml:"secretRef,omitempty"`
	// CertSecretRef points at a Secret with tls.crt + tls.key
	// (client cert) and/or ca.crt (server CA) for mTLS endpoints.
	CertSecretRef *LocalObjectReference `json:"certSecretRef,omitempty" yaml:"certSecretRef,omitempty"`
	// ProxySecretRef points at a Secret carrying an HTTP proxy
	// configuration (address + optional username/password) used when
	// reaching the bucket endpoint.
	ProxySecretRef *LocalObjectReference `json:"proxySecretRef,omitempty" yaml:"proxySecretRef,omitempty"`
	Suspend        bool                  `json:"-" yaml:"-"`
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
		return nil, inputf("Bucket decode: %v", err)
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
	provider := cr.Spec.Provider
	if provider == "" {
		provider = BucketProviderGeneric
	}
	out := &Bucket{
		Name:       cr.Name,
		Namespace:  cr.Namespace,
		Provider:   provider,
		BucketName: cr.Spec.BucketName,
		Endpoint:   cr.Spec.Endpoint,
		Region:     cr.Spec.Region,
		Prefix:     cr.Spec.Prefix,
		Insecure:   cr.Spec.Insecure,
		Suspend:    cr.Spec.Suspend,
	}
	if cr.Spec.SecretRef != nil && cr.Spec.SecretRef.Name != "" {
		out.SecretRef = &LocalObjectReference{Name: cr.Spec.SecretRef.Name}
	}
	if cr.Spec.CertSecretRef != nil && cr.Spec.CertSecretRef.Name != "" {
		out.CertSecretRef = &LocalObjectReference{Name: cr.Spec.CertSecretRef.Name}
	}
	if cr.Spec.ProxySecretRef != nil && cr.Spec.ProxySecretRef.Name != "" {
		out.ProxySecretRef = &LocalObjectReference{Name: cr.Spec.ProxySecretRef.Name}
	}
	return out, nil
}
