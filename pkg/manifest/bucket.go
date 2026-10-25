package manifest

import (
	"cmp"
	"time"

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
	return NamedResource{Group: kindTable[kindBucket].Group, Kind: KindBucket, Namespace: b.Namespace, Name: b.Name}
}

// BootstrapAlias returns a local copy; a nil Git descriptor selects a derived frozen root.
func (b *Bucket) BootstrapAlias(root string) (BaseManifest, *GitRepository) {
	out := *b
	out.Endpoint, out.BucketName = "file://"+root, b.Name
	return &out, nil
}

// Content returns the source inputs for the effective bucket endpoint.
func (b *Bucket) Content(endpoint string, secure bool) any {
	var auth []NamedResource
	b.references(func(ref Reference) bool {
		if ref.Content && ref.Name != "" {
			auth = append(auth, ref.NamedResource)
		}
		return true
	})
	return struct {
		Endpoint   string
		Secure     bool
		BucketName string
		Prefix     string
		Region     string
		Auth       []NamedResource
	}{endpoint, secure, b.BucketName, b.Prefix, b.Region, auth}
}

// WithNamespace returns a shallow namespace copy; borrowed payloads MUST remain immutable.
func (b *Bucket) WithNamespace(namespace string) BaseManifest {
	out := *b
	out.Namespace = namespace
	return &out
}

// ProjectionCompatible reports whether an eligible previous projection can be reused.
func (b *Bucket) ProjectionCompatible(BaseManifest) bool { return true }

// Suspended reports whether reconciliation is paused on this resource.
func (b *Bucket) Suspended() bool { return b.Suspend }

// ExistenceOnly reports whether readiness requires no artifact acquisition.
func (b *Bucket) ExistenceOnly() bool { return false }

// SourceInterval returns the authored source refresh interval.
func (b *Bucket) SourceInterval() time.Duration { return b.Interval.Duration }

// parseBucket decodes a Bucket CR via the source-controller typed
// schema.
func parseBucket(doc map[string]any) (*Bucket, error) {
	var cr sourcev1.Bucket
	if err := decodeCR(doc, &cr, "Bucket", SourceDomain); err != nil {
		return nil, err
	}
	if cr.Spec.BucketName == "" {
		return nil, inputf("Bucket %s/%s missing spec.bucketName", cr.Namespace, cr.Name)
	}
	if cr.Spec.Endpoint == "" {
		return nil, inputf("Bucket %s/%s missing spec.endpoint", cr.Namespace, cr.Name)
	}
	cr.Spec.Provider = cmp.Or(cr.Spec.Provider, sourcev1.BucketProviderGeneric)
	out := &Bucket{
		Name:       cr.Name,
		Namespace:  cr.Namespace,
		BucketSpec: cr.Spec,
	}
	var err error
	out.references(validateSecretReference(out.Named(), &err))
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (b *Bucket) references(yield func(Reference) bool) {
	credentialReferences(yield, b.Namespace, b.SecretRef, b.CertSecretRef, b.ProxySecretRef)
}

func (b *Bucket) kind() KindInfo { return kindTable[kindBucket].KindInfo }
