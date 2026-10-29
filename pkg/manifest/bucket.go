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
	Name        string            `json:"name"                yaml:"name"`
	Namespace   string            `json:"namespace,omitempty" yaml:"namespace,omitempty"`
	Labels      map[string]string `json:"-" yaml:"-"`
	Annotations map[string]string `json:"-" yaml:"-"`
	// BucketSpec is the embedded upstream sourcev1.BucketSpec — all
	// fields (Provider, BucketName, Endpoint, Region, Prefix, Insecure,
	// SecretRef, CertSecretRef, ProxySecretRef, Suspend, ...) are
	// promoted to the top level for ergonomic access (b.Endpoint, etc.).
	sourcev1.BucketSpec `json:",inline" yaml:",inline"`
}

// NewBucket normalizes an owned typed Bucket declaration.
func NewBucket(cr sourcev1.Bucket) *Bucket {
	cr.Spec.Provider = cmp.Or(cr.Spec.Provider, sourcev1.BucketProviderGeneric)
	return &Bucket{Name: cr.Name, Namespace: cr.Namespace, Labels: cr.Labels, Annotations: cr.Annotations, BucketSpec: cr.Spec}
}

// GetLabels returns the Bucket's metadata.labels.
func (b *Bucket) GetLabels() map[string]string { return b.Labels }

// GetAnnotations returns the Bucket's metadata.annotations.
func (b *Bucket) GetAnnotations() map[string]string { return b.Annotations }

// Named identifies the Bucket.
func (b *Bucket) Named() NamedResource {
	return NamedResource{Group: kindTable[kindBucket].Group, Kind: KindBucket, Namespace: b.Namespace, Name: b.Name}
}

// BootstrapAlias returns a local copy.
func (b *Bucket) BootstrapAlias(root string) BaseManifest {
	spec := b.BucketSpec
	spec.Endpoint, spec.BucketName = "file://"+root, b.Name
	return NewBucket(sourcev1.Bucket{Name: b.Name, Namespace: b.Namespace, Labels: b.Labels, Annotations: b.Annotations, Spec: spec})
}

// BootstrapFacts returns the borrowed immutable Bucket view inputs.
func (b *Bucket) BootstrapFacts() BootstrapFacts {
	facts := BootstrapFacts{URL: b.Endpoint, DerivedRoot: true}
	if b.Ignore != nil {
		facts.Ignore = *b.Ignore
	}
	return facts
}

// BootstrapInput returns the authored inputs that shape a local Bucket alias.
func (b *Bucket) BootstrapInput() any {
	return struct {
		Content any
		Ignore  string
	}{b.Content(b.Endpoint, !b.Insecure), b.BootstrapFacts().Ignore}
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

// OperationTimeout returns the authored operation timeout or the Flux default.
func (b *Bucket) OperationTimeout() time.Duration {
	return operationTimeout(b.Timeout)
}

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
	out := NewBucket(cr)
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
