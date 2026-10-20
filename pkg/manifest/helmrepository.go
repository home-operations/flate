package manifest

import (
	"cmp"
	"strings"
	"time"

	sourcev1 "github.com/fluxcd/source-controller/api/v1"
)

// HelmRepository is the Flux HelmRepository CR. The embedded
// sourcev1.HelmRepositorySpec promotes URL, Type, Provider, Interval,
// Suspend, SecretRef, CertSecretRef, etc. to the top level.
type HelmRepository struct {
	Name      string `json:"name"      yaml:"name"`
	Namespace string `json:"namespace" yaml:"namespace"`

	sourcev1.HelmRepositorySpec `json:",inline" yaml:",inline"`
}

// Named identifies the repo.
func (h *HelmRepository) Named() NamedResource {
	return NamedResource{Group: kindTable[kindHelmRepository].Group, Kind: KindHelmRepository, Namespace: h.Namespace, Name: h.Name}
}

// Suspended reports whether reconciliation is paused on this resource.
func (h *HelmRepository) Suspended() bool { return h.Suspend }

// ExistenceOnly reports whether readiness requires no artifact acquisition.
func (h *HelmRepository) ExistenceOnly() bool { return true }

// SourceInterval returns the authored source refresh interval.
func (h *HelmRepository) SourceInterval() time.Duration { return h.Interval.Duration }

// Content returns the repository inputs that define HTTP acquisitions.
func (h *HelmRepository) Content() any {
	var auth []NamedResource
	for _, ref := range []*LocalObjectReference{h.SecretRef, h.CertSecretRef} {
		if ref != nil && ref.Name != "" {
			auth = append(auth, NamedResource{Kind: KindSecret, Namespace: h.Namespace, Name: ref.Name})
		}
	}
	return struct {
		URL             string
		Auth            []NamedResource
		PassCredentials bool
	}{h.URL, auth, h.PassCredentials}
}

// WithNamespace returns a shallow namespace copy; borrowed payloads MUST remain immutable.
func (h *HelmRepository) WithNamespace(namespace string) BaseManifest {
	out := *h
	out.Namespace = namespace
	return &out
}

// parseHelmRepository decodes a HelmRepository CR via the
// source-controller typed schema.
func parseHelmRepository(doc map[string]any) (*HelmRepository, error) {
	var cr sourcev1.HelmRepository
	if err := decodeCR(doc, &cr, "HelmRepository", SourceDomain); err != nil {
		return nil, err
	}
	if cr.Spec.URL == "" {
		return nil, inputf("HelmRepository missing spec.url")
	}
	cr.Spec.Type = cmp.Or(cr.Spec.Type, sourcev1.HelmRepositoryTypeDefault)
	out := &HelmRepository{
		Name:               cr.Name,
		Namespace:          cr.Namespace,
		HelmRepositorySpec: cr.Spec,
	}
	var err error
	out.references(validateSecretReference(out.Named(), &err))
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (h *HelmRepository) references(yield func(Reference) bool) {
	credentialReferences(yield, h.Namespace, h.SecretRef, h.CertSecretRef)
}

func (h *HelmRepository) kind() KindInfo { return kindTable[kindHelmRepository].KindInfo }

// IsOCI reports whether the repository serves charts through OCI.
func (h *HelmRepository) IsOCI() bool {
	return h.Type == sourcev1.HelmRepositoryTypeOCI || strings.HasPrefix(h.URL, "oci://")
}
