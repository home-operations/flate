package manifest

import (
	"cmp"

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
	return NamedResource{Kind: KindHelmRepository, Namespace: h.Namespace, Name: h.Name}
}

// RepoName delegates to the canonical NamedResource helper.
func (h *HelmRepository) RepoName() string { return h.Named().FluxResourceName() }

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
	cr.Spec.Type = cmp.Or(cr.Spec.Type, RepoTypeDefault)
	owner := cr.Namespace + "/" + cr.Name
	if err := validateOptionalRefs("HelmRepository", owner,
		secretRefCheck{Field: "spec.secretRef", Ref: cr.Spec.SecretRef},
		secretRefCheck{Field: "spec.certSecretRef", Ref: cr.Spec.CertSecretRef},
	); err != nil {
		return nil, err
	}
	return &HelmRepository{
		Name:               cr.Name,
		Namespace:          cr.Namespace,
		HelmRepositorySpec: cr.Spec,
	}, nil
}
