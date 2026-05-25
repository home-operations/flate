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

// RepoName is "<namespace>-<name>".
func (h *HelmRepository) RepoName() string { return h.Namespace + "-" + h.Name }

// parseHelmRepository decodes a HelmRepository CR via the
// source-controller typed schema.
func parseHelmRepository(doc map[string]any) (*HelmRepository, error) {
	if err := checkAPIVersion(doc, SourceDomain); err != nil {
		return nil, err
	}
	var cr sourcev1.HelmRepository
	if err := decodeTyped(doc, &cr); err != nil {
		return nil, inputf("HelmRepository decode: %w", err)
	}
	if cr.Name == "" {
		return nil, inputf("HelmRepository missing metadata.name")
	}
	if cr.Spec.URL == "" {
		return nil, inputf("HelmRepository missing spec.url")
	}
	cr.Spec.Type = cmp.Or(cr.Spec.Type, RepoTypeDefault)
	owner := cr.Namespace + "/" + cr.Name
	if r := cr.Spec.SecretRef; r != nil {
		if err := validateSecretRefName("HelmRepository", owner, "spec.secretRef", r.Name); err != nil {
			return nil, err
		}
	}
	if r := cr.Spec.CertSecretRef; r != nil {
		if err := validateSecretRefName("HelmRepository", owner, "spec.certSecretRef", r.Name); err != nil {
			return nil, err
		}
	}
	return &HelmRepository{
		Name:               cr.Name,
		Namespace:          cr.Namespace,
		HelmRepositorySpec: cr.Spec,
	}, nil
}
