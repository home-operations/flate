package kustomize

import (
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/values"
)

// Prepare clones ks, applies its effective namespace to the render document,
// expands substitutions and applies overlay before rendering or fingerprinting.
// A nil overlay adds no overrides.
// Substitution references contain flat strings, with no parsed YAML tree to cache.
func Prepare(ks *manifest.Kustomization, provider values.Provider, overlay map[string]string) (*manifest.Kustomization, error) {
	ks = ks.Clone()
	if ks.Contents != nil {
		doc := unstructured.Unstructured{Object: ks.Contents}
		if doc.GetNamespace() != ks.Namespace {
			doc.SetNamespace(ks.Namespace)
		}
	}
	if err := values.ExpandPostBuildSubstituteReferenceWithSubstitutions(ks, provider, overlay); err != nil {
		return nil, err
	}
	return ks, nil
}
