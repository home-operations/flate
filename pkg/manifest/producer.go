package manifest

import (
	"cmp"
	"maps"
	"slices"
	"strings"
)

// ProducerTarget identifies an object a cluster-side producer declares and its known Secret keys.
//
// DeclaredKeys contains sorted, deduplicated, statically declared Secret keys for
// offline placeholders. Remote dataFrom-only ExternalSecrets leave it nil because
// their keys are unknowable offline. ObjectBucketClaim keys also remain nil because
// the provisioner's fixed keys are outside the byte-verified scope. Such outputs
// remain unreadable and can still produce the unresolved-substitution advisory.
type ProducerTarget struct {
	NamedResource
	DeclaredKeys []string
}

// ProducerTargets returns fixed outputs and matching concrete candidates declared by obj.
// Predictions establish identity only; they provide no generated content.
// ArtifactGenerator contributes fixed outputs and compatible concrete candidates.
//
// ExternalSecret produces target.name, defaulting to its own name. Nonempty
// target.template.data defines the output keys; otherwise data[].secretKey supplies
// them. dataFrom feeds remote inputs and cannot declare their keys offline.
// SealedSecret produces template.metadata.name, defaulting to its own name, with
// encryptedData keys. ObjectBucketClaim produces a same-named Secret and ConfigMap
// for credentials and connection settings, without declaring their keys.
//
// Targets use the producer's effective namespace. Inference reads raw declared
// names: kustomize namePrefix, nameSuffix and replacements are not followed because
// those generated identities have no nameReference fieldSpec. Transformed targets
// can therefore be missed, but cannot create a false match.
func ProducerTargets(obj BaseManifest, candidates ...NamedResource) []ProducerTarget {
	return obj.producerTargets(candidates)
}

// ProducerNeedsCandidates reports whether obj declares outputs with dynamic names.
func ProducerNeedsCandidates(obj BaseManifest) bool {
	r, ok := obj.(*RawObject)
	return ok && r.producerNeedsCandidates()
}

func (r *RawObject) producerNeedsCandidates() bool {
	if !r.isArtifactGenerator() {
		return false
	}
	entries, _ := r.Spec["artifacts"].([]any)
	for _, entry := range entries {
		m, _ := entry.(map[string]any)
		if name, _ := m["name"].(string); strings.ContainsAny(name, "{}") {
			return true
		}
	}
	return false
}

func (r *RawObject) producerTargets(candidates []NamedResource) []ProducerTarget {
	spec := specView{r.Spec}
	switch {
	case r.isArtifactGenerator():
		return r.artifactTargets(candidates)
	case r.Kind == "ExternalSecret" && apiGroup(r.APIVersion) == "external-secrets.io":
		name := cmp.Or(spec.child("target").str("name"), r.Name)
		// Template output keys take precedence over explicit data inputs; an empty
		// template falls back to data[].secretKey, never remote dataFrom keys.
		keys := spec.child("target").child("template").mapKeys("data")
		if keys == nil {
			keys = spec.entryKeys("data", "secretKey")
		}
		return []ProducerTarget{r.secret(name, keys)}
	case r.Kind == "SealedSecret" && apiGroup(r.APIVersion) == "bitnami.com":
		name := cmp.Or(spec.child("template").child("metadata").str("name"), r.Name)
		return []ProducerTarget{r.secret(name, spec.mapKeys("encryptedData"))}
	case r.Kind == "ObjectBucketClaim" && apiGroup(r.APIVersion) == "objectbucket.io":
		return []ProducerTarget{r.secret(r.Name, nil), r.target(KindConfigMap, r.Name, nil)}
	default:
		return nil
	}
}

func (r *RawObject) secret(name string, declaredKeys []string) ProducerTarget {
	return r.target(KindSecret, name, declaredKeys)
}

func (r *RawObject) target(kind, name string, declaredKeys []string) ProducerTarget {
	return ProducerTarget{
		Kind: kind, Namespace: r.Namespace, Name: name,
		DeclaredKeys: declaredKeys,
	}
}

// specView is a typed, nil-safe read-only window onto a producer's decoded-YAML
// spec (map[string]any). It confines the unavoidable any-grubbing to these few
// accessors so the classifier reads as plain field access. A missing or
// wrong-typed node yields the zero value rather than panicking, so a chain like
// spec.child("target").child("template").mapKeys("data") collapses to nil the
// instant a level is absent.
type specView struct{ m map[string]any }

func (s specView) child(key string) specView {
	m, _ := s.m[key].(map[string]any)
	return specView{m}
}

func (s specView) str(key string) string {
	v, _ := s.m[key].(string)
	return v
}

func (s specView) mapKeys(key string) []string {
	m, _ := s.m[key].(map[string]any)
	if len(m) == 0 {
		return nil
	}
	return slices.Sorted(maps.Keys(m))
}

func (s specView) entryKeys(listKey, field string) []string {
	entries, _ := s.m[listKey].([]any)
	var keys []string
	for _, e := range entries {
		m, _ := e.(map[string]any)
		if k, _ := m[field].(string); k != "" {
			keys = append(keys, k)
		}
	}
	if len(keys) == 0 {
		return nil
	}
	slices.Sort(keys)
	return slices.Compact(keys)
}

func (*Kustomization) producerTargets([]NamedResource) []ProducerTarget            { return nil }
func (*HelmRelease) producerTargets([]NamedResource) []ProducerTarget              { return nil }
func (*HelmRepository) producerTargets([]NamedResource) []ProducerTarget           { return nil }
func (*HelmChartSource) producerTargets([]NamedResource) []ProducerTarget          { return nil }
func (*GitRepository) producerTargets([]NamedResource) []ProducerTarget            { return nil }
func (*OCIRepository) producerTargets([]NamedResource) []ProducerTarget            { return nil }
func (*ExternalArtifact) producerTargets([]NamedResource) []ProducerTarget         { return nil }
func (*Bucket) producerTargets([]NamedResource) []ProducerTarget                   { return nil }
func (*ResourceSet) producerTargets([]NamedResource) []ProducerTarget              { return nil }
func (*ResourceSetInputProvider) producerTargets([]NamedResource) []ProducerTarget { return nil }
func (*ConfigMap) producerTargets([]NamedResource) []ProducerTarget                { return nil }
func (*Secret) producerTargets([]NamedResource) []ProducerTarget                   { return nil }
