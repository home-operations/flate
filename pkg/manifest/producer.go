package manifest

import (
	"cmp"
	"maps"
	"slices"
	"strings"
)

// KeySet retains known names without proving absence until coverage is complete.
type KeySet struct {
	Names    []string `json:"names,omitempty"`
	Complete bool     `json:"complete,omitempty"`
}

// ProducerTarget identifies a declared object and its known data keys.
type ProducerTarget struct {
	NamedResource

	Keys KeySet
}

// ProducerTargets returns fixed outputs and matching concrete candidates declared by obj.
// Predictions establish identity only; they provide no generated content.
// ArtifactGenerator contributes fixed outputs and compatible concrete candidates.
//
// SealedSecret produces template.metadata.name, defaulting to its own name, with
// encryptedData keys. ObjectBucketClaim produces a same-named Secret and ConfigMap
// for credentials and connection settings, without declaring their keys.
//
// Targets use the supplied producer's namespace and spec target names.
// Declaration identity projection belongs to the caller; explicit spec names
// require their own nameReference transformation.
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
	view := r.artifactGeneratorView()
	for _, entry := range view.entries("artifacts") {
		if strings.ContainsAny(view.child(entry).str(nameKey), "{}") {
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
		name := cmp.Or(spec.child("target").str(nameKey), r.Name)
		template := spec.child("target").child("template")
		keys := template.mapKeys(dataKey)
		includeInputs := len(keys) == 0 || template.str("mergePolicy") == "Merge"
		if includeInputs {
			keys = append(keys, spec.entryKeys(dataKey, "secretKey")...)
			slices.Sort(keys)
			keys = slices.Compact(keys)
		}
		dataFrom, _ := r.Spec["dataFrom"].([]any)
		templateFrom, _ := template.m["templateFrom"].([]any)
		complete := len(keys) != 0 && len(templateFrom) == 0 && (!includeInputs || len(dataFrom) == 0)
		return []ProducerTarget{r.secret(name, KeySet{Names: keys, Complete: complete})}
	case r.Kind == "SealedSecret" && apiGroup(r.APIVersion) == "bitnami.com":
		name := cmp.Or(spec.child("template").child(metadataKey).str(nameKey), r.Name)
		return []ProducerTarget{r.secret(name, KeySet{Names: spec.mapKeys("encryptedData"), Complete: true})}
	case r.Kind == "ObjectBucketClaim" && apiGroup(r.APIVersion) == "objectbucket.io":
		return []ProducerTarget{r.secret(r.Name, KeySet{}), r.target(KindConfigMap, r.Name, KeySet{})}
	default:
		return nil
	}
}

func (r *RawObject) secret(name string, keys KeySet) ProducerTarget {
	return r.target(KindSecret, name, keys)
}

func (r *RawObject) target(kind, name string, keys KeySet) ProducerTarget {
	return ProducerTarget{
		Kind: kind, Namespace: r.Namespace, Name: name,
		Keys: keys,
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

func (*Kustomization) producerTargets([]NamedResource) []ProducerTarget    { return nil }
func (*HelmRelease) producerTargets([]NamedResource) []ProducerTarget      { return nil }
func (*HelmRepository) producerTargets([]NamedResource) []ProducerTarget   { return nil }
func (*HelmChartSource) producerTargets([]NamedResource) []ProducerTarget  { return nil }
func (*GitRepository) producerTargets([]NamedResource) []ProducerTarget    { return nil }
func (*OCIRepository) producerTargets([]NamedResource) []ProducerTarget    { return nil }
func (*ExternalArtifact) producerTargets([]NamedResource) []ProducerTarget { return nil }
func (*Bucket) producerTargets([]NamedResource) []ProducerTarget           { return nil }
func (*ResourceSet) producerTargets([]NamedResource) []ProducerTarget      { return nil }

func (*ResourceSetInputProvider) producerTargets([]NamedResource) []ProducerTarget { return nil }
func (*ConfigMap) producerTargets([]NamedResource) []ProducerTarget                { return nil }
func (*Secret) producerTargets([]NamedResource) []ProducerTarget                   { return nil }

// Equal MUST include key completeness in publication identity.
//
//nolint:gocritic // The SDK preserves immutable value snapshots and the value method set.
func (t ProducerTarget) Equal(other ProducerTarget) bool {
	return t.NamedResource == other.NamedResource && t.Keys.Complete == other.Keys.Complete &&
		slices.Equal(t.Keys.Names, other.Keys.Names)
}
