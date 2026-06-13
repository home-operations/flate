package manifest

import (
	"cmp"
	"sync"
)

// ProducerTargets returns the in-cluster objects raw will generate live, or nil
// when raw is not a recognised producer kind. It is the single source of truth
// for producer classification: extending coverage to a new generator kind means
// adding a case here.
//
//   - ExternalSecret (external-secrets.io) / SealedSecret
//     (bitnami-labs/sealed-secrets): the one Secret each materializes —
//     spec.target.name / spec.template.metadata.name, defaulting to
//     metadata.name (matching each controller's own defaulting).
//   - ObjectBucketClaim (objectbucket.io — Rook/Ceph's lib-bucket-provisioner):
//     a Secret AND a ConfigMap, both named after the OBC. The Secret holds the
//     S3 credentials (AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY), the ConfigMap
//     the bucket connection info (BUCKET_HOST / BUCKET_PORT / BUCKET_NAME); a
//     consuming HelmRelease valuesFrom (or substituteFrom) references them by
//     the claim's name and neither exists in the offline tree.
//
// Every target lands in the producer's namespace.
//
// Coverage caveat: this reads the producer's RAW declared name. A kustomize
// namePrefix / nameSuffix / replacement that rewrites the generated object's
// identity is NOT followed (kustomize registers no nameReference fieldSpec for
// these), so producer-inference misses a transformed target and the consumer
// falls back to fail-loud or --allow-missing-secrets. Degraded-but-safe: never
// a false match.
func ProducerTargets(raw *RawObject) []NamedResource {
	// Failed map asserts yield nil, and indexing nil yields the zero value, so
	// the nested lookups need no explicit nil checks.
	switch raw.Kind {
	case "ExternalSecret":
		target, _ := raw.Spec["target"].(map[string]any)
		name, _ := target["name"].(string)
		return []NamedResource{{Kind: KindSecret, Namespace: raw.Namespace, Name: cmp.Or(name, raw.Name)}}
	case "SealedSecret":
		tmpl, _ := raw.Spec["template"].(map[string]any)
		meta, _ := tmpl["metadata"].(map[string]any)
		name, _ := meta["name"].(string)
		return []NamedResource{{Kind: KindSecret, Namespace: raw.Namespace, Name: cmp.Or(name, raw.Name)}}
	case "ObjectBucketClaim":
		return []NamedResource{
			{Kind: KindSecret, Namespace: raw.Namespace, Name: raw.Name},
			{Kind: KindConfigMap, Namespace: raw.Namespace, Name: raw.Name},
		}
	default:
		return nil
	}
}

// ProducerIndex maps a target resource — the Secret an ExternalSecret /
// SealedSecret declares it will materialize live in-cluster — to the producer
// that declares it. It lets consumers (HelmRelease valuesFrom, source auth)
// distinguish a secret that is *intended* to exist live (skip it, the producer
// is positive in-repo evidence) from one that is simply missing (fail loud).
//
// Two writers populate it: a discovery-time scan of in-repo ES/SS files (seeded
// before any fetch, so source auth — which runs early — can consult it) and the
// HelmRelease controller's render-time EventObjectAdded listener (which sees
// post-kustomize-transform names, the accurate signal for valuesFrom). Both
// write the same target→producer mapping for the same producer, so concurrent
// writes are idempotent — last-write-wins needs no conflict handling.
//
// Nil-safe: a zero/absent index (stripped-down tests, no orchestrator) reports
// no producers, so consumers degrade to their pre-feature behavior.
type ProducerIndex struct {
	m sync.Map // NamedResource -> NamedResource
}

// Record notes that producer generates target. Idempotent; nil-safe.
func (p *ProducerIndex) Record(target, producer NamedResource) {
	if p == nil {
		return
	}
	p.m.Store(target, producer)
}

// Producer returns the producer declaring target, or (zero, false). Nil-safe.
func (p *ProducerIndex) Producer(target NamedResource) (NamedResource, bool) {
	if p == nil {
		return NamedResource{}, false
	}
	v, ok := p.m.Load(target)
	if !ok {
		return NamedResource{}, false
	}
	return v.(NamedResource), true
}
