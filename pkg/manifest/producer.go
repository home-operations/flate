package manifest

import "sync"

// ProducerTargetSecret returns the Secret that raw — an ExternalSecret
// (external-secrets.io) or SealedSecret (bitnami-labs/sealed-secrets) — will
// generate in-cluster, or (zero, false) when raw is not a recognised producer
// kind. It is the single source of truth for producer classification:
// extending coverage to a new generator kind means adding a case here.
//
// The target name comes from the producer's own declaration — ExternalSecret
// spec.target.name, SealedSecret spec.template.metadata.name — defaulting to
// metadata.name when unset, matching each controller's own defaulting. The
// target Secret lands in the producer's namespace.
//
// Coverage caveat: this reads the producer's RAW declared name. A kustomize
// namePrefix / nameSuffix / replacement that rewrites the generated Secret's
// identity is NOT followed (kustomize does not register a nameReference
// fieldSpec for spec.target.name), so producer-inference misses a transformed
// target and the consumer falls back to fail-loud or --allow-missing-secrets.
// Degraded-but-safe: never a false match.
func ProducerTargetSecret(raw *RawObject) (NamedResource, bool) {
	// Failed map asserts yield nil, and indexing nil yields the zero value, so
	// the nested lookups need no explicit nil checks.
	var name string
	switch raw.Kind {
	case "ExternalSecret":
		target, _ := raw.Spec["target"].(map[string]any)
		name, _ = target["name"].(string)
	case "SealedSecret":
		tmpl, _ := raw.Spec["template"].(map[string]any)
		meta, _ := tmpl["metadata"].(map[string]any)
		name, _ = meta["name"].(string)
	default:
		return NamedResource{}, false
	}
	if name == "" {
		name = raw.Name // both controllers default the target to the CR's own name
	}
	return NamedResource{Kind: KindSecret, Namespace: raw.Namespace, Name: name}, true
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
