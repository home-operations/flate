package manifest

import (
	"cmp"
	"maps"
	"slices"
	"sync"
)

// ProducerTarget is one in-cluster object a producer will generate live: its
// identity, plus — when flate can know them offline — the data keys the output
// Secret will hold.
//
// NamedResource is embedded, so a caller that only wants the identity uses the
// target exactly as it would a NamedResource (range the slice, read target.Kind
// / .Namespace, pass target.NamedResource to the index); the DeclaredKeys field
// is the extra signal the placeholder path reads, and identity + keys ride the
// one value so classification needs a single kind-switch.
//
// DeclaredKeys is non-nil only for a Secret whose keys are STATICALLY DECLARED
// in the producer's manifest (an ExternalSecret's target.template.data or
// data[].secretKey, a SealedSecret's encryptedData). It lets a consumer
// (postBuild substituteFrom, HelmRelease valuesFrom) resolve the secret's
// ${VAR}s to ..PLACEHOLDER_<key>.. instead of empty, exactly as flate already
// does for a SOPS-encrypted Secret (known keys, unknowable values). It stays nil
// when the keys live only in the remote store (a dataFrom-only ExternalSecret)
// or are out of the byte-verified scope (an ObjectBucketClaim's fixed keys) —
// those secrets remain genuinely unreadable and the UnresolvedSubstitution
// advisory still fires. Sorted and de-duplicated, for a deterministic synthetic
// Secret.
type ProducerTarget struct {
	NamedResource
	DeclaredKeys []string
}

// ProducerTargets returns the in-cluster objects raw will generate live — each
// carrying the output keys it statically declares — or nil when raw is not a
// recognised producer kind. It is the single source of truth for producer
// classification: extending coverage to a new generator kind means adding one
// case here, which decides the target's identity AND its declared keys together.
// The unavoidable map[string]any walk of the decoded-YAML spec is funnelled
// through the typed, nil-safe specView accessors, so each case reads as plain
// field access rather than a ladder of type-asserts.
//
//   - ExternalSecret (external-secrets.io): the one Secret it materializes —
//     spec.target.name, defaulting to metadata.name (matching the controller's
//     own defaulting). Its declared keys are spec.target.template.data when a
//     template is present — that fully defines the output (its default
//     mergePolicy is Replace; spec.dataFrom only feeds the template's *inputs*) —
//     else each spec.data[].secretKey. spec.dataFrom extract/find is deliberately
//     not enumerated: those keys live in the remote store, unknowable offline, so
//     a dataFrom-only ExternalSecret declares none.
//   - SealedSecret (bitnami-labs/sealed-secrets): the one Secret it unseals —
//     spec.template.metadata.name, defaulting to metadata.name. Its declared keys
//     are spec.encryptedData's (a key→ciphertext map decrypted into same-named
//     output keys).
//   - ObjectBucketClaim (objectbucket.io — Rook/Ceph's lib-bucket-provisioner):
//     a Secret AND a ConfigMap, both named after the OBC, declaring no keys (the
//     provisioner's fixed keys are out of the byte-verified scope). The Secret
//     holds the S3 credentials (AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY), the
//     ConfigMap the bucket connection info (BUCKET_HOST / BUCKET_PORT /
//     BUCKET_NAME); a consuming HelmRelease valuesFrom (or substituteFrom)
//     references them by the claim's name and neither exists in the offline tree.
//
// Every target lands in the producer's namespace.
//
// Coverage caveat: this reads the producer's RAW declared name. A kustomize
// namePrefix / nameSuffix / replacement that rewrites the generated object's
// identity is NOT followed (kustomize registers no nameReference fieldSpec for
// these), so producer-inference misses a transformed target and the consumer
// falls back to fail-loud or --allow-missing-secrets. Degraded-but-safe: never
// a false match.
func ProducerTargets(raw *RawObject) []ProducerTarget {
	spec := specView{raw.Spec}
	switch raw.Kind {
	case "ExternalSecret":
		name := cmp.Or(spec.child("target").str("name"), raw.Name)
		// A target.template fully defines the output keys; without one they are
		// each data[].secretKey. (Slices aren't comparable, so this fallback
		// can't fold into a cmp.Or the way the name above does.)
		keys := spec.child("target").child("template").mapKeys("data")
		if keys == nil {
			keys = spec.entryKeys("data", "secretKey")
		}
		return []ProducerTarget{raw.secret(name, keys)}
	case "SealedSecret":
		name := cmp.Or(spec.child("template").child("metadata").str("name"), raw.Name)
		return []ProducerTarget{raw.secret(name, spec.mapKeys("encryptedData"))}
	case "ObjectBucketClaim":
		return []ProducerTarget{raw.secret(raw.Name, nil), raw.target(KindConfigMap, raw.Name, nil)}
	default:
		return nil
	}
}

// secret is the Secret ProducerTarget every producer case reaches for; target is
// the general form. Both land in raw's namespace.
func (r *RawObject) secret(name string, declaredKeys []string) ProducerTarget {
	return r.target(KindSecret, name, declaredKeys)
}

func (r *RawObject) target(kind, name string, declaredKeys []string) ProducerTarget {
	return ProducerTarget{
		NamedResource: NamedResource{Kind: kind, Namespace: r.Namespace, Name: name},
		DeclaredKeys:  declaredKeys,
	}
}

// specView is a typed, nil-safe read-only window onto a producer's decoded-YAML
// spec (map[string]any). It confines the unavoidable any-grubbing to these few
// accessors so the classifier reads as plain field access. A missing or
// wrong-typed node yields the zero value rather than panicking, so a chain like
// spec.child("target").child("template").mapKeys("data") collapses to nil the
// instant a level is absent.
type specView struct{ m map[string]any }

// child descends into the nested map at key (an empty view if absent/non-map).
func (s specView) child(key string) specView {
	m, _ := s.m[key].(map[string]any)
	return specView{m}
}

// str reads the string at key ("" if absent/non-string).
func (s specView) str(key string) string {
	v, _ := s.m[key].(string)
	return v
}

// mapKeys returns the keys of the map at key, sorted; nil if absent or empty.
// Each key of such a map (target.template.data, encryptedData) is an output
// Secret key.
func (s specView) mapKeys(key string) []string {
	m, _ := s.m[key].(map[string]any)
	if len(m) == 0 {
		return nil
	}
	return slices.Sorted(maps.Keys(m))
}

// entryKeys reads the []any at listKey and returns each element's string field
// (e.g. data[].secretKey), sorted and de-duplicated; nil if none. This is how an
// ExternalSecret names its output keys when it has no template.
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
