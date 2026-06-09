// Package manifest defines the data model for Flux GitOps resources as
// observed locally in a Git repository.
//
// The types here mirror the Flux CRDs (GitRepository, OCIRepository,
// HelmRepository, HelmRelease, Kustomization, ...) together with the
// supporting Kubernetes core kinds (ConfigMap, Secret). Each resource type
// can be decoded from raw YAML (the form Flux stores in a repo) via its
// ParseDoc-equivalent constructor, and re-encoded to a canonical YAML
// representation for export.
//
// Secrets are stripped of their values by default during parsing: the
// data/stringData fields are rewritten with placeholder tokens of the form
// "..PLACEHOLDER_<key>..". This matches flux-local's behavior — flate never
// needs the cleartext values to verify cluster shape.
//
// # Shadow fields
//
// HelmRelease and Kustomization embed the upstream Flux Spec struct and
// also expose projected top-level fields (Values, DependsOn, Chart,
// Path, SourceKind, ...) that mirror commonly-read pieces of that
// nested spec. Reads should use the projected fields; the embedded
// Spec is retained for round-tripping unknown fields back out via
// pkg/manifest's encoders. Writing to the embedded Spec.* after parse
// is a bug — the projections are populated once during ParseDoc and
// later reads will diverge.
//
// # Mutation contract
//
// Every concrete manifest type in this package is treated as
// immutable once stored. Controllers and embedders that need to amend
// a resource must Clone() it, mutate the clone, and AddObject the
// result. Mutating a *HelmRelease / *Kustomization / etc. in place
// after it has been added to the store corrupts the canonical state
// that other concurrent readers depend on.
package manifest
