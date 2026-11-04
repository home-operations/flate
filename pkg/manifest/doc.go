// Package manifest defines the data model for Flux GitOps resources as
// observed locally in a Git repository.
//
// The types here mirror the Flux CRDs (GitRepository, OCIRepository,
// HelmRepository, HelmRelease, Kustomization, ...) together with the
// supporting Kubernetes core kinds (ConfigMap, Secret). ParseDoc decodes
// raw YAML documents into these types for canonical YAML export.
//
// Data flate cannot read is absent, warned once, and supplied by the user.
// Admission removes SOPS-encrypted Secret and ConfigMap entries and SOPS metadata.
// Readable data remains authoritative; supplied dummy values fill absent reference keys.
// Re-encryption alone cannot be compared without decryption.
//
// # Shadow fields
//
// HelmRelease and Kustomization embed the upstream Flux Spec struct.
// Values and Chart project fields whose decoded shapes differ.
// Dependency and source identities come from References over the authored spec.
// Embedded specs MUST remain immutable after parse so projections cannot diverge.
//
// References expose borrowed source files and authored chart paths. Discovery
// expands these reads into exact DiscoveryRecord.Inputs without render ownership;
// DiscoveryIndex.Readers contains only Kustomize graph ownership evidence.
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
