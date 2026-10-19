// Package manifest defines the data model for Flux GitOps resources as
// observed locally in a Git repository.
//
// The types here mirror the Flux CRDs (GitRepository, OCIRepository,
// HelmRepository, HelmRelease, Kustomization, ...) together with the
// supporting Kubernetes core kinds (ConfigMap, Secret). ParseDoc decodes
// raw YAML documents into these types for canonical YAML export.
//
// flate renders the user's own repo offline, not a live cluster, so Secret
// values pass through verbatim — plaintext, public keys, certs, binary data.
// The one exception (when wipeSecrets is set, the default) is SOPS ciphertext:
// flate can't decrypt it, and a raw "ENC[AES256_GCM,...]" value poisons
// downstream rendering (the ':'/commas break envsubst, Ingress hosts,
// cert-manager dnsNames), so SOPS-encrypted data/stringData scalars are
// rewritten with placeholder tokens of the form "..PLACEHOLDER_<key>..".
//
// # Shadow fields
//
// HelmRelease and Kustomization embed the upstream Flux Spec struct.
// Values and Chart project fields whose decoded shapes differ.
// Dependency and source identities come from References over the authored spec.
// Embedded specs MUST remain immutable after parse so projections cannot diverge.
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
