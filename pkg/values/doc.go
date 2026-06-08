// Package values implements HelmRelease values resolution and
// Kustomization postBuild substitution. It is the bridge between
// authored manifests and rendered manifests: it consults the central
// Store for ConfigMap/Secret content and merges referenced data into a
// HelmRelease's values map or a Kustomization's postBuild substitution
// table.
//
// Two key behaviors mirror flux-local:
//
//   - Deep merge follows Helm semantics: nested maps are merged, but
//     lists are REPLACED entirely (not concatenated).
//   - When a valuesFrom reference is optional and the target ConfigMap /
//     Secret (or values key) is missing, it is skipped silently so the
//     render proceeds; a wiped Secret value (a placeholder token the
//     manifest parser injects for SOPS-encrypted data) is treated as
//     empty rather than failing the whole HelmRelease.
package values
