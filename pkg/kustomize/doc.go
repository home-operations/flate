// Package kustomize wraps sigs.k8s.io/kustomize/api so the rest of
// flate never invokes the `kustomize` CLI. It provides:
//
//   - Build: renders a kustomization directory to YAML documents.
//   - Filtering helpers (FilterKinds) that mirror flux-local's
//     chainable Kustomize wrapper.
//   - Variable substitution (envsubst-style "${VAR}" and "${VAR:=default}")
//     for Flux post-build substitutions.
//
// Concurrent builds against the same path are serialized via an
// internal per-path lock — krusty mutates the workspace and concurrent
// invocations against the same directory race.
package kustomize
