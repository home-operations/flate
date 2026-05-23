// Package kustomize wraps sigs.k8s.io/kustomize/api so the rest of
// flate never invokes the `kustomize` CLI. It provides:
//
//   - Build / RenderFlux: render a kustomization directory to YAML
//     documents. Build is the plain krusty surface; RenderFlux adds
//     the Flux generator that handles spec.components and embedded
//     inline Contents.
//   - Prepare: the standard pre-render dance (Clone + expand
//     postBuild.substituteFrom) for embedders rendering a single
//     Kustomization. Mirrors helm.Prepare for HelmReleases.
//   - FilterKinds: chainable kind filter that mirrors flux-local.
//   - Substitute: envsubst-style "${VAR}" / "${VAR:=default}" used
//     for Flux post-build substitutions.
//
// Concurrent builds against the same path are serialized via an
// internal per-path lock — krusty mutates the workspace and concurrent
// invocations against the same directory race.
package kustomize
