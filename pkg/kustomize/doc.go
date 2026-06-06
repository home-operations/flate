// Package kustomize wraps sigs.k8s.io/kustomize/api so the rest of
// flate never invokes the `kustomize` CLI. It provides:
//
//   - Build / RenderFlux: render a kustomization directory to YAML
//     documents. Build is the plain krusty surface; RenderFlux adds
//     the Flux generator that handles spec.components and embedded
//     inline Contents.
//   - Prepare: the standard pre-render dance (Clone + expand
//     postBuild.substituteFrom) for embedders rendering a single
//     Kustomization. Symmetric to helm.Prepare for HelmReleases, minus
//     a values cache — the substituteFrom path has no YAML parse to
//     memoize; see Prepare's docstring.
//   - Substitute: envsubst-style "${VAR}" / "${VAR:=default}" used
//     for Flux post-build substitutions.
//
// Concurrency. Two locks guard krusty's non-thread-safety:
//
//   - A per-path lock (stageLocks) serializes builds against the SAME
//     staged directory, which krusty mutates in place.
//   - The process-wide BuildMutex (flux.go) serializes EVERY krusty
//     invocation flate runs — including the helm post-renderer's
//     krusty.Run in pkg/helm — because kustomize's package-global state
//     (the openapi schema registry, builtin plugin/transformer
//     factories) is not goroutine-safe. fluxcd/pkg/kustomize guards its
//     own SecureBuild for the same reason; every flate-owned krusty
//     entrypoint MUST hold BuildMutex.
//
// Caching boundary. kustomize output caching is deferred to the
// controller layer (the Kustomization controller's spec+source
// fingerprint dedup), NOT memoized inside this package the way
// helm.Client caches template output. RenderFlux mutates the staged
// workspace and krusty plugins/generators are not guaranteed
// deterministic, so the stable point to memoize is the coarse
// spec+source hash the controller already computes — not the render
// engine's fluid internals.
package kustomize
