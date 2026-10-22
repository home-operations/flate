// Package kustomize wraps sigs.k8s.io/kustomize/api so the rest of
// flate never invokes the `kustomize` CLI. It provides:
//
//   - Render: return unstamped output from disk inputs through Flux's secure FS,
//     or immutable source artifacts through their authoritative fs.FS. Generated
//     files use one private overlay; artifact labels need not exist on disk.
//   - Prepare: the standard pre-render dance (Clone + expand
//     postBuild.substituteFrom + overlay) for embedders rendering a single
//     Kustomization. Symmetric to helm.Prepare for HelmReleases, minus
//     a values cache; the substituteFrom path has no YAML parse to
//     memoize; see Prepare's docstring.
//   - Substitute: envsubst-style "${VAR}" / "${VAR:=default}" used
//     for Flux post-build substitutions.
//
// Concurrency. Each Render call builds against its own private overlay,
// so no two renders share mutable state and there is no per-tree build lock.
// krusty's package-global state (the openapi schema registry, builtin
// plugin/transformer factories) is still not goroutine-safe, so the
// process-wide internal/kustomize.BuildMutex serializes EVERY krusty invocation flate
// runs, including the helm post-renderer's krusty.Run in pkg/helm.
// fluxcd/pkg/kustomize guards its own Build for the same reason; every
// flate-owned krusty entrypoint MUST hold that same mutex.
//
// Render persists unstamped output with its input read-set snapshot and
// validates that snapshot against the source view before reusing a frame.
package kustomize
