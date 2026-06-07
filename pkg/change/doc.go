// Package change computes file-level differences between two
// filesystem trees and maps them onto the Flux resources they affect.
//
// It is the core of flate's "changed-only" mode (--path-orig on any
// command): walk both trees in parallel, SHA-256 every regular file,
// emit the relative paths that differ. Callers then ask Filter whether
// a given resource — identified by the file it was loaded from — has
// changed.
//
// The filter cascades through references: when a HelmRelease's file
// changed, its chart source (OCIRepository / HelmRepository /
// GitRepository) is also marked needed so the actual chart still gets
// downloaded. Likewise, a Kustomization's sourceRef is kept ready so
// downstream waits succeed. dependsOn is intentionally excluded: it's a
// reconcile-ordering signal, not a content dependency.
//
// The cascade also runs in reverse: when the changed file IS a source
// resource, every HelmRelease/Kustomization that references it is kept
// so its render re-runs against the new source spec. This catches the
// centralized-source layout where an OCIRepository lives in its own
// Kustomization tree, separate from the HelmReleases that chartRef it —
// bumping the OCIRepository's tag must still re-render those consumers.
package change
