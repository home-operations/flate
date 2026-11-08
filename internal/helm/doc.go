// Package helm wraps helm.sh/helm/v4 to render HelmReleases without
// shelling out to the `helm` binary.
//
// The exported surface:
//
//   - Client.Template renders a HelmRelease to
//     a YAML stream (the equivalent of
//     `helm template --dry-run --client-only`).
//   - NewSources binds each operation to its captured source declarations and artifacts.
//   - Client.Template receives those sources through RenderOptions.
package helm
