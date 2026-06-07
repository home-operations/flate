// Package helm wraps helm.sh/helm/v4 to render HelmReleases without
// shelling out to the `helm` binary.
//
// The exported surface:
//
//   - Client.Template / Client.TemplateDocs render a HelmRelease to
//     YAML documents (the equivalent of
//     `helm template --dry-run --client-only`).
//   - Client.SetSourceResolver wires the canonical source-CR lookup
//     surface — production callers pass NewStoreSourceResolver(store)
//     so HelmRepository / OCIRepository / GitRepository / Bucket /
//     ExternalArtifact lookups read straight from the canonical
//     object store. Embedders rendering a single HR without an
//     orchestrator can implement SourceResolver directly.
//   - Prepare(hr, lookup, provider, cache) performs the pre-render dance
//     (Clone → ResolveChartRef → ExpandValueReferences) — call this
//     before TemplateDocs when rendering a HelmRelease in isolation.
//   - Options exposes the helm CLI flags flate understands
//     (--kube-version, --api-versions, --no-hooks, etc.).
//
// The client is safe for concurrent use; chart downloads are cached
// on disk keyed by chart name + version, and parallel first-loads
// of the same chart coalesce through a per-path keylock.
package helm
