// Package diff compares two sets of rendered Kubernetes manifests and
// reports the resources whose rendered form differs.
//
// The pipeline is two stages. [Run] pairs each resource against its
// counterpart in the other set — keyed by parent KS/HR and resource
// identity, not just name, so a Deployment from HelmRelease A never
// diffs against the same-named Deployment from HelmRelease B — and
// renders a per-resource body. [Render] then aggregates those bodies
// into the chosen [Format]. Both take the same Format so the body style
// matches the aggregation (see [Options].Format).
//
// Most styles delegate to [dyff], whose K8s-aware comparison pairs
// named list entries (containers, env vars) by their identifier, so
// reordering a list yields an `⇆ order changed` marker instead of a
// wall of phantom value-line churn:
//
//   - github (default) — dyff path-keyed diff syntax (`@@ <path> @@`,
//     `+`/`-`, `!`); GitHub's diff lexer renders it natively. Also the
//     body embedded in the yaml/json/markdown aggregations.
//   - human / brief — dyff's colored report and one-line summary.
//   - gitlab / gitea — dyff diff syntax with forge-specific prefixes.
//   - diff — a plain unified diff (`diff -u`) of each resource's YAML;
//     not K8s-aware, but consumable by any unified-diff tooling.
//   - yaml / json — a structured list of {parent, resource, body}.
//   - markdown — a PR-comment view: a summary table plus one ```diff
//     fence per resource.
//
// [Options].StripAttrs is applied to a deep-copied tree before the
// comparison runs — used to drop chart-bump noise (`helm.sh/chart`,
// `checksum/config`, …) that rotates on every Helm upgrade but carries
// no review-relevant signal. ConfigMap binaryData is summarized to a
// content hash for the same reason.
//
// [dyff]: https://github.com/homeport/dyff
package diff
