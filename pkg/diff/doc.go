// Package diff compares two sets of rendered Kubernetes manifests and
// reports the resources whose rendered form differs.
//
// [RenderDocs] is the entry point, and it takes one of two paths by
// [Format]. The dyff text styles (human default, github, brief, gitlab,
// gitea) render the whole set through dyff at once: dyff pairs documents
// by their Kubernetes identity and labels each diff natively with
// apiVersion/kind/namespace/name. The plain unified diff takes a
// per-resource path — each resource is paired against its counterpart,
// keyed by parent KS/HR and resource identity (not just name, so a
// Deployment from HelmRelease A never diffs against the same-named
// Deployment from HelmRelease B), and the per-resource bodies are
// concatenated.
//
// Most styles delegate to [dyff], whose K8s-aware comparison pairs named
// list entries (containers, env vars) by their identifier, so reordering
// a list yields an `⇆ order changed` marker instead of a wall of phantom
// value-line churn:
//
//   - human (default) — dyff's colored, human-readable report.
//   - github — dyff path-keyed diff syntax (`@@ <path> @@`, `+`/`-`,
//     `!`); GitHub's diff lexer renders it natively.
//   - brief — dyff's one-line-per-change summary.
//   - gitlab / gitea — dyff diff syntax with forge-specific prefixes.
//   - diff — a plain unified diff (`diff -u`) of each resource's YAML;
//     not K8s-aware, but consumable by any unified-diff tooling.
//
// [Options].StripAttrs is applied to a deep-copied tree before the
// comparison runs — used to drop chart-bump noise (`helm.sh/chart`,
// `checksum/config`, …) that rotates on every Helm upgrade but carries
// no review-relevant signal. ConfigMap binaryData is summarized to a
// content hash for the same reason.
//
// [dyff]: https://github.com/homeport/dyff
package diff
