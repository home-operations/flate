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
// content hash for the same reason. [DefaultStripAttrs] and
// [DefaultStripFields] are the lists `flate diff` uses out of the box.
//
// # SDK usage
//
// [RenderDocs] returns formatted bytes. A consumer that needs the diff as
// *data* — to build its own API payload, web UI, or image report —
// instead calls [Changes], which returns the same paired, normalized,
// noise-filtered set as a [][Change] (added / changed / removed, with the
// per-side manifests and the captured helm.sh/chart label). Render two
// cluster directories with [orchestrator.RenderTrees] (it owns the
// two-orchestrator, shared-cache, changed-only dance), then feed the
// Results to [Changes]:
//
//	base, head, _ := orchestrator.RenderTrees(ctx, baseDir, headDir, cfg)
//	changes := diff.Changes(
//		diff.DocsFromManifests(base.Result.Manifests, nil),
//		diff.DocsFromManifests(head.Result.Manifests, nil),
//		diff.Options{
//			StripAttrs:  diff.DefaultStripAttrs,
//			StripFields: diff.DefaultStripFields,
//			Normalize:   redact, // optional extra per-manifest scrub
//		},
//	)
//
// [DocsFromManifests] is the store-free adapter from a render Result's
// per-parent manifests to the flat []Doc both [Changes] and [RenderDocs]
// take. [Options].Normalize is an optional per-manifest hook (applied
// after the built-in strips) for noise the defaults don't cover — e.g.
// redacting Secret values or chart-minted TLS certs. See the package
// Example.
//
// [dyff]: https://github.com/homeport/dyff
// [orchestrator.RenderTrees]: https://pkg.go.dev/github.com/home-operations/flate/pkg/orchestrator#RenderTrees
//
// [orchestrator.Result]: https://pkg.go.dev/github.com/home-operations/flate/pkg/orchestrator#Result
package diff
