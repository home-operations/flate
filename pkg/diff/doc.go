// Package diff compares two sets of rendered Kubernetes manifests and
// reports the resources whose rendered form differs.
//
// [RenderDocs] matches normalized resources by canonical parent and resource
// identity in every [Format]. Parent group, kind, namespace and name distinguish
// producers; [Parent.Path] is display metadata. A resource moved between parents
// is a removal and an addition, even when its contents are identical.
// Native styles retain Kubernetes apiVersion/kind/namespace/name labels and use
// dyff to compare each pair's contents. Unified, HTML and JSON formats present
// the same pairs with their producing parents.
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
// [Compare] consumes two [Snapshot] values from completed runs selected by
// orchestrator.Select, withholds unavailable producer batches, and returns
// formatted bytes, duplicate warnings and structured suppressions without discarding
// selected render errors. The comparison requires cfg.HeadOnly to be false so
// both runs are completed. Render two trees, select the requested producer kinds,
// then compare them:
//
//	runs, runErr := orchestrator.RenderTrees(ctx, []orchestrator.Tree{
//		{ID: manifest.TreeID{Cluster: ".", Side: manifest.TreeBase}, RepoRoot: baseDir},
//		{ID: manifest.TreeID{Cluster: ".", Side: manifest.TreeHead}, RepoRoot: headDir},
//	}, cfg)
//	for start := 0; start < len(runs); {
//		end := start + 1
//		for end < len(runs) && runs[end].ID.Cluster == runs[start].ID.Cluster {
//			end++
//		}
//		pair := runs[start:end]
//		start = end
//		selected, selectedErr, _ := orchestrator.Select(
//			pair, errors.Join(pair[0].Err, pair[1].Err),
//			[]manifest.NamedResource{{Group: manifest.FluxKustomizeDomain, Kind: manifest.KindKustomization}, {Group: manifest.HelmReleaseDomain, Kind: manifest.KindHelmRelease}}, nil)
//		out, warnings, suppressed, err := diff.Compare(
//			diff.Snapshot{Manifests: selected[0].Manifests, Unavailable: selected[0].Unavailable, ProducerKeys: selected[0].ProducerKeys},
//			diff.Snapshot{Manifests: selected[1].Manifests, Unavailable: selected[1].Unavailable, ProducerKeys: selected[1].ProducerKeys},
//			selectedErr,
//			diff.Options{
//				StripAttrs: diff.DefaultStripAttrs, StripFields: diff.DefaultStripFields,
//				Normalize: redact, // optional extra per-manifest scrub
//			},
//		)
//		// Deliver out under pair[0].ID.Cluster; present warnings, suppressed and err
//		// through the diagnostic writer, including when partial output is available.
//	}
//	// Check the invocation error once after every cluster, including shared cleanup failures.
//	return runErr
//
// [CompareImages] uses the same selected producer availability and returns a
// sorted image list plus structured suppressions. [Changes] and [RenderDocs] retain
// their low-level document APIs for callers that already have attributed
// [Doc] values. [Options].Normalize runs after the built-in strips on copied
// manifests. See the package Example for structured changes with chart metadata.
//
// [dyff]: https://github.com/homeport/dyff
//
// [orchestrator.RenderTrees]: https://pkg.go.dev/github.com/home-operations/flate/pkg/orchestrator#RenderTrees
package diff
