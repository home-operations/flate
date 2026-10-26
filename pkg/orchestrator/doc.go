// Package orchestrator plans repository entries and renders isolated cluster trees.
// RenderTrees is the SDK lifecycle boundary:
//
//	runs, err := orchestrator.RenderTrees(ctx, []orchestrator.Tree{{
//		ID: manifest.TreeID{Cluster: ".", Side: manifest.TreeHead},
//		Path: "/path/to/repository",
//	}}, orchestrator.Config{Concurrency: 4})
//
// Results are sorted by cluster and side. Each completed tree has an independent
// Store, controller state and result; all trees share the invocation's task budget,
// cache, network and Helm client. Cancellation joins every launched tree before
// releasing owned resources. Failed trees retain diagnostics and successful trees
// retain readable stores and materialized output.
//
// Caller-supplied Cache, Network and HelmClient remain caller-owned. Keep the
// supplied cache alive through any later lazy source reads, and join those reads
// before closing it. Internally created resources close before RenderTrees returns.
package orchestrator
