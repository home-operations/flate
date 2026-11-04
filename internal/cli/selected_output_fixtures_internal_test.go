package cli

import (
	"testing"

	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/orchestrator"
)

func outputByParent(batches []manifest.DocumentBatch) map[manifest.NamedResource][]map[string]any {
	out := make(map[manifest.NamedResource][]map[string]any, len(batches))
	for _, batch := range batches {
		if batch.Parent != nil {
			out[batch.Parent.Named()] = batch.Documents
		}
	}
	return out
}

func completedOrchestrator(tb testing.TB) (*orchestrator.Orchestrator, error) {
	tb.Helper()
	runs, err := orchestrator.RenderTrees(
		tb.Context(),
		[]orchestrator.Tree{{ID: manifest.TreeID{Cluster: ".", Side: manifest.TreeHead}, Path: tb.TempDir()}},
		orchestrator.Config{Concurrency: 2},
	)
	if err != nil {
		return nil, err
	}
	if len(runs) != 1 || runs[0].Orchestrator == nil {
		tb.Fatal("missing completed store")
	}
	return runs[0].Orchestrator, nil
}
