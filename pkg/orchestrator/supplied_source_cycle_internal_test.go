package orchestrator

import (
	"errors"
	"strings"
	"testing"

	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/loader"
	"github.com/home-operations/flate/pkg/manifest"
)

func testSuppliedSourceCycle(t *testing.T) {
	t.Helper()
	root := t.TempDir()
	testutil.WriteFile(t,
		root,
		"consumer.yaml",
		testutil.KustomizationYAML("consumer",
			"apps",
			"  path: ./app\n  sourceRef: {kind: ExternalArtifact, name: a}\n"))
	var stream strings.Builder
	for _, pair := range [][2]string{{"a", "b"}, {"b", "a"}} {
		stream.WriteString("---\napiVersion: source.toolkit.fluxcd.io/v1\nkind: ExternalArtifact\nmetadata: {name: " +
			pair[0] +
			", namespace: apps}\nspec: {sourceRef: {kind: ExternalArtifact, name: " +
			pair[1] +
			"}}\n")
	}
	inputs, err := loader.LoadDocuments(t.Context(), strings.NewReader(stream.String()), "supply.yaml")
	if err != nil {
		t.Fatal(err)
	}
	o, err := newTestOrchestrator(t, Tree{Path: root, RepoRoot: root}, Config{Concurrency: 2, Supply: inputs})
	if err != nil {
		t.Fatal(err)
	}
	defer o.stop()
	if bootstrapErr := o.bootstrap(t.Context()); bootstrapErr != nil {
		t.Fatal(bootstrapErr)
	}
	seen := map[manifest.NamedResource]bool{}
	err = o.visitNodes(inputs[0].Obj, true, func(id manifest.NamedResource, _ bool) {
		if seen[id] {
			t.Fatalf("source visited twice: %s", id)
		}
		seen[id] = true
	})
	if !errors.Is(err, manifest.ErrInput) || !errors.Is(err, manifest.ErrFlux) {
		t.Fatalf("source cycle error: %v", err)
	}
	if err := o.run(
		t.Context(),
	); !errors.Is(err, manifest.ErrInput) ||
		!strings.Contains(err.Error(), "source reference cycle") {
		t.Fatalf("run source cycle: %v", err)
	}
}
