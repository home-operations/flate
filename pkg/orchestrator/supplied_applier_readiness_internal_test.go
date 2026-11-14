package orchestrator

import (
	"strings"
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/loader"
	"github.com/home-operations/flate/pkg/manifest"
)

func testSuppliedApplierReadiness(t *testing.T) {
	t.Helper()
	root := t.TempDir()
	testutil.WriteFile(t,
		root,
		"consumer.yaml",
		testutil.KustomizationYAML("consumer",
			"apps",
			"  path: ./app\n  dependsOn: [{name: supplied}]\n"))
	for _, path := range []string{"app", "supplied"} {
		testutil.WriteFile(t, root, path+"/kustomization.yaml", "resources: [cm.yaml]\n")
		testutil.WriteFile(t,
			root,
			path+"/cm.yaml",
			"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: "+path+"-output, namespace: apps}\n")
	}
	inputs, err := loader.LoadDocuments(t.Context(),
		strings.NewReader(testutil.KustomizationYAML("supplied",
			"apps",
			"  path: ./supplied\n  dependsOn: [{name: consumer}]\n")),
		"supply.yaml")
	if err != nil {
		t.Fatal(err)
	}
	o, err := newTestOrchestrator(t, Tree{Path: root, RepoRoot: root}, Config{Concurrency: 2, Supply: inputs})
	if err != nil {
		t.Fatal(err)
	}
	if bootstrapErr := o.bootstrap(t.Context()); bootstrapErr != nil {
		t.Fatal(bootstrapErr)
	}
	supplied := inputs[0].Obj.Named()
	if _, exists := o.depGraph.outEdges[supplied]; exists {
		t.Fatal("supplied applier entered initial producer graph")
	}
	assert.NoError(t, o.updateDependencyGraphFor(t.Context(), supplied))
	if _, exists := o.depGraph.outEdges[supplied]; exists {
		t.Fatal("supplied applier entered event-driven producer graph")
	}
	result, err := o.collectResult(t.Context(), o.run(t.Context()))
	t.Cleanup(result.Close)
	if err == nil {
		t.Fatal("supplied declaration manufactured readiness")
	}
	if _, exists := o.depGraph.outEdges[supplied]; exists {
		t.Fatal("supplied applier acquired dependency graph ownership")
	}
	for i := range result.Warnings {
		if result.Warnings[i].Category == manifest.WarnDependencyCycle {
			t.Fatal("supplied applier created a producer cycle")
		}
	}
	if o.store.HasInventory(supplied) {
		t.Fatal("supplied applier executed to satisfy readiness")
	}
	if _, known := o.store.GetStatus(supplied); known {
		t.Fatal("supplied applier acquired readiness status")
	}
	if len(result.Blocked[testutil.KustomizationID("apps", "consumer")]) == 0 {
		t.Fatal("consumer lost its readiness failure")
	}
}
