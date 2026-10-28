package cli

import (
	"errors"
	"fmt"
	"slices"
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testrunner"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/change"
	"github.com/home-operations/flate/pkg/diff"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/orchestrator"
	"github.com/home-operations/flate/pkg/store"
)

func completedScopeFixture(tb testing.TB, count int, failures, cycle bool) orchestrator.Rendered {
	tb.Helper()
	o, err := completedOrchestrator(tb)
	if err != nil {
		tb.Fatal(err)
	}
	ids := make([]manifest.NamedResource, count-1)
	for i := range ids {
		ids[i] = testutil.KustomizationID(fmt.Sprintf("ns-%d", i%8), fmt.Sprintf("item-%05d", i))
		o.Store().AddObject(&manifest.Kustomization{Name: ids[i].Name, Namespace: ids[i].Namespace})
	}
	res := &orchestrator.Result{Manifests: make([]manifest.DocumentBatch, 0, count), Blocked: make(map[manifest.NamedResource][]manifest.NamedResource)}
	for i, id := range ids {
		if i%64 != 0 {
			_, _, next := o.Store().GetEmission(ids[i-1])
			next.Children, next.Complete = []store.Child{{Obj: o.Store().GetObject(id)}}, true
			o.Store().RecordEmitted(ids[i-1], &next, nil)
			next.Finish()
		}
		if cycle && (i%64 == 63 || i == len(ids)-1) {
			_, _, next := o.Store().GetEmission(id)
			next.Children, next.Complete = []store.Child{{Obj: o.Store().GetObject(ids[i-i%64])}}, true
			o.Store().RecordEmitted(id, &next, nil)
			// A closed ownership cycle needs independent file-indexed support.
			o.Store().AddObject(next.Children[0].Obj)
			next.Finish()
		}
		res.Manifests = append(res.Manifests, manifest.DocumentBatch{Parent: o.Store().GetObject(id), Documents: []map[string]any{{
			"apiVersion": "apps/v1", "kind": "Deployment",
			"metadata": map[string]any{"name": id.Name, "namespace": id.Namespace},
			"spec":     map[string]any{"template": map[string]any{"spec": map[string]any{"containers": []any{map[string]any{"name": "app", "image": "example.test/app:v1"}}}}},
		}, {
			"apiVersion": "v1", "kind": "Service",
			"metadata": map[string]any{"name": id.Name, "namespace": id.Namespace},
			"spec":     map[string]any{"ports": []any{map[string]any{"port": 80}}},
		}, {
			"apiVersion": "v1", "kind": "ConfigMap",
			"metadata": map[string]any{"name": id.Name, "namespace": id.Namespace},
			"data":     map[string]any{"mode": "fixture"},
		}}})
	}
	for i, id := range ids {
		o.Store().UpdateStatus(id, store.StatusInfo{Status: store.StatusReady, Reason: "", Message: ""})
		if failures && i%4 == 0 {
			o.Store().UpdateFailure(id, errors.New("render failed"))
		}
		if failures && i%4 == 1 {
			blockers := []manifest.NamedResource{ids[i-1]}
			o.Store().UpdateFailure(id, &manifest.DependencyFailedError{Parent: id, Failed: blockers})
			res.Blocked[id] = blockers
		}
	}
	res.Failed = o.Store().FailedResources()
	var runErr error
	if len(res.Failed) != 0 {
		runErr = &orchestrator.FailuresError{Message: "raw resource failures"}
	}
	run := orchestrator.Rendered{Orchestrator: o, Result: res, Err: runErr}
	roots := []manifest.NamedResource{{Group: "kustomize.toolkit.fluxcd.io", Kind: manifest.KindKustomization}}
	selected, _, _ := orchestrator.Select([]orchestrator.Rendered{run}, runErr, roots, nil)
	documents := 0
	for _, batch := range selected[0].Manifests {
		documents += len(batch.Documents)
	}
	wantFailures := 0
	if failures {
		wantFailures = (count+2)/4 + (count+1)/4
	}
	if len(selected[0].Objects) != count-1 || documents != 3*(count-1) || len(selected[0].Failed) != wantFailures {
		tb.Fatalf("selection workload: objects=%d documents=%d failures=%d; want %d/%d/%d", len(selected[0].Objects), documents, len(selected[0].Failed), count-1, 3*(count-1), wantFailures)
	}
	return run
}

func TestCompletedScopeFixture_SelectionCounts(t *testing.T) {
	for _, count := range []int{100, 5000} {
		for _, failures := range []bool{false, true} {
			for _, cycle := range []bool{false, true} {
				t.Run(fmt.Sprintf("keep_%d/failures_%t/cycle_%t", count, failures, cycle), func(t *testing.T) {
					completedScopeFixture(t, count, failures, cycle)
				})
			}
		}
	}
}

func keepSetFixture(tb testing.TB, count, sides int) []orchestrator.Rendered {
	tb.Helper()
	runs := make([]orchestrator.Rendered, sides)
	want := make([][]manifest.NamedResource, sides)
	for i := range runs {
		root := tb.TempDir()
		paths := make([]string, count)
		want[i] = make([]manifest.NamedResource, count)
		for j := range paths {
			id := testutil.KustomizationID(fmt.Sprintf("side-%d-ns-%d", i, j%8), fmt.Sprintf("item-%05d", j))
			want[i][j] = id
			paths[j] = id.Name + ".yaml"
			testutil.WriteFile(tb, root, paths[j], testutil.KustomizationYAML(id.Name, id.Namespace, "  suspend: true\n"))
		}
		side := manifest.TreeHead
		if sides == 2 && i == 0 {
			side = manifest.TreeBase
		}
		id := manifest.TreeID{Cluster: ".", Side: side}
		rendered, err := orchestrator.RenderTrees(tb.Context(), []orchestrator.Tree{{
			ID: id, RepoRoot: root, Path: root, ExternalChanges: change.NewSet(paths),
		}}, orchestrator.Config{Concurrency: 2})
		assert.NoError(tb, err)
		if len(rendered) != 1 || rendered[0].Absent || rendered[0].Orchestrator == nil || rendered[0].Result == nil {
			tb.Fatal("missing completed keep-set run")
		}
		runs[i] = rendered[0]
		assert.Equal(tb, runs[i].ID, id)
		assert.NoError(tb, runs[i].Err)
		sentinel := testutil.KustomizationID(fmt.Sprintf("side-%d-ns-0", 1-i), "off-scope")
		runs[i].Store().AddObject(&manifest.Kustomization{Name: sentinel.Name, Namespace: sentinel.Namespace})
		if runs[i].Store().GetObject(sentinel) == nil {
			tb.Fatal("missing local off-scope sentinel")
		}
		explicit, err, remainder := orchestrator.Select([]orchestrator.Rendered{runs[i]}, nil,
			[]manifest.NamedResource{testutil.KustomizationID(sentinel.Namespace, "")}, nil)
		assert.NoError(tb, err)
		assert.NoError(tb, remainder)
		if len(explicit[0].Objects) != 1 {
			tb.Fatalf("explicit namespace selected %d objects; want sentinel only", len(explicit[0].Objects))
		}
		assert.Equal(tb, explicit[0].Objects[0].Named(), sentinel)
		assert.Equal(tb, explicit[0].Counts[manifest.KindKustomization], 1)
	}
	if sides == 2 && runs[0].Store() == runs[1].Store() {
		tb.Fatal("comparison sides share a store")
	}
	selected, err, remainder := orchestrator.Select(runs, nil, []manifest.NamedResource{testutil.KustomizationID("", "")}, nil)
	assert.NoError(tb, err)
	assert.NoError(tb, remainder)
	assert.Equal(tb, len(selected), sides)
	for i, side := range selected {
		got := make([]manifest.NamedResource, len(side.Objects))
		for j, obj := range side.Objects {
			got[j] = obj.Named()
		}
		slices.SortFunc(got, manifest.NamedResource.Compare)
		slices.SortFunc(want[i], manifest.NamedResource.Compare)
		assert.Diff(tb, got, want[i])
		assert.Equal(tb, side.Counts[manifest.KindKustomization], count)
	}
	return runs
}

func TestKeepSetFixture_InferredScope(t *testing.T) {
	for _, sides := range []int{1, 2} {
		t.Run(fmt.Sprintf("sides_%d", sides), func(t *testing.T) {
			keepSetFixture(t, 16, sides)
		})
	}
}

func BenchmarkIncludeNamespace_KeepSet(b *testing.B) {
	for _, count := range []int{100, 5000} {
		for _, sides := range []int{1, 2} {
			b.Run(fmt.Sprintf("keep_%d/sides_%d", count, sides), func(b *testing.B) {
				runs := keepSetFixture(b, count, sides)
				roots := []manifest.NamedResource{testutil.KustomizationID("", "")}
				b.ReportAllocs()
				for b.Loop() {
					_, _, _ = orchestrator.Select(runs, nil, roots, nil)
				}
			})
		}
	}
}

func BenchmarkSelect_CompletedScope(b *testing.B) {
	for _, count := range []int{100, 5000} {
		for _, failures := range []bool{false, true} {
			for _, cycle := range []bool{false, true} {
				for _, command := range []string{"test", "documents", "images"} {
					b.Run(fmt.Sprintf("keep_%d/failures_%t/cycle_%t/%s", count, failures, cycle, command), func(b *testing.B) {
						current := completedScopeFixture(b, count, failures, cycle)
						runs := []orchestrator.Rendered{current}
						if command != "test" {
							orig := completedScopeFixture(b, count, false, cycle)
							runs = []orchestrator.Rendered{orig, current}
							for _, docs := range current.Result.Manifests {
								spec := docs.Documents[0]["spec"].(map[string]any)["template"].(map[string]any)["spec"].(map[string]any)
								spec["containers"].([]any)[0].(map[string]any)["image"] = "example.test/app:v2"
							}
						}
						roots := []manifest.NamedResource{testutil.KustomizationID("", "")}
						opts := diff.Options{Format: diff.FormatJSON}
						b.ReportAllocs()
						for b.Loop() {
							selected, selectedErr, _ := orchestrator.Select(runs, current.Err, roots, nil)
							switch command {
							case "test":
								_ = failureModel(selected[0])
								_ = testrunner.Run(selected[0])
							case "documents", "images":
								orig := diff.Snapshot{Manifests: selected[0].Manifests, Unavailable: selected[0].Unavailable, ProducerKeys: selected[0].ProducerKeys}
								current := diff.Snapshot{Manifests: selected[1].Manifests, Unavailable: selected[1].Unavailable, ProducerKeys: selected[1].ProducerKeys}
								if command == "documents" {
									_, _, _, _ = diff.Compare(orig, current, selectedErr, opts)
								} else {
									_, _, _ = diff.CompareImages(orig, current, selectedErr, false)
								}
							}
						}
					})
				}
			}
		}
	}
}
