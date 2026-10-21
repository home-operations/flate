package cli

import (
	"errors"
	"fmt"
	"strings"
	"testing"

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
	root := tb.TempDir()
	var source strings.Builder
	ids := make([]manifest.NamedResource, count-1)
	for i := range ids {
		ids[i] = testutil.KustomizationID(fmt.Sprintf("ns-%d", i%8), fmt.Sprintf("item-%05d", i))
		source.WriteString("---\n")
		source.WriteString(testutil.KustomizationYAML(ids[i].Name, ids[i].Namespace, "  path: ./objects/"+ids[i].Name+"\n  sourceRef: {kind: GitRepository, name: flux-system, namespace: flux-system}\n"))
	}
	testutil.WriteFile(tb, root, "flux.yaml", source.String())
	o, err := orchestrator.New(tb.Context(), orchestrator.Config{Path: root, RepoRoot: root, CacheDir: tb.TempDir(), Concurrency: 2, ExternalChanges: change.NewSet([]string{"flux.yaml"})})
	if err != nil {
		tb.Fatal(err)
	}
	tb.Cleanup(func() {
		if err := o.Stop(); err != nil {
			tb.Errorf("Stop: %v", err)
		}
	})
	if err := o.Bootstrap(tb.Context()); err != nil {
		tb.Fatal(err)
	}
	res := &orchestrator.Result{Manifests: make(map[manifest.NamedResource][]map[string]any, count), Blocked: make(map[manifest.NamedResource][]manifest.NamedResource)}
	for i, id := range ids {
		if i%64 != 0 {
			_, next := o.Store().GetEmission(ids[i-1])
			next.Children, next.Complete = []store.Child{{Obj: o.Store().GetObject(id)}}, true
			o.Store().RecordEmitted(ids[i-1], &next)
			next.Finish()
		}
		if cycle && (i%64 == 63 || i == len(ids)-1) {
			_, next := o.Store().GetEmission(id)
			next.Children, next.Complete = []store.Child{{Obj: o.Store().GetObject(ids[i-i%64])}}, true
			o.Store().RecordEmitted(id, &next)
			// A closed ownership cycle needs independent file-indexed support.
			o.Store().AddObject(next.Children[0].Obj)
			next.Finish()
		}
		res.Manifests[id] = []map[string]any{{
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
		}}
	}
	for i, id := range ids {
		o.Store().UpdateStatus(id, store.StatusReady, "", "")
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
		documents += len(batch)
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
								spec := docs[0]["spec"].(map[string]any)["template"].(map[string]any)["spec"].(map[string]any)
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
								orig := diff.Snapshot{Objects: selected[0].Objects, Manifests: selected[0].Manifests, Unavailable: selected[0].Unavailable}
								current := diff.Snapshot{Objects: selected[1].Objects, Manifests: selected[1].Manifests, Unavailable: selected[1].Unavailable}
								if command == "documents" {
									_, _ = diff.Compare(orig, current, selectedErr, opts)
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
