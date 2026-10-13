package orchestrator_test

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/home-operations/flate/internal/report"
	"github.com/home-operations/flate/internal/testrunner"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/change"
	"github.com/home-operations/flate/pkg/diff"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/orchestrator"
	"github.com/home-operations/flate/pkg/store"
)

func completedScopeFixture(b *testing.B, count int, failures, cycle bool) orchestrator.Rendered {
	b.Helper()
	root := b.TempDir()
	var source strings.Builder
	// The shared bootstrap source contributes one entry to the keep set.
	ids := make([]manifest.NamedResource, count-1)
	for i := range ids {
		ids[i] = manifest.NamedResource{Kind: manifest.KindKustomization, Namespace: fmt.Sprintf("ns-%d", i%8), Name: fmt.Sprintf("item-%05d", i)}
		fmt.Fprintf(&source, "---\napiVersion: kustomize.toolkit.fluxcd.io/v1\nkind: Kustomization\nmetadata: {name: %s, namespace: %s}\nspec:\n  path: ./objects/%s\n  sourceRef: {kind: GitRepository, name: flux-system, namespace: flux-system}\n", ids[i].Name, ids[i].Namespace, ids[i].Name)
	}
	testutil.WriteFile(b, root, "flux.yaml", source.String())
	o, err := orchestrator.New(orchestrator.Config{Path: root, RepoRoot: root, CacheDir: b.TempDir(), Concurrency: 2, ExternalChanges: change.NewSet([]string{"flux.yaml"})})
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(o.Stop)
	if err := o.Bootstrap(b.Context()); err != nil {
		b.Fatal(err)
	}
	res := &orchestrator.Result{Manifests: make(map[manifest.NamedResource][]map[string]any, count), Blocked: make(map[manifest.NamedResource][]manifest.NamedResource)}
	for i, id := range ids {
		o.Store().UpdateStatus(id, store.StatusReady, "")
		if i%64 != 0 {
			o.Store().RecordEmitted(ids[i-1], []manifest.NamedResource{id})
		}
		if cycle && (i%64 == 63 || i == len(ids)-1) {
			o.Store().RecordEmitted(id, []manifest.NamedResource{ids[i-i%64]})
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
	return orchestrator.Rendered{Orchestrator: o, Result: res, Err: runErr}
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
						roots := []manifest.NamedResource{{Kind: manifest.KindKustomization}}
						opts := diff.Options{Format: diff.FormatJSON}
						b.ReportAllocs()
						for b.Loop() {
							selected, selectedErr, _ := orchestrator.Select(runs, current.Err, roots, nil)
							switch command {
							case "test":
								_ = report.Build(selected[0])
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
