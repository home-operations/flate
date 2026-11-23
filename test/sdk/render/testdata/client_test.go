package sdk_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/orchestrator"
)

//nolint:paralleltest // RenderTrees sets the process-wide egress policy.
func TestRun_SelectOwnedSides(t *testing.T) {
	before, after := t.TempDir(), t.TempDir()
	writeClusters(t, before, []string{"main", "test"}, false)
	writeClusters(t, after, []string{"main", "test", "utility"}, true)
	trees := []orchestrator.Tree{
		{ID: manifest.TreeID{Cluster: ".", Side: manifest.TreeBase}, RepoRoot: before, Path: before, SelfURLs: []string{"https://fixture.invalid/sdk"}},
		{ID: manifest.TreeID{Cluster: ".", Side: manifest.TreeHead}, RepoRoot: after, Path: after, SelfURLs: []string{"https://fixture.invalid/sdk"}},
	}
	roots := []manifest.NamedResource{{Group: manifest.FluxKustomizeDomain, Kind: manifest.KindKustomization, Namespace: "flux-system", Name: "cluster-apps"}}
	for _, headOnly := range []bool{false, true} {
		//nolint:paralleltest // RenderTrees sets the process-wide egress policy.
		t.Run(fmt.Sprintf("head_only=%t", headOnly), func(t *testing.T) {
			var callbacks atomic.Int32
			cfg := orchestrator.Config{
				RecordWork: true, Selection: orchestrator.SelectionRequest{Roots: roots, Changed: false, Labels: map[string]string{"fixture": "selected"}},
				HelmOptions:         orchestrator.HelmOptions{DisableChartDigestTracking: true, SkipCRDs: false, SkipTests: true, SkipKinds: []string{}, KubeVersion: "1.32.0", APIVersions: "v1", EnableDNS: false},
				AllowMissingKinds:   []string{"CustomResourceDefinition"},
				Supply:              []orchestrator.SuppliedDocument{{Doc: map[string]any{"apiVersion": "v1", "kind": "ConfigMap", "metadata": map[string]any{"name": "unused", "namespace": "apps"}, "data": map[string]any{"key": "value"}}, InputPath: "supplied.yaml", Ordinal: 0}},
				StrictSubstitutions: true, RestrictEgress: false, RegistryConfig: "", CacheDir: t.TempDir(),
				StatusListener: func(_ manifest.TreeID, _ manifest.DiagnosticIdentity, info orchestrator.StatusInfo) {
					if info.Status == orchestrator.StatusReady && info.Outcome == orchestrator.OutcomePassed {
						callbacks.Add(1)
					}
				},
				Concurrency: 2, HeadOnly: headOnly, SourceRetry: orchestrator.RetryConfig{Attempts: 1, MinWait: time.Millisecond, MaxWait: time.Second, Jitter: 0}, GitDepth: 1,
			}
			run, err := orchestrator.RenderTrees(t.Context(), trees, cfg)
			if run == nil || err == nil {
				t.Fatalf("expected partial run: %v", err)
			}
			t.Cleanup(func() {
				if err := run.Close(); err != nil {
					t.Error(err)
				}
			})
			if len(run.Results) != 6 {
				t.Fatalf("results = %d", len(run.Results))
			}
			if callbacks.Load() == 0 {
				t.Fatal("missing status callbacks")
			}
			if !slices.IsSortedFunc(run.Results, func(a, b orchestrator.Rendered) int { return a.ID.Compare(b.ID) }) {
				t.Fatal("unordered results")
			}
			for _, cluster := range []string{"main", "test", "utility"} {
				ids := []manifest.TreeID{{Cluster: manifest.ClusterID("kubernetes/clusters/" + cluster), Side: manifest.TreeBase}, {Cluster: manifest.ClusterID("kubernetes/clusters/" + cluster), Side: manifest.TreeHead}}
				selected, projection := run.Select(ids, roots, map[string]string{"fixture": "selected"})
				if len(selected) != 2 {
					t.Fatalf("%s: %v", cluster, projection)
				}
				if cluster == "test" {
					if projection.Selected == nil {
						t.Fatal("selected neighbor failure lost")
					}
				} else if !headOnly && projection.Selected != nil {
					t.Fatalf("neighbor failure leaked into %s: %v", cluster, projection.Selected)
				}
				for side, id := range ids {
					index := slices.IndexFunc(run.Results, func(r orchestrator.Rendered) bool { return r.ID == id })
					if index < 0 {
						t.Fatalf("missing owned side: %v", id)
					}
					rendered := run.Results[index]
					absent := cluster == "utility" && side == 0
					if rendered.Absent != absent || rendered.Complete == (absent || headOnly && side == 0) {
						t.Fatalf("wrong completeness: %+v", rendered)
					}
					if absent && (len(rendered.Result.Manifests) != 0 || rendered.Err != nil) {
						t.Fatal("absent side has output")
					}
					inspectResult(t, rendered.Result)
					inspectSelection(t, selected[side])
				}
				if !headOnly && cluster != "test" {
					if value(t, selected[1].Manifests) != cluster+"-head" {
						t.Fatal("head output order lost")
					}
					if cluster == "main" && value(t, selected[0].Manifests) != cluster+"-base" {
						t.Fatal("base output order lost")
					}
				}
				head, projection := run.Select(ids[1:], roots, nil)
				if len(head) != 1 {
					t.Fatal("head-only projection lost")
				}
				if cluster != "test" && projection.Selected != nil {
					t.Fatalf("head-only neighbor failure: %v", projection.Selected)
				}
			}
			original := run.Results[1].ID
			run.Results = []orchestrator.Rendered{{ID: manifest.TreeID{Cluster: "fake", Side: manifest.TreeHead}}}
			if _, projection := run.Select([]manifest.TreeID{original}, roots, nil); projection.Selected != nil {
				t.Fatal(projection.Selected)
			}
			run.Results = nil
			if err := run.Close(); err != nil {
				t.Fatal(err)
			}
			if err := run.Close(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

//nolint:paralleltest // RenderTrees sets the process-wide egress policy.
func TestRun_SelectInvalidIdentities(t *testing.T) {
	root := t.TempDir()
	writeClusters(t, root, []string{"main", "test", "utility"}, false)
	run, err := orchestrator.RenderTrees(t.Context(), []orchestrator.Tree{{ID: manifest.TreeID{Cluster: ".", Side: manifest.TreeHead}, RepoRoot: root, SelfURLs: []string{"https://fixture.invalid/sdk"}}}, orchestrator.Config{Concurrency: 2})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := run.Close(); err != nil {
			t.Error(err)
		}
	})
	base, head := run.Results[0].ID, run.Results[0].ID
	base.Side = manifest.TreeBase
	other := run.Results[1].ID
	for _, tc := range []struct {
		name string
		ids  []manifest.TreeID
	}{
		{"empty", nil}, {"duplicate", []manifest.TreeID{head, head}}, {"unknown", []manifest.TreeID{{Cluster: "missing", Side: manifest.TreeHead}}},
		{"cross_cluster", []manifest.TreeID{base, other}}, {"reversed", []manifest.TreeID{head, base}},
		{"base_only", []manifest.TreeID{base}}, {"oversized", []manifest.TreeID{base, head, other}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			selected, projection := run.Select(tc.ids, []manifest.NamedResource{{Kind: manifest.KindKustomization}}, nil)
			if selected != nil || !errors.Is(projection.Selected, manifest.ErrInput) {
				t.Fatalf("identity validation: %v", projection)
			}
		})
	}
}

//nolint:paralleltest // RenderTrees sets the process-wide egress policy.
func TestRun_ValidationAndSharedError(t *testing.T) {
	bad := orchestrator.Tree{ID: manifest.TreeID{Cluster: "a", Side: manifest.TreeHead}}
	if run, err := orchestrator.RenderTrees(t.Context(), []orchestrator.Tree{bad, bad}, orchestrator.Config{Concurrency: 2}); run != nil || !errors.Is(err, manifest.ErrInput) {
		t.Fatalf("validation returned %v, %v", run, err)
	}
	root := t.TempDir()
	writeClusters(t, root, []string{"main", "test", "utility"}, false)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	run, err := orchestrator.RenderTrees(ctx, []orchestrator.Tree{{ID: manifest.TreeID{Cluster: ".", Side: manifest.TreeHead}, RepoRoot: root, SelfURLs: []string{"https://fixture.invalid/sdk"}}}, orchestrator.Config{Concurrency: 2, StatusListener: func(_ manifest.TreeID, _ manifest.DiagnosticIdentity, _ orchestrator.StatusInfo) { cancel() }})
	if run == nil || !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled partial run: %v", err)
	}
	t.Cleanup(func() {
		if err := run.Close(); err != nil {
			t.Error(err)
		}
	})
	if len(run.Results) != 3 {
		t.Fatalf("cancellation lost owned identities: %v", run.Results)
	}
	for _, result := range run.Results {
		_, projection := run.Select([]manifest.TreeID{result.ID}, []manifest.NamedResource{{Group: manifest.FluxKustomizeDomain, Kind: manifest.KindKustomization}}, nil)
		if !errors.Is(projection.Selected, context.Canceled) || !errors.Is(projection.Remaining, context.Canceled) {
			t.Fatalf("shared cancellation lost: %v", projection)
		}
	}
}

func inspectResult(t *testing.T, r *orchestrator.Result) {
	t.Helper()
	if r == nil {
		t.Fatal("missing result")
	}
	for _, record := range r.Work {
		_ = record.Key.Tree
	}
	for _, info := range r.Failed {
		if info.Status != orchestrator.StatusFailed || info.Outcome != orchestrator.OutcomeFailed && info.Outcome != orchestrator.OutcomeBlocked || info.Message == "" {
			t.Fatalf("failure conversion: %+v", info)
		}
	}
	_ = len(r.Orphans) + len(r.Blocked) + len(r.Warnings) + len(r.ClusterWarnings) + len(r.Manifests)
}

func inspectSelection(t *testing.T, s *orchestrator.Selection) {
	t.Helper()
	if s == nil {
		t.Fatal("missing selection")
	}
	for _, info := range s.Statuses {
		if info.Status == "" || info.Outcome == "" {
			t.Fatalf("missing status: %+v", info)
		}
	}
	if s.BlockedCount > len(s.Blocked) {
		t.Fatal("invalid blocked count")
	}
	_ = len(s.Subjects) + len(s.Manifests) + len(s.Failed) + len(s.Warnings) + len(s.ClusterWarnings) + len(s.Objects) + len(s.Counts) + len(s.ChartVersions) + len(s.Causes) + len(s.Unavailable) + len(s.Unrelated) + len(s.External)
}

func writeClusters(t *testing.T, root string, clusters []string, head bool) {
	t.Helper()
	write(t, root, ".taskfiles/bootstrap/Taskfile.yaml", "version: '3'\ntasks: {bootstrap: {cmds: [true]}}\n")
	side := "base"
	if head {
		side = "head"
	}
	for _, cluster := range clusters {
		entry := "kubernetes/clusters/" + cluster
		write(t, root, entry+"/source.yaml", "apiVersion: source.toolkit.fluxcd.io/v1\nkind: GitRepository\nmetadata: {name: cluster, namespace: flux-system}\nspec: {url: https://fixture.invalid/sdk}\n")
		write(t, root, entry+"/instance.yaml", "apiVersion: fluxcd.controlplane.io/v1\nkind: FluxInstance\nmetadata: {name: flux, namespace: flux-system}\nspec:\n  sync: {kind: GitRepository, name: cluster, url: https://fixture.invalid/sdk, ref: refs/heads/main, path: ./"+entry+"}\n")
		write(t, root, entry+"/apps.yaml", "apiVersion: kustomize.toolkit.fluxcd.io/v1\nkind: Kustomization\nmetadata: {name: cluster-apps, namespace: flux-system, labels: {fixture: selected}}\nspec:\n  interval: 1m\n  path: ./kubernetes/apps/"+cluster+"\n  sourceRef: {kind: GitRepository, name: cluster}\n")
		resource := "cm.yaml"
		if head && cluster == "test" {
			resource = "missing.yaml"
		}
		write(t, root, "kubernetes/apps/"+cluster+"/kustomization.yaml", "resources: ["+resource+"]\n")
		write(t, root, "kubernetes/apps/"+cluster+"/cm.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: workload}\ndata: {value: '"+cluster+"-"+side+"'}\n")
	}
}

func write(t *testing.T, root, path, body string) {
	t.Helper()
	target := filepath.Join(root, path)
	if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(target, []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
}

func value(t *testing.T, batches []manifest.DocumentBatch) string {
	t.Helper()
	for _, batch := range batches {
		for _, doc := range batch.Documents {
			if data, ok := doc["data"].(map[string]any); ok {
				if value, ok := data["value"].(string); ok {
					return value
				}
			}
		}
	}
	t.Fatal("missing output value")
	return ""
}
