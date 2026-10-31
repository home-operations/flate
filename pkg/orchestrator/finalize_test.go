package orchestrator

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"slices"
	"strings"
	"testing"
	"testing/fstest"

	kustomizev1 "github.com/fluxcd/kustomize-controller/api/v1"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/schedule"
	"github.com/home-operations/flate/pkg/store"
	"github.com/home-operations/flate/pkg/task"
)

func TestReportOwnership_LocalPartition(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name    string
		indexed bool
	}{
		{name: "published"},
		{name: "unchanged declaration", indexed: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			st := store.New()
			source := &manifest.GitRepository{Name: "external", Namespace: "apps"}
			external := &manifest.Kustomization{Name: "external", Namespace: "apps", Path: "apps/deeper", SourceRef: kustomizev1.CrossNamespaceSourceReference{Kind: manifest.KindGitRepository, Name: source.Name}}
			local := &manifest.Kustomization{Name: "local", Namespace: "apps", Path: "elsewhere"}
			child := &manifest.HelmRelease{Name: "child", Namespace: "apps"}
			st.AddObjects([]manifest.BaseManifest{source, child})
			index := indexForTest(map[manifest.NamedResource]string{child.Named(): "apps/deeper/child.yaml"})
			entry := testutil.KustomizationID("apps", "entry")
			for _, owner := range []*manifest.Kustomization{external, local} {
				if tc.indexed {
					index.Records[owner.Named()] = manifest.DiscoveryRecord{File: "entry.yaml", Parent: entry, Declaration: owner}
				} else {
					st.AddObject(owner)
					st.UpdateStatus(owner.Named(), store.StatusInfo{Status: store.StatusReady})
				}
			}
			st.UpdateStatus(entry, store.StatusInfo{Status: store.StatusReady, Reason: store.ReasonUnchanged})
			st.UpdateStatus(child.Named(), store.StatusInfo{Status: store.StatusFailed, Message: "unwired"})
			o := &Orchestrator{store: st, index: index}
			all := o.reportingClaims()
			localClaims := o.reportOwnership(all)
			assert.Diff(t, all, []manifest.KSClaim{{ID: external.Named(), Prefix: "apps/deeper/"}, {ID: local.Named(), Prefix: "elsewhere/"}})
			assert.Diff(t, localClaims, []manifest.KSClaim{{ID: local.Named(), Prefix: "elsewhere/"}})
			_, owned := manifest.StrictParent(all, "apps/deeper/invalid.yaml", manifest.NamedResource{}, nil)
			assert.Equal(t, owned, true)
			o.classifyCompleted()
			info, _ := st.GetStatus(child.Named())
			assert.Equal(t, info.Status, store.StatusFailed)
			assert.Equal(t, len(o.orphans), 0)
			if tc.indexed {
				assert.Equal(t, st.GetObject(external.Named()) == nil, true)
				assert.Equal(t, st.GetObject(local.Named()) == nil, true)
				st.UpdateStatus(entry, store.StatusInfo{Status: store.StatusReady})
				all = o.reportingClaims()
				assert.Equal(t, len(all), 0)
				_, owned = manifest.StrictParent(all, "apps/deeper/invalid.yaml", manifest.NamedResource{}, nil)
				assert.Equal(t, owned, false)
			}
		})
	}
}

func TestReportOwnership_FreshPasses(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name            string
		owned, orphaned [2]bool
	}{
		{name: "publication", owned: [2]bool{false, true}, orphaned: [2]bool{false, true}},
		{name: "replacement", owned: [2]bool{false, true}, orphaned: [2]bool{false, true}},
		{name: "withdrawal", owned: [2]bool{true, false}, orphaned: [2]bool{true, false}},
		{name: "source publication", owned: [2]bool{true, true}, orphaned: [2]bool{false, true}},
		{name: "source replacement", owned: [2]bool{true, true}, orphaned: [2]bool{true, false}},
		{name: "source withdrawal", owned: [2]bool{true, true}, orphaned: [2]bool{true, false}},
		{name: "source contents replacement", owned: [2]bool{false, true}, orphaned: [2]bool{false, true}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			st := store.New()
			root := t.TempDir()
			parent := &manifest.Kustomization{Name: "parent", Namespace: "apps", Path: "apps"}
			child := &manifest.HelmRelease{Name: "child", Namespace: "apps"}
			source := &manifest.GitRepository{Name: "source", Namespace: "apps"}
			local := &store.SourceArtifact{LocalPath: root, LocalRoot: root, FS: fstest.MapFS{}}
			base := "apps"
			if tc.name == "source contents replacement" {
				base = "components"
			}
			if tc.name == "replacement" {
				parent.Path = "unrelated"
			}
			if strings.HasPrefix(tc.name, "source ") {
				parent.SourceRef = kustomizev1.CrossNamespaceSourceReference{Kind: manifest.KindGitRepository, Name: source.Name}
				st.AddObject(source)
				if tc.name != "source publication" {
					st.SetArtifact(source.Named(), local)
				}
			}
			if tc.name != "publication" {
				st.AddObject(parent)
				st.UpdateStatus(parent.Named(), store.StatusInfo{Status: store.StatusReady})
			}
			st.AddObject(child)
			index := indexForTest(map[manifest.NamedResource]string{parent.Named(): "parent.yaml", child.Named(): base + "/child.yaml"})
			index.Claims = map[string][]manifest.NamedResource{"startup/": {parent.Named()}}
			index.Readers = map[string][]manifest.FileReader{"unrelated.yaml": {{Reader: child.Named(), Source: manifest.BootstrapSourceID}}}
			records := maps.Clone(index.Records)
			claims := map[string][]manifest.NamedResource{"startup/": slices.Clone(index.Claims["startup/"])}
			readers := map[string][]manifest.FileReader{"unrelated.yaml": slices.Clone(index.Readers["unrelated.yaml"])}
			o := &Orchestrator{store: st, repoRoot: root, index: index, tasks: task.NewBounded(2)}
			var previousAll, previousLocal, savedAll, savedLocal []manifest.KSClaim
			for pass := range 2 {
				if pass != 0 {
					switch tc.name {
					case "publication":
						st.AddObject(parent)
						st.UpdateStatus(parent.Named(), store.StatusInfo{Status: store.StatusReady})
					case "replacement":
						replacement := *parent
						replacement.Path = "apps"
						st.AddObject(&replacement)
					case "withdrawal":
						assert.Equal(t, st.DeleteObject(parent.Named()), true)
					case "source publication":
						st.SetArtifact(source.Named(), local)
					case "source replacement":
						st.SetArtifact(source.Named(), &store.SourceArtifact{LocalPath: t.TempDir(), FS: fstest.MapFS{}})
					case "source withdrawal":
						st.SetArtifact(source.Named(), nil)
					case "source contents replacement":
						st.SetArtifact(source.Named(), &store.SourceArtifact{LocalPath: root, LocalRoot: root, FS: fstest.MapFS{
							"apps/kustomization.yaml": &fstest.MapFile{Data: []byte("components:\n- ../components\n")},
						}})
					}
				}
				st.UpdateStatus(child.Named(), store.StatusInfo{Status: store.StatusFailed, Message: "unwired"})
				all := o.reportingClaims()
				localClaims := o.reportOwnership(all)
				_, owned := manifest.StrictParent(all, base+"/invalid.yaml", manifest.NamedResource{}, nil)
				assert.Equal(t, owned, tc.owned[pass])
				assert.Equal(t, len(localClaims) != 0 && tc.owned[pass], tc.orphaned[pass])
				o.classifyCompleted()
				info, _ := st.GetStatus(child.Named())
				assert.Equal(t, info.Status == store.StatusReady, tc.orphaned[pass])
				assert.Equal(t, len(o.orphans) != 0, tc.orphaned[pass])
				assert.Equal(t, o.finalize() != nil, !tc.orphaned[pass])
				assert.Diff(t, previousAll, savedAll)
				assert.Diff(t, previousLocal, savedLocal)
				assert.Diff(t, index.Records, records)
				assert.Diff(t, index.Claims, claims)
				assert.Diff(t, index.Readers, readers)
				previousAll, previousLocal = all, localClaims
				savedAll, savedLocal = slices.Clone(all), slices.Clone(localClaims)
			}
		})
	}
}

func TestClassifyCompleted_ZeroClaims(t *testing.T) {
	t.Parallel()
	st := store.New()
	child := &manifest.HelmRelease{Name: "child", Namespace: "apps"}
	st.AddObject(child)
	st.UpdateStatus(child.Named(), store.StatusInfo{Status: store.StatusFailed, Message: "failed"})
	index := indexForTest(map[manifest.NamedResource]string{child.Named(): "apps/child.yaml"})
	o := &Orchestrator{store: st, index: index}
	for range 2 {
		o.classifyCompleted()
		info, _ := st.GetStatus(child.Named())
		assert.Equal(t, info.Status, store.StatusFailed)
		assert.Equal(t, len(o.orphans), 0)
	}
}

func TestAggregateFailures_Deterministic(t *testing.T) {
	t.Parallel()
	a, b := testutil.KustomizationID("apps", "a"), testutil.KustomizationID("apps", "b")
	o := &Orchestrator{store: store.New(), index: &manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{a: {File: "a.yaml"}}}}
	var previous string
	for _, ids := range [][]manifest.NamedResource{{b, a}, {a, b}} {
		failed := make(map[manifest.NamedResource]store.StatusInfo)
		for _, id := range ids {
			failed[id] = store.StatusInfo{Status: store.StatusFailed, Message: "invalid input"}
		}
		got := o.aggregateFailures(failed)
		if !errors.Is(got, manifest.ErrFlux) || strings.Count(got.Error(), a.String()) != 1 || strings.Count(got.Error(), b.String()) != 1 || strings.Count(got.Error(), "invalid input") != 2 {
			t.Fatalf("aggregate lost identities or causes: %v", got)
		}
		if previous != "" {
			assert.Equal(t, got.Error(), previous)
		}
		previous = got.Error()
		assert.Equal(t, failed[a].String(), "invalid input")
	}
}

func TestFinalize_ProjectedDeclarationCount(t *testing.T) {
	var logs bytes.Buffer
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug})))
	t.Cleanup(func() { slog.SetDefault(previous) })
	root := t.TempDir()
	testutil.WriteFile(t, root, "kustomization.yaml", "resources: [ks.yaml]\n")
	testutil.WriteFile(t, root, "ks.yaml", testutil.KustomizationYAML("parent", "flux-system", "  path: ./app\n  targetNamespace: apps\n"))
	testutil.WriteFile(t, root, "app/hr.yaml", "apiVersion: helm.toolkit.fluxcd.io/v2\nkind: HelmRelease\nmetadata: {name: child}\nspec: {suspend: true, chartRef: {kind: OCIRepository, name: missing}}\n")
	o, err := newTestOrchestrator(t, Tree{Path: root, RepoRoot: root}, Config{Concurrency: 2})
	assert.NoError(t, err)
	_, err = o.render(t.Context())
	assert.NoError(t, err)
	assert.Equal(t, len(o.store.ListObjects(manifest.HelmReleaseDomain, manifest.KindHelmRelease)), 1)
	projected := testutil.HelmReleaseID("apps", "child")
	assert.Equal(t, o.store.GetObject(projected) != nil, true)
	info, _ := o.store.GetStatus(projected)
	assert.Equal(t, info.Outcome(), store.OutcomeSuspended)
	raw := testutil.HelmReleaseID("", "child")
	record, known := o.index.Lookup(raw)
	assert.Equal(t, known && record.Declaration != nil, true)
	assert.Equal(t, o.store.GetObject(raw) == nil, true)
	parent, _ := o.store.GetStatus(record.Parent)
	assert.Equal(t, parent.Outcome(), store.OutcomePassed)
	var counts []int
	for line := range bytes.Lines(logs.Bytes()) {
		var event struct {
			Message      string `json:"msg"`
			HelmReleases int    `json:"helm_releases"`
		}
		assert.NoError(t, json.Unmarshal(line, &event))
		if event.Message == "orchestrator: reconcile complete" {
			counts = append(counts, event.HelmReleases)
		}
	}
	assert.Diff(t, counts, []int{1})
}

func TestFinalize_DispatchPanic(t *testing.T) {
	t.Parallel()
	for _, workers := range []int{2, 16} {
		t.Run(fmt.Sprintf("workers=%d", workers), func(t *testing.T) {
			t.Parallel()
			st, tasks := store.New(), task.NewBounded(workers)
			id := testutil.KustomizationID("apps", "panic")
			st.AddObject(&manifest.Kustomization{Name: id.Name, Namespace: id.Namespace})
			s := schedule.New(tasks, fixtureDispatcher{store: st, dispatch: func(context.Context, schedule.NodeID, manifest.DrainLevel) schedule.Result {
				panic("dispatch panic")
			}})
			s.Seed([]schedule.NodeID{id})
			assert.NoError(t, s.Run(t.Context()))
			tasks.BlockTillDone()
			assert.Equal(t, tasks.Failures(), int64(1))
			assert.Equal(t, len(s.Results()), 0)
			assert.Equal(t, t.Context().Err(), nil)
			o := &Orchestrator{store: st, tasks: tasks}
			err := o.finalize()
			if err == nil || !strings.Contains(err.Error(), "1 task(s) panicked without per-resource attribution") {
				t.Fatalf("poisoned task service finalized successfully: %v", err)
			}
		})
	}
}
