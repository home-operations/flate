package orchestrator

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/home-operations/flate/pkg/loader"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/change"
	"github.com/home-operations/flate/pkg/controllers/base"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/schedule"
	"github.com/home-operations/flate/pkg/store"
	"github.com/home-operations/flate/pkg/task"
)

func testRenderChangedLiteralEntryDrainsSettledCandidates(t *testing.T) {
	t.Helper()
	for _, first := range []string{"a", "b"} {
		t.Run(first+"_first", func(t *testing.T) {
			trees := changedLiteralEntryTrees(t, 1)
			for side, tree := range trees {
				writeSettledEntryCandidates(t, tree.RepoRoot, side)
			}
			full := make([]Rendered, len(trees))
			for side, tree := range trees {
				runs, err := RenderTrees(t.Context(), []Tree{tree}, Config{Concurrency: 2, CacheDir: t.TempDir()})
				assert.NoError(t, err)
				full[side] = runs[0]
			}
			want := stagePrecisionDiff(t, full)
			for range 2 {
				runs := make([]Rendered, len(trees))
				for side, tree := range trees {
					tree.ExternalChanges = change.NewSet([]string{"entry/cm.yaml", "charts/settled/templates/cm.yaml"})
					runs[side] = renderSettledEntryCandidates(t, &tree, first)
				}
				assert.Equal(t, string(stagePrecisionDiff(t, runs)), string(want))
			}
		})
	}
}

func writeSettledEntryCandidates(t *testing.T, root string, side int) {
	t.Helper()
	for _, name := range []string{"a", "b"} {
		testutil.WriteFile(t, root, "entry/"+name+".yaml", fmt.Sprintf(`apiVersion: helm.toolkit.fluxcd.io/v2
kind: HelmRelease
metadata: {name: %s, namespace: apps}
spec:
  interval: 1h
  chart: {spec: {chart: charts/settled, sourceRef: {kind: GitRepository, name: self}}}
`, name))
	}
	testutil.WriteFile(t, root, "charts/settled/Chart.yaml", "apiVersion: v2\nname: settled\nversion: 1.0.0\n")
	testutil.WriteFile(t, root, "charts/settled/templates/cm.yaml", fmt.Sprintf(`apiVersion: v1
kind: ConfigMap
metadata: {name: {{ .Release.Name }}, namespace: apps}
data: {value: unchanged}
# side-%d
`, side))
}

func renderSettledEntryCandidates(t *testing.T, tree *Tree, first string) Rendered {
	t.Helper()
	second := "b"
	if first == second {
		second = "a"
	}
	var mu sync.Mutex
	var published []string
	o, err := newTestOrchestrator(t, *tree, Config{
		Concurrency: 2,
		CacheDir:    t.TempDir(),
		StatusListener: func(_ manifest.TreeID, subject manifest.DiagnosticIdentity, info store.StatusInfo) {
			if subject.ID.Kind == manifest.KindHelmRelease && info.Outcome() == store.OutcomePassed &&
				(subject.ID.Name == first || subject.ID.Name == second) {
				mu.Lock()
				published = append(published, subject.ID.Name)
				mu.Unlock()
			}
		},
	})
	assert.NoError(t, err)
	assert.NoError(t, o.bootstrap(t.Context()))
	var reads store.ReadSet
	reads.Init(o.store, manifest.NamedResource{}, nil)
	for _, name := range []string{first, second} {
		reads.RequireData(testutil.HelmReleaseID("apps", name))
	}
	_, err = o.ksc.KeepEmitted(
		t.Context(),
		manifest.NamedResource{},
		loader.PrepareRequest{Generation: nil, Reads: &reads},
	)
	reads.Retire()
	assert.NoError(t, err)
	o.controllers.HelmRelease = &settledEntryController{
		reconcileController: o.hrc,
		tasks:               o.tasks,
		first:               first,
		second:              second,
		ready:               make(chan struct{}),
	}
	result, err := o.collectResult(o.run(t.Context()))
	assert.NoError(t, err)
	assert.Diff(t, published, []string{first, second})
	status, ok := o.store.GetStatus(testutil.HelmReleaseID("apps", "generator"))
	assert.Equal(t, ok, true)
	assert.Equal(t, status.Outcome(), store.OutcomePassed)
	assert.Equal(t, o.entryBinding.Producer != (manifest.NamedResource{}), true)
	return Rendered{Orchestrator: o, ID: tree.ID, Result: result, Err: err}
}

type settledEntryController struct {
	reconcileController

	tasks         *task.Service
	first, second string
	ready         chan struct{}
	once          sync.Once
}

func (c *settledEntryController) ReconcileNode(
	ctx context.Context,
	id manifest.NamedResource,
	level manifest.DrainLevel,
	previous schedule.Result,
) *base.Execution {
	if id.Name == c.second && level == manifest.DrainNone {
		c.tasks.YieldSlot(func() {
			select {
			case <-c.ready:
			case <-ctx.Done():
			}
		})
	}
	execution := c.reconcileController.ReconcileNode(ctx, id, level, previous)
	if id.Name == c.first && execution != nil && execution.Outcome() == schedule.Terminal {
		c.once.Do(func() { close(c.ready) })
	}
	return execution
}
