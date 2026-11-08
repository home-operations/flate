package orchestrator

import (
	"errors"
	"strings"
	"testing"

	"github.com/home-operations/flate/internal/selection"

	"github.com/home-operations/flate/internal/assert"

	"github.com/home-operations/flate/internal/store"
	"github.com/home-operations/flate/pkg/manifest"
)

//nolint:paralleltest // The test calls a helper that changes process-global state.
func TestStatusListener_RepresentativeProvenance(t *testing.T) {
	seen := map[manifest.NamedResource]int{}
	o, err := newTestOrchestrator(t, Tree{Path: t.TempDir()}, Config{
		Concurrency:    2,
		StatusListener: func(_ manifest.TreeID, subject manifest.DiagnosticIdentity, _ store.StatusInfo) { seen[subject.ID]++ },
	})
	assert.FatalIf(t, err != nil, "%v", err)
	defer o.stop()
	owner := &manifest.Kustomization{Name: "parent", Namespace: "apps"}
	chart := &manifest.HelmChartSource{Name: "auxiliary-chart", Namespace: "apps"}
	oci := &manifest.OCIRepository{Name: "auxiliary-oci", Namespace: "apps"}
	config := manifest.NewConfigMap(manifest.NamedResource{Name: "auxiliary-config", Namespace: "apps"}, nil)
	authored := &manifest.HelmChartSource{Name: "authored-chart", Namespace: "apps"}
	ordinary := manifest.NewConfigMap(manifest.NamedResource{Name: "ordinary", Namespace: "apps"}, nil)
	o.store.AddObject(owner)
	_, _, inventory := o.store.GetEmission(owner.Named())
	defer inventory.Finish()
	inventory.Children = []store.Child{
		{Obj: chart, Generated: true},
		{Obj: oci, Generated: true},
		{Obj: config, Generated: true},
		{Obj: authored},
		{Obj: ordinary},
	}
	o.store.RecordEmitted(owner.Named(), &inventory, nil)
	for _, object := range []manifest.BaseManifest{owner, chart, oci, config, authored, ordinary} {
		o.store.UpdateStatus(object.Named(), store.StatusInfo{Status: store.StatusPending})
	}
	o.store.UpdateStatus(manifest.BootstrapSourceID, store.StatusInfo{Status: store.StatusPending})
	for _, object := range []manifest.BaseManifest{owner, authored, ordinary} {
		if seen[object.Named()] != 1 {
			t.Fatalf("ordinary representative %s missing from progress: %v", object.Named(), seen)
		}
	}
	assert.FatalIf(t, len(seen) != 3, "auxiliary or bootstrap progress escaped: %v", seen)
	o.store.AddObject(chart)
	o.store.UpdateStatus(chart.Named(), store.StatusInfo{Status: store.StatusReady})
	assert.FatalIf(t, seen[chart.Named()] != 1, "%v", "independent chart winner retained generated visibility")
	o.store.UpdateFailure(oci.Named(), errors.New("source failed"))
	o.store.UpdateFailure(
		owner.Named(),
		&manifest.DependencyFailedError{Parent: owner.Named(), Failed: []manifest.NamedResource{oci.Named()}},
	)
	run := selectedRun(t,
		&Result{
			Failed:  o.store.FailedResources(),
			Blocked: map[manifest.NamedResource][]manifest.NamedResource{owner.Named(): {oci.Named()}},
		},
	)
	run.store = o.store
	run.Err = &selection.FailuresError{Message: "source failed"}
	projectedResults0, projectionErrors0 := Select(
		[]Rendered{run},
		run.Err,
		[]manifest.NamedResource{owner.Named()},
		nil,
	)
	selected, _, _ := projectedResults0, projectionErrors0.Selected, projectionErrors0.Remaining
	{
		_, visible := selected[0].Statuses[oci.Named()]
		assert.FatalIf(t, visible ||
			!strings.Contains(
				selected[0].Statuses[owner.Named()].String(),
				"source failed",
			), "%v", "hidden generated-source failure did not remain visible through its parent diagnostic")
	}
}
