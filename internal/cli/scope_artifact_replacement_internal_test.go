package cli

import (
	"testing"

	"github.com/home-operations/flate/internal/selection"

	helmv2 "github.com/fluxcd/helm-controller/api/v2"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/store"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/orchestrator"
)

func TestScopedFailures_CanonicalReplacementWithStaleArtifact(t *testing.T) {
	t.Parallel()
	root := writeNamedScopeFixture(t)
	flags := commonFlags{path: root, cacheDir: t.TempDir(), concurrency: 2}
	runs, runErr := renderTreesForTest(t.Context(), t, inputTrees(&flags), buildOrchCfg(&flags))
	assert.FatalIf(t, len(runs) != 1, "missing run: %v", runErr)
	assert.NoError(t, runErr)
	run := runs[0]
	o, res, err := run.Orchestrator, run.Result, run.Err
	assert.FatalIf(t, err != nil || res == nil, "fresh fixture failed: %v", err)
	id := testutil.HelmReleaseID("apps", "selected")
	hr, _ := o.Store().Get[*manifest.HelmRelease](id)
	artifact := o.Store().GetArtifact(id, store.ArtifactContent)
	assert.FatalIf(t, artifact == nil, "%v", "fresh render has no artifact")
	clone := hr.Clone()
	clone.ValuesFrom = []helmv2.ValuesReference{{Kind: "Secret", Name: "current"}}
	o.Store().AddObject(clone)
	current := manifest.NamedResource{Kind: "Secret", Namespace: "apps", Name: "current"}
	old := manifest.NamedResource{Kind: "Secret", Namespace: "apps", Name: "old"}
	var currentReads store.ReadSet
	currentReads.Init(o.Store(), id, nil)
	currentReads.GetObject(id)
	currentReads.RequireData(current)
	currentReads.Accept(store.AssessmentReconciled)
	t.Cleanup(currentReads.Retire)
	res = &orchestrator.Result{Failed: map[manifest.NamedResource]store.StatusInfo{
		id:      {Status: store.StatusFailed, Message: "selected failed after rendering"},
		current: {Status: store.StatusFailed, Message: "current input failure"},
		old:     {Status: store.StatusFailed, Message: "old input failure"},
	}}
	projectedResults0, projectionErrors0 := orchestrator.Select(
		[]orchestrator.Rendered{{Orchestrator: o, Result: res}},
		&selection.FailuresError{
			Message: "raw reconcile failures",
		},
		[]manifest.NamedResource{{Group: id.Group, Kind: id.Kind, Name: id.Name}},
		nil,
	)
	selectedResults, selectedErr, _ := projectedResults0, projectionErrors0.Selected, projectionErrors0.Remaining
	assert.FatalIf(t, len(selectedResults) != 1 || selectedErr == nil, "selection failed: %v", selectedErr)
	{
		_, ok := selectedResults[0].Failed[id]
		assert.FatalIf(t, !ok, "%v", "stale artifact weakened selected failure")
	}
	if len(selectedResults[0].Failed) != 2 || selectedResults[0].Failed[current].String() != "current input failure" ||
		len(
			selectedResults[0].Unrelated,
		) != 1 || selectedResults[0].Unrelated[0].Resource != old || o.Store().GetArtifact(id, store.ArtifactContent) != nil {
		t.Fatalf("projection did not follow current canonical refs: %+v", selectedResults[0])
	}
	_, _, accepted := o.Store().GetEmission(id)
	defer accepted.Finish()
	assert.Equal(t, len(accepted.Manifests), 0)
}
