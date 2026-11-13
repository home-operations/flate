package orchestrator

import (
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/selection"
)

// Select borrows completed results until their final consumer calls Close.
func Select(
	runs []Rendered,
	runErr error,
	roots []manifest.NamedResource,
	labels map[string]string,
) ([]*selection.Result, selection.ProjectionErrors) {
	snapshots := make([]selection.Snapshot, len(runs))
	for i, run := range runs {
		snapshots[i] = renderSnapshot(run)
	}
	return selection.Project(snapshots, runErr, selection.Request{Roots: roots, Labels: labels})
}

func renderSnapshot(run Rendered) selection.Snapshot {
	snapshot := selection.Snapshot{
		Absent:        run.Absent,
		HasResult:     run.Result != nil,
		Err:           run.Err,
		ErrorIdentity: run.Err,
	}
	if attributed, ok := run.Err.(*treeError); ok { //nolint:errorlint // Strip only the attribution node, preserving joined siblings.
		snapshot.Err = attributed.err
	}
	if result := run.Result; result != nil {
		if result.catalog != nil {
			snapshot.Facts = result.catalog
		}
		snapshot.Manifests, snapshot.Emissions = result.Manifests, result.emissions
		snapshot.Failed, snapshot.Blocked = result.Failed, result.Blocked
		snapshot.Warnings, snapshot.ClusterWarnings = result.Warnings, result.ClusterWarnings
	}
	if run.Orchestrator == nil {
		return snapshot
	}
	o := run.Orchestrator
	snapshot.Store, snapshot.Index = o.store, o.index
	snapshot.Complete = run.Result != nil
	snapshot.Decision = o.selection.Snapshot()
	snapshot.Entry = selection.EntryIdentity{Producer: o.entryBinding.Producer, Key: o.entryBinding.Key}
	snapshot.ChangeNamespaces = o.changeNamespaces
	return snapshot
}
