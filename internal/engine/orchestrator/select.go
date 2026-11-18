package orchestrator

import (
	"errors"
	"fmt"
	"slices"

	"github.com/home-operations/flate/internal/engine/selection"
	"github.com/home-operations/flate/pkg/manifest"
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

// Select MUST resolve private owned identities before projecting errors and results.
func (r *Invocation) Select(
	ids []manifest.TreeID,
	roots []manifest.NamedResource,
	labels map[string]string,
) ([]*selection.Result, selection.ProjectionErrors) {
	runs, err := r.ownedSides(ids)
	if err != nil {
		return nil, selection.ProjectionErrors{Selected: err, Remaining: err}
	}
	_, grouped := ownedErrors(r.err, ids)
	return Select(runs, grouped, roots, labels)
}

func (r *Invocation) ownedSides(ids []manifest.TreeID) ([]Rendered, error) {
	if len(ids) < 1 || len(ids) > 2 {
		return nil, manifest.NewError(
			manifest.ErrInput,
			nil,
			"selection requires one head or an ordered base/head pair",
		)
	}
	runs := make([]Rendered, len(ids))
	for i, id := range ids {
		index, found := r.byID[id]
		if !found {
			return nil, manifest.NewError(
				manifest.ErrInput,
				nil,
				fmt.Sprintf("selection tree %s/%d is not owned by this run", id.Cluster, id.Side),
			)
		}
		runs[i] = r.results[index]
	}
	if len(ids) == 1 {
		if ids[0].Side != manifest.TreeHead {
			return nil, manifest.NewError(manifest.ErrInput, nil, "selection of one side requires a head")
		}
	} else if ids[0].Cluster != ids[1].Cluster || ids[0].Side != manifest.TreeBase || ids[1].Side != manifest.TreeHead {
		return nil, manifest.NewError(
			manifest.ErrInput,
			nil,
			"selection requires an ordered base/head pair from one cluster",
		)
	}
	return runs, nil
}

func ownedErrors(err error, ids []manifest.TreeID) (bool, error) {
	if err == nil {
		return true, nil
	}
	if attributed, ok := err.(*treeError); ok { //nolint:errorlint // Only the current attribution node owns its complete error subtree.
		if slices.Contains(ids, attributed.id) {
			return true, err
		}
		return false, nil
	}
	if joined, ok := err.(interface{ Unwrap() []error }); ok {
		children := joined.Unwrap()
		projected := make([]error, len(children))
		complete := true
		for i, child := range children {
			var kept bool
			kept, projected[i] = ownedErrors(child, ids)
			complete = complete && kept
		}
		if complete {
			return true, err
		}
		return false, errors.Join(projected...)
	}
	if child := errors.Unwrap(err); child != nil {
		complete, projected := ownedErrors(child, ids)
		if !complete {
			return false, projected
		}
	}
	return true, err
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
