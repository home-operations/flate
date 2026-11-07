package loader

import (
	"context"
	"slices"
	"testing"

	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/selection"
	"github.com/home-operations/flate/pkg/store"
)

func configureSelection(tb testing.TB, l *Loader, request selection.Request) {
	tb.Helper()
	session, err := selection.New(l.Store, l, request)
	if err != nil {
		tb.Fatal(err)
	}
	l.Options.Session = session
}

//nolint:gocritic // Each proposal MUST retain an independent immutable request snapshot.
func prepareSelection(
	ctx context.Context,
	tb testing.TB,
	l *Loader,
	request selection.PrepareRequest,
) (*selection.Admission, error) {
	tb.Helper()
	if request.Preparation == nil {
		var preparation *selection.Preparation
		request.Preparation = &preparation
		tb.Cleanup(func() { preparation.Retire() })
	}
	request.Changes = l.Options.Changes
	if request.AllowMissingKinds == nil {
		request.AllowMissingKinds = l.Options.AllowMissingKinds
	}
	proposal, err := l.Options.Session.Prepare(ctx, request)
	for err == nil {
		var acquired []selection.Acquisition
		acquired, err = l.acquireSelection(ctx, proposal)
		if err != nil {
			proposal.Retire()
			return nil, err
		}
		if len(acquired) == 0 {
			return proposal, nil
		}
		request = proposal.Request
		request.Acquired = acquired
		next, nextErr := l.Options.Session.Prepare(ctx, request)
		proposal.Retire()
		proposal, err = next, nextErr
	}
	return nil, err
}

func (l *Loader) commitSelection(
	ctx context.Context,
	tb testing.TB,
	proposal *selection.Admission,
	stage selection.CommitStage,
) (*selection.Admission, bool, error) {
	tb.Helper()
	for {
		current, committed, err := l.Options.Session.Commit(ctx, proposal, stage)
		if err != nil || committed || current == nil {
			return current, committed, err
		}
		acquired, err := l.acquireSelection(ctx, current)
		if err != nil {
			current.Retire()
			return nil, false, err
		}
		if len(acquired) == 0 {
			return current, false, nil
		}
		request := current.Request
		request.Acquired = acquired
		next, err := prepareSelection(ctx, tb, l, request)
		current.Retire()
		if err != nil {
			return nil, false, err
		}
		proposal = next
	}
}

func (l *Loader) acquireSelection(ctx context.Context, proposal *selection.Admission) ([]selection.Acquisition, error) {
	demands := slices.Collect(proposal.Demands())
	acquired := make([]selection.Acquisition, 0, len(demands))
	for i := range demands {
		demand := &demands[i]
		result := selection.Acquisition{Demand: *demand, Reads: demand.Reads}
		var err error
		if demand.History {
			if demand.Root != "" {
				result.Root, result.Available, err = l.ResolveRoot(ctx, demand.Reads, demand.Source, demand.Root)
			}
		} else if demand.Occurrence.Identity != (manifest.NamedResource{}) {
			result.Child, err = l.OccurrenceObject(ctx, &demand.Occurrence, demand.Artifact)
			result.Available = result.Child.Obj != nil
		}
		if err != nil {
			return nil, err
		}
		acquired = append(acquired, result)
	}
	return acquired, nil
}

func proposalSelected(decision *selection.Decision, index *manifest.DiscoveryIndex) []manifest.NamedResource {
	var selected []manifest.NamedResource
	for id := range index.Records {
		if decision.Output(id) {
			selected = append(selected, id)
		}
	}
	return selected
}

func requireSelection(
	tb testing.TB,
	l *Loader,
	ids []manifest.NamedResource,
	qualification bool,
) (*selection.Admission, bool, error) {
	tb.Helper()
	var reads store.ReadSet
	reads.Init(l.Store, manifest.NamedResource{}, nil)
	defer reads.Retire()
	for _, id := range ids {
		if qualification {
			reads.RequireQualification(id)
		} else {
			reads.RequireData(id)
		}
	}
	proposal, err := prepareSelection(tb.Context(), tb, l, selection.PrepareRequest{Reads: &reads})
	if err != nil {
		return nil, false, err
	}
	return l.commitSelection(tb.Context(), tb, proposal, selection.Execution)
}

func rootAtForTest(l *Loader, object manifest.BaseManifest, file string) selection.RootEvidence {
	if object == nil {
		return selection.RootEvidence{}
	}
	record, known := l.Index.Lookup(object.Named())
	if !known || record.Path != file {
		return selection.RootEvidence{}
	}
	return l.RootEvidence(object.Named(), nil)
}

func standaloneRootsForTest(t *testing.T, l *Loader, root string) []string {
	t.Helper()
	l.Admit(root)
	if err := l.captureNativeRoots(root); err != nil {
		t.Fatal(err)
	}
	var paths []string
	for i := range l.scopes.rootFacts {
		facts := &l.scopes.rootFacts[i]
		if selection.Root(selection.RootEvidence{Native: facts}).Construction {
			paths = append(paths, facts.Path)
		}
	}
	return paths
}
