package admissiontest

import (
	"context"
	"slices"
	"testing"

	"github.com/home-operations/flate/pkg/loader"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/selection"
)

// Prepare MUST preserve one preparation across physical acquisition retries.
//
//nolint:gocritic // The fixture retains its own immutable request snapshot.
func Prepare(
	ctx context.Context,
	tb testing.TB,
	l *loader.Loader,
	request selection.PrepareRequest,
) (*selection.Admission, error) {
	tb.Helper()
	if request.Preparation == nil {
		var preparation *selection.Preparation
		request.Preparation = &preparation
		tb.Cleanup(func() { preparation.Retire() })
	}
	request.Changes = l.Options.Changes
	if request.Missing == nil {
		request.Missing = l.Options.Missing
	}
	proposal, err := l.Options.Session.Prepare(ctx, request)
	if err != nil {
		return nil, err
	}
	for {
		demands := slices.Collect(proposal.Demands())
		if len(demands) == 0 {
			return proposal, nil
		}
		acquired := make([]selection.Acquisition, 0, len(demands))
		for i := range demands {
			demand := &demands[i]
			result := selection.Acquisition{Demand: *demand, Reads: demand.Reads}
			if demand.History && demand.Root != "" {
				result.Root, result.Available, err = l.ResolveRoot(ctx, demand.Reads, demand.Source, demand.Root)
			} else if !demand.History && demand.Occurrence.Identity != (manifest.NamedResource{}) {
				result.Child, err = l.OccurrenceObject(ctx, &demand.Occurrence, demand.Artifact)
				result.Available = result.Child.Obj != nil
			}
			if err != nil {
				proposal.Retire()
				return nil, err
			}
			acquired = append(acquired, result)
		}
		request = proposal.Request
		request.Acquired = acquired
		next, err := l.Options.Session.Prepare(ctx, request)
		proposal.Retire()
		if err != nil {
			return nil, err
		}
		proposal = next
	}
}

// Execute MUST service every demand before the fixture publishes membership.
func Execute(tb testing.TB, l *loader.Loader, proposal *selection.Admission) {
	tb.Helper()
	if l.Options.Session == nil {
		return
	}
	request := selection.PrepareRequest{Invocation: true, Missing: l.Options.Missing}
	if proposal != nil {
		request = proposal.Request
		proposal.Retire()
	}
	request.Changes = l.Options.Changes
	_, committed, err := l.Options.Session.Admit(tb.Context(), selection.AdmissionOptions{
		Request: request,
		Stage:   selection.Execution,
	})
	if err != nil {
		tb.Fatal(err)
	}
	if !committed {
		tb.Fatal("fixture admission capture became invalid")
	}
}
