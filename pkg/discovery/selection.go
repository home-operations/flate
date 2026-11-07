package discovery

import (
	"context"

	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/selection"
)

func (d *Session) prepare(
	ctx context.Context,
	request selection.PrepareRequest, //nolint:gocritic // Each acquisition round owns an independent immutable request snapshot.
) (*selection.Admission, error) {
	proposal, err := d.loader.Options.Session.Prepare(ctx, request)
	if err != nil {
		return nil, err
	}
	for {
		var acquired []selection.Acquisition
		for demand := range proposal.Demands() {
			result := selection.Acquisition{Demand: demand, Reads: demand.Reads}
			switch {
			case demand.History && demand.Root != "":
				result.Root, result.Available, err = d.loader.ResolveRoot(ctx, demand.Reads, demand.Source, demand.Root)
			case demand.Occurrence.Identity != (manifest.NamedResource{}):
				result.Child, err = d.loader.OccurrenceObject(ctx, &demand.Occurrence, demand.Artifact)
				result.Available = result.Child.Obj != nil
			default:
				continue
			}
			if err != nil {
				proposal.Retire()
				return nil, err
			}
			acquired = append(acquired, result)
		}
		if len(acquired) == 0 {
			return proposal, nil
		}
		request.Acquired = acquired
		previous := proposal
		proposal, err = d.loader.Options.Session.Prepare(ctx, request)
		previous.Retire()
		if err != nil {
			return nil, err
		}
	}
}
