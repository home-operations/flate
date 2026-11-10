package selection

import (
	"context"
	"fmt"
	"iter"

	"github.com/home-operations/flate/pkg/change"
	"github.com/home-operations/flate/pkg/manifest"
)

// EntryFailure borrows a retained document failure and its physical root evidence.
type EntryFailure struct {
	File       string
	Coordinate string
	Evidence   RootEvidence
	Occurrence manifest.DocOccurrence
}

// EntryFailure selects failures under the invocation's roots and qualified comparison.
func (s *Session) EntryFailure(ctx context.Context, failures iter.Seq[EntryFailure]) error {
	var request Request
	if s != nil {
		request = s.request
	}
	if !request.Matches(manifest.NamedResource{}) {
		return nil
	}
	var source SourceFacts
	if s != nil && request.Changed {
		source = s.facts.SourceFacts(manifest.BootstrapSourceID, nil)
	}
	for failure := range failures {
		if err := ctx.Err(); err != nil {
			return err //nolint:wrapcheck // Cancellation preserves the caller context error identity.
		}
		if request.Changed && source.Comparison != nil &&
			!source.Delta.FileChanged(change.FileContext{
				Root: source.Context.Root, Origin: source.Origin, ToIndex: source.ToIndex,
				Input: source.Input, Paired: source.Paired,
			}, failure.Coordinate) {
			continue
		}
		if Root(failure.Evidence).Admitted {
			return fmt.Errorf("entry %s: %w (invalid YAML)", failure.File,
				manifest.DocumentFailure([]manifest.DocOccurrence{failure.Occurrence}))
		}
	}
	return nil
}
