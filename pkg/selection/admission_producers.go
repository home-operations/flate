package selection

import (
	"cmp"
	"context"
	"slices"
	"strings"

	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

type producerObligation struct {
	consumer   manifest.NamedResource
	production manifest.Production
}

func sortProducerObligations(obligations []producerObligation) {
	slices.SortFunc(obligations, func(a, b producerObligation) int {
		return cmp.Or(
			a.consumer.Compare(b.consumer),
			a.production.Owner.Compare(b.production.Owner),
			a.production.Declaration.Compare(b.production.Declaration),
			a.production.Authored.Compare(b.production.Authored),
			a.production.Source.Compare(b.production.Source),
			strings.Compare(a.production.Path, b.production.Path),
			strings.Compare(a.production.File, b.production.File),
		)
	})
}

func (c *admissionClosure) requestProducer(consumer manifest.NamedResource, production *manifest.Production) {
	owner := production.Materializer(consumer)
	if owner == (manifest.NamedResource{}) || owner == consumer || !owner.KindInfo().Structural {
		return
	}
	obligation := producerObligation{consumer: consumer, production: *production}
	if !slices.Contains(c.proposal.producers, obligation) {
		c.proposal.producers = append(c.proposal.producers, obligation)
	}
	preparation := c.proposal.preparation
	if preparation.producerOwner == nil {
		preparation.producerOwner = &c.proposal.producers[0]
	}
	c.proposal.producerOwner = preparation.producerOwner
}

func (c *admissionClosure) producerWitness(obligation *producerObligation) (store.Child, bool) {
	retained := &obligation.production
	var state admissionState
	if !slices.ContainsFunc(
		c.currentSourceProductions(obligation.consumer, &state),
		func(current manifest.Production) bool {
			if current.Materializer(obligation.consumer) != retained.Materializer(obligation.consumer) {
				return false
			}
			// Physical paths MUST dominate diff coordinates for the same source-qualified occurrence.
			if current.Path != "" && retained.Path != "" {
				current.File = retained.File
			} else {
				current.Path = retained.Path
			}
			return current == *retained
		},
	) {
		return store.Child{}, false
	}
	if c.objects.GetDeclaration(obligation.consumer) != nil {
		return store.Child{}, false
	}
	facts := c.facts.RootEvidence(obligation.consumer, c.proposal.reads)
	if facts.BuiltCurrent && facts.BuiltChild.Obj != nil {
		return store.Child{}, false
	}
	owner := retained.Materializer(obligation.consumer)
	if object := c.objects.GetDeclaration(owner); object != nil {
		return store.Child{Obj: object}, true
	}
	record, _ := c.facts.SelectionIndex().Lookup(owner)
	if record.Builder != "" {
		facts := c.facts.RootEvidence(owner, c.proposal.reads)
		if facts.BuiltCurrent {
			return facts.BuiltChild, true
		}
	}
	return store.Child{}, true
}

// RefreshProducers MUST recheck the exact pending production after its native prerequisite changes.
// The caller retains its logical Preparation and retires the result after consuming its demands.
func (s *Session) RefreshProducers(
	ctx context.Context,
	previous *Admission,
	request PrepareRequest, //nolint:gocritic // The frontier borrows the driver's immutable acquisition snapshot.
) (*Admission, error) {
	if err := ctx.Err(); err != nil {
		return nil, err //nolint:wrapcheck // Cancellation preserves the caller context error identity.
	}
	var preparation *Preparation
	if request.Preparation != nil {
		preparation = *request.Preparation
	}
	if previous == nil || preparation == nil || preparation.closure.session != s ||
		len(previous.producers) != 0 && previous.producerOwner != preparation.producerOwner {
		return nil, manifest.NewError(manifest.ErrInput, nil, "producer frontier requires its admission preparation")
	}
	preparation.acceptAcquired(request.Acquired)
	proposal := &Admission{
		Request: request, preparation: preparation, entry: s.entry.Load(), Revision: s.Snapshot().version(),
		producerOwner: preparation.producerOwner,
		stale:         true, // Physical acquisition frontiers MUST never publish membership.
	}
	proposal.reads = &proposal.owned
	proposal.reads.Init(s.store, manifest.NamedResource{}, nil)
	closure := admissionClosure{session: s, facts: s.facts, objects: proposal.reads, proposal: proposal}
	defer func() {
		if closure.deriving {
			closure.objects.WorkScope().Admission().Finish(ctx.Err())
		}
	}()
	for i := range previous.producers {
		obligation := &previous.producers[i]
		if err := ctx.Err(); err != nil {
			proposal.Retire()
			return nil, err //nolint:wrapcheck // Cancellation preserves the caller context error identity.
		}
		child, required := closure.producerWitness(obligation)
		if !required {
			continue
		}
		closure.requestProducer(obligation.consumer, &obligation.production)
		if child.Obj != nil {
			closure.acquire(Demand{Identity: child.Obj.Named(), Witness: child.Obj})
		}
	}
	sortProducerObligations(proposal.producers)
	return proposal, nil
}
