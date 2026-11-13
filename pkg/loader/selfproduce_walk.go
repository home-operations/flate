package loader

import (
	"context"

	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source/safepath"
)

type productionDeclarations []producerDeclaration

func (ds *productionDeclarations) recordVisit(
	ctx context.Context,
	l *Loader,
	b *selfProduceBuilder,
	vArg *graphVisit,
) error {
	v := *vArg
	if v.readable || l.documents.compact && v.deferDecode && safepath.Contains(v.root, v.file) {
		var err error
		*ds, err = b.recordProduced(ctx, new(v), *ds, l)
		return err
	}
	return nil
}

func (ds *productionDeclarations) recordContributions(
	ctx context.Context,
	l *Loader,
	b *selfProduceBuilder,
	record func(context.Context, graphVisit) error,
) error {
	{
		contributionValues := l.scopes.contributions
		for contributionIndex := range contributionValues {
			contribution := contributionValues[contributionIndex]
			if !contribution.active || contribution.membershipOnly {
				continue
			}
			{
				vValues := contribution.visits
				for vIndex := range vValues {
					v := vValues[vIndex]
					v.owner, v.source = contribution.owner, contribution.source
					v.requiresBuild = v.requiresBuild && contribution.applies
					if err := record(ctx, v); err != nil {
						return err
					}
				}
			}
			b.joinProductions(*ds, contribution.visits)
			*ds = nil
		}
	}
	return nil
}

func (ds *productionDeclarations) recordStandalone(
	ctx context.Context,
	b *selfProduceBuilder,
	record func(context.Context, graphVisit) error,
	contribution *scopeContribution,
) error {
	{
		vValues3 := contribution.visits
		for vIndex3 := range vValues3 {
			v := vValues3[vIndex3]
			v.owner = manifest.NamedResource{}
			v.source = manifest.BootstrapSourceID
			v.requiresBuild = v.requiresBuild && contribution.applies
			if err := record(ctx, v); err != nil {
				return err
			}
		}
	}
	b.joinProductions(*ds, contribution.visits)
	*ds = nil
	return nil
}
