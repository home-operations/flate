package cache

import (
	"context"
	"iter"

	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

// RenderAcceptance MUST retain complete emission evidence and live captures through publication.
type RenderAcceptance struct {
	Owner       manifest.NamedResource
	Receipt     *manifest.RenderReceipt
	Records     iter.Seq2[manifest.NamedResource, manifest.DiscoveryRecord]
	Sources     []manifest.SourceEvidence
	Content     any
	Work        *manifest.RenderWork
	Source      manifest.SourceIdentity
	Reads       *store.ReadSet
	OutputReads *store.ReadSet
}

// RenderPublication MUST project engine metadata without publishing or encoding it.
type RenderPublication struct {
	Frame    RenderFrame
	Domain   string
	Metadata any
	Content  any
	Sources  []manifest.SourceEvidence
}

// Nil acceptance MUST validate the receipt without preparing persistent data.
type renderPublisher interface {
	RenderPublication(context.Context, *RenderAcceptance) (RenderPublication, error)
}

func (a *RenderAcceptance) complete() bool {
	return a.Receipt != nil && a.Records != nil && a.Sources != nil
}

func (a *RenderAcceptance) valid() bool {
	return a.Reads.Valid() && a.OutputReads.Valid()
}

// RecordRender MUST publish accepted frames and searchable source evidence together.
//
//nolint:gocritic // Preserve the public value API and its accepted-inventory snapshot.
func (c *Cache) RecordRender(ctx context.Context, acceptance RenderAcceptance) error {
	if !acceptance.complete() || !acceptance.valid() {
		return nil
	}
	publisher, ok := acceptance.Receipt.Value().(renderPublisher)
	if !ok {
		return manifest.NewError(manifest.ErrFlux, nil, "invalid render receipt")
	}
	if !c.CanPersistRenders() {
		if _, err := publisher.RenderPublication(ctx, nil); err != nil {
			return err
		}
		return ctx.Err()
	}
	snapshot := acceptance
	projection, err := publisher.RenderPublication(ctx, &snapshot)
	if err != nil {
		return err
	}
	if projection.Content == nil || projection.Sources == nil {
		return nil
	}
	frame, err := projection.Frame.Encode(projection.Domain, projection.Metadata)
	if err != nil || frame == nil {
		return err
	}
	publication, err := frame.PreparePublication(ctx, FramePublicationInput{
		Owner: acceptance.Owner, Content: projection.Content, Records: acceptance.Records, Complete: true,
	})
	if err != nil || publication == nil {
		return err
	}
	if !acceptance.valid() {
		return nil
	}
	return RecordInventory(ctx, c, AcceptedInventory[*FramePublication]{
		Key: projection.Frame.Key, Lookup: projection.Frame.Lookup, Acceptance: publication.Identity(),
		Work: acceptance.Work.History(), History: RenderHistory{Owner: acceptance.Owner, Sources: projection.Sources},
		Content: publication,
	})
}
