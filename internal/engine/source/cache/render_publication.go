package cache

import (
	"context"
	"io"
	"iter"

	"github.com/home-operations/flate/internal/engine/store"
	"github.com/home-operations/flate/pkg/manifest"
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

// WritePending stores raw output without granting ownership or acceptance.
func (d *RenderDomain[P, H]) WritePending(ctx context.Context, w io.Writer, f *RenderReceipt[P, H]) error {
	if d.Prepare != nil {
		f.Header = d.Prepare(f.Header)
	}
	var err error
	f.Stored, err = f.Encode(d.Name, f.Header)
	if err != nil || f.Stored == nil {
		return err
	}
	publication, err := f.Stored.PreparePublication(ctx, FramePublicationInput{})
	if err == nil {
		_, err = publication.WriteTo(w)
	}
	return err
}

// RenderPublication prepares acceptance from a copy of the immutable receipt.
func (f RenderReceipt[P, H]) RenderPublication(_ context.Context, a *RenderAcceptance) (RenderPublication, error) {
	d := f.domain
	if d == nil {
		return RenderPublication{}, manifest.NewError(manifest.ErrFlux, nil, "invalid render receipt")
	}
	if !d.matches(&f, f.Key) {
		return RenderPublication{}, manifest.NewError(manifest.ErrFlux, nil, "invalid "+d.Name+" receipt")
	}
	if a == nil {
		return RenderPublication{}, nil
	}
	content, sources := d.Accept(f.Header, a)
	if content == nil || sources == nil {
		return RenderPublication{}, nil
	}
	if f.Binding == "" {
		// Only the unbound path needs heap storage for the fingerprint input.
		inputs := f.Inputs
		f.Rebind(inputs.Fingerprint())
	}
	var metadata any
	if f.Stored == nil {
		if d.Prepare != nil {
			f.Header = d.Prepare(f.Header)
		}
		metadata = f.Header
	}
	return RenderPublication{
		Frame: f.RenderFrame, Domain: d.Name, Metadata: metadata, Content: content, Sources: sources,
	}, nil
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
