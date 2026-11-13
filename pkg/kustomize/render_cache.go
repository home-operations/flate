package kustomize

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io/fs"
	"iter"
	"log/slog"
	"slices"

	"github.com/home-operations/flate/pkg/manifest"
	sourcecache "github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/store"
)

const frameDomain = "kustomize-render"

type renderHeader struct {
	Key     string          `json:"Key"`
	Binding string          `json:"Binding"`
	Reads   readSetSnapshot `json:"Reads"`
}

type renderBinding struct {
	Render   string                 `json:"Render"`
	Source   manifest.NamedResource `json:"Source"`
	Kind     string                 `json:"Kind"`
	URL      string                 `json:"URL"`
	Path     string                 `json:"Path"`
	Root     string                 `json:"Root"`
	Key      string                 `json:"Key"`
	Revision string                 `json:"Revision"`
	Digest   string                 `json:"Digest"`
	Alias    bool                   `json:"Alias"`
	Strict   bool                   `json:"Strict"`
	Skip     []string               `json:",omitempty"`
}

func (a *renderBinding) equal(b *renderBinding) bool {
	return a.Render == b.Render && a.Source == b.Source && a.Kind == b.Kind && a.URL == b.URL && a.Path == b.Path &&
		a.Root == b.Root &&
		a.Key == b.Key &&
		a.Revision == b.Revision &&
		a.Digest == b.Digest &&
		a.Alias == b.Alias &&
		a.Strict == b.Strict &&
		slices.Equal(a.Skip, b.Skip)
}

type renderFrame struct {
	lookup  string
	header  renderHeader
	binding renderBinding
	output  []byte
	stored  *sourcecache.Frame
}

type decodedRender struct {
	identity string
	frame    renderFrame
}

func (p *Preparation) readFrame(ctx context.Context, generation fs.FS) (renderFrame, bool, error) {
	identity := sourcecache.AcquisitionIdentity(generation)
	if p != nil && p.decoded != nil && identity != "" && p.decoded.identity == identity {
		return p.decoded.frame, true, nil
	}
	var frame renderFrame
	stored, err := sourcecache.OpenFrame(ctx, frameDomain, generation, &frame.header)
	if err == nil {
		frame.output, err = stored.Payload(ctx)
	}
	if err != nil {
		if _, ok := errors.AsType[*sourcecache.CacheError](err); ok {
			return renderFrame{}, false, manifest.NewError(
				manifest.ErrFlux, fmt.Errorf("read kustomize render cache: %w", err), "",
			)
		}
		slog.Debug("kustomize: decode render frame", "err", err)
		return renderFrame{}, false, nil
	}
	frame.stored = stored
	if p != nil && identity != "" {
		p.decoded = &decodedRender{identity: identity, frame: frame}
	}
	return frame, true, nil
}

func equalRenderFrame(a, b any) bool {
	left, ok := a.(renderFrame)
	right, other := b.(renderFrame)
	if !ok || !other || left.header.Key != right.header.Key || !left.binding.equal(&right.binding) ||
		!bytes.Equal(left.output, right.output) {
		return false
	}
	x, y := left.header.Reads, right.header.Reads
	return slices.Equal(x.Files, y.Files) && slices.Equal(x.Dirs, y.Dirs) && slices.Equal(x.Nodes, y.Nodes) &&
		slices.EqualFunc(x.Remotes, y.Remotes, func(a, b remoteEntry) bool {
			return a.Input == b.Input && a.Identity == b.Identity && slices.Equal(a.Parents, b.Parents)
		})
}

// PreviousRender returns validated raw output without rendering or acquiring remote inputs.
//
//nolint:gocritic // Preserve the existing value API and its snapshot semantics.
func (c *TreeCache) PreviousRender(
	ctx context.Context,
	req RenderRequest,
) ([]manifest.DocOccurrence, *manifest.RenderReceipt, bool) {
	if c == nil || ctx.Err() != nil || !c.owner.CanPersistRenders() {
		return nil, nil, false
	}
	prepared, err := c.prepareRender(ctx, &req)
	if err != nil {
		return nil, nil, false
	}
	reader, _, lookup, err := c.owner.Blob(
		ctx,
		sourcecache.BlobKey{
			Content: prepared.key,
			Render:  sourcecache.RenderAccepted,
		},
		&sourcecache.Policy{MaxAge: sourcecache.Forever, Work: req.Work.History()},
		nil,
	)
	if err != nil {
		return nil, nil, false
	}
	frame, decoded, err := req.Preparation.readFrame(ctx, reader)
	validBinding := err == nil && decoded && frame.header.Key == prepared.key &&
		frame.header.Binding == manifest.Fingerprint(prepared.binding)
	if !validBinding || len(frame.header.Reads.Remotes) != 0 {
		return nil, nil, false
	}
	valid, err := frame.header.Reads.localsValid(ctx, prepared.lower, req.Source.Content != "")
	if err != nil || !valid {
		return nil, nil, false
	}
	docs, err := manifest.DecodeRender(frame.output)
	if err != nil {
		return nil, nil, false
	}
	if ctx.Err() != nil {
		return nil, nil, false
	}
	frame.binding, frame.lookup = prepared.binding, lookup
	return docs, manifest.NewRenderReceipt(frame, equalRenderFrame), true
}

// RenderHistory returns complete accepted inventory matching the current final inputs.
func (*TreeCache) RenderHistory(
	owner manifest.NamedResource,
	receipt *manifest.RenderReceipt,
	content any,
) (iter.Seq2[manifest.NamedResource, manifest.DiscoveryRecord], bool) {
	if receipt == nil || content == nil {
		return nil, false
	}
	frame, ok := receipt.Value().(renderFrame)
	if !ok || frame.stored == nil {
		return nil, false
	}
	return frame.stored.History(owner, content)
}

// RenderAcceptance MUST retain the inventory and reads from the accepted emission.
type RenderAcceptance struct {
	Work *manifest.RenderWork `json:"-"`
	// Source identifies the admitted filesystem used by the receipt.
	Source  manifest.SourceIdentity   `json:"Source"`
	Sources []manifest.SourceEvidence `json:"Sources"`
	Owner   manifest.NamedResource    `json:"Owner"`
	Receipt *manifest.RenderReceipt   `json:"Receipt"`
	// Records MUST describe accepted output under Owner.
	Records     iter.Seq2[manifest.NamedResource, manifest.DiscoveryRecord] `json:"Records"`
	Content     any                                                         `json:"Content"`
	Reads       *store.ReadSet                                              `json:"Reads"`
	OutputReads *store.ReadSet                                              `json:"OutputReads"`
}

// RecordRender MUST publish history only after the emission accepts its captured inputs.
//
//nolint:gocritic // Preserve the public value API and its accepted-inventory snapshot.
func (c *TreeCache) RecordRender(ctx context.Context, acceptance RenderAcceptance) error {
	owner, receipt, records := acceptance.Owner, acceptance.Receipt, acceptance.Records
	if receipt == nil {
		return nil
	}
	finalContent, validContent := acceptance.Content.(renderContent)
	if records != nil {
		if !validContent || !acceptance.Reads.Valid() || !acceptance.OutputReads.Valid() {
			return nil
		}
	}
	frame, ok := receipt.Value().(renderFrame)
	if !ok || frame.header.Key == "" {
		return manifest.NewError(manifest.ErrFlux, nil, "invalid kustomize receipt")
	}
	var sources []manifest.SourceEvidence
	if records != nil {
		sources = acceptance.Sources
	}
	sources = collectRenderSources(&frame, acceptance.Source, sources)
	if !c.owner.CanPersistRenders() {
		//nolint:wrapcheck // Cancellation must preserve the caller-visible sentinel identity.
		return ctx.Err()
	}
	if frame.header.Binding == "" {
		frame.header.Binding = manifest.Fingerprint(frame.binding)
	}
	if frame.stored == nil {
		var err error
		frame.stored, err = sourcecache.NewFrame(frameDomain, frame.header, frame.output)
		if err != nil || frame.stored == nil {
			return err
		}
	}
	publication, err := frame.stored.PreparePublication(ctx, sourcecache.FramePublicationInput{
		Owner: owner, Content: finalContent, Records: records, Complete: records != nil,
	})
	if err != nil || publication == nil {
		return err
	}
	return sourcecache.RecordInventory(
		ctx,
		c.owner,
		sourcecache.AcceptedInventory[*sourcecache.FramePublication]{
			Key:        frame.header.Key,
			Lookup:     frame.lookup,
			Acceptance: publication.Identity(),
			Work:       acceptance.Work.History(),
			History:    sourcecache.RenderHistory{Owner: owner, Sources: sources},
			Content:    publication,
		},
	)
}

func collectRenderSources(
	f *renderFrame,
	source manifest.SourceIdentity,
	sources []manifest.SourceEvidence,
) []manifest.SourceEvidence {
	if sources == nil {
		return nil
	}
	sources = slices.Clone(sources)
	for _, file := range f.header.Reads.Files {
		sources = append(sources, manifest.SourceEvidence{Source: source, Path: file.Path})
	}
	for _, node := range f.header.Reads.Nodes {
		if node.Kind == kindFile {
			sources = append(sources, manifest.SourceEvidence{Source: source, Path: node.Path})
		}
	}
	return sources
}
