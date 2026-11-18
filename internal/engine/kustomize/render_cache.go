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

	sourcecache "github.com/home-operations/flate/internal/engine/source/cache"
	"github.com/home-operations/flate/pkg/manifest"
)

const frameDomain = "kustomize-render"

type renderHeader struct {
	Reads readSetSnapshot `json:"Reads"`
}

type renderPolicy struct {
	Strict bool     `json:"Strict"`
	Skip   []string `json:",omitempty"`
}

func equalRenderPolicy(a, b *renderPolicy) bool {
	return a.Strict == b.Strict && slices.Equal(a.Skip, b.Skip)
}

type renderFrame struct {
	sourcecache.RenderFrame

	header  renderHeader
	binding sourcecache.RenderBinding[renderPolicy]
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
	var err error
	frame.RenderFrame, err = sourcecache.OpenRenderFrame(ctx, frameDomain, generation, &frame.header)
	if err != nil {
		if _, ok := errors.AsType[*sourcecache.Error](err); ok {
			return renderFrame{}, false, manifest.NewError(
				manifest.ErrFlux, fmt.Errorf("read kustomize render cache: %w", err), "",
			)
		}
		slog.Debug("kustomize: decode render frame", "err", err)
		return renderFrame{}, false, nil
	}
	if p != nil && identity != "" {
		p.decoded = &decodedRender{identity: identity, frame: frame}
	}
	return frame, true, nil
}

func equalRenderFrame(a, b any) bool {
	left, ok := a.(renderFrame)
	right, other := b.(renderFrame)
	if !ok || !other || left.Key != right.Key {
		return false
	}
	if !left.binding.Equal(&right.binding, equalRenderPolicy) || !bytes.Equal(left.Output, right.Output) {
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
	validBinding := err == nil && decoded && frame.Key == prepared.key &&
		frame.Binding == prepared.binding.Fingerprint()
	if !validBinding || len(frame.header.Reads.Remotes) != 0 {
		return nil, nil, false
	}
	valid, err := frame.header.Reads.localsValid(ctx, prepared.lower, req.Source.Content != "")
	if err != nil || !valid {
		return nil, nil, false
	}
	docs, err := manifest.DecodeRender(frame.Output)
	if err != nil {
		return nil, nil, false
	}
	if ctx.Err() != nil {
		return nil, nil, false
	}
	frame.binding, frame.Lookup = prepared.binding, lookup
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
	if !ok || frame.Stored == nil {
		return nil, false
	}
	return frame.Stored.History(owner, content)
}

//nolint:gocritic // Receipts retain concrete frame values, so this interface requires a value receiver.
func (f renderFrame) RenderPublication(
	_ context.Context,
	acceptance *sourcecache.RenderAcceptance,
) (sourcecache.RenderPublication, error) {
	if f.Key == "" {
		return sourcecache.RenderPublication{}, manifest.NewError(manifest.ErrFlux, nil, "invalid kustomize receipt")
	}
	if acceptance == nil {
		return sourcecache.RenderPublication{}, nil
	}
	content, ok := acceptance.Content.(renderContent)
	if !ok {
		return sourcecache.RenderPublication{}, nil
	}
	if f.Binding == "" {
		binding := f.binding
		f.Rebind(binding.Fingerprint())
	}
	var metadata any
	if f.Stored == nil {
		metadata = f.header
	}
	return sourcecache.RenderPublication{
		Frame: f.RenderFrame, Domain: frameDomain, Metadata: metadata,
		Content: content, Sources: collectRenderSources(&f, acceptance.Source, acceptance.Sources),
	}, nil
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
