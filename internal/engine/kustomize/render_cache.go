package kustomize

import (
	"context"
	"fmt"
	"io/fs"
	"iter"
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

func equalRenderPolicy(a, b renderPolicy) bool {
	return a.Strict == b.Strict && slices.Equal(a.Skip, b.Skip)
}

type renderFrame = sourcecache.RenderReceipt[renderPolicy, renderHeader]

//nolint:gochecknoglobals // Receipts share this immutable domain definition.
var renderDomain = sourcecache.NewRenderDomain(sourcecache.RenderDomain[renderPolicy, renderHeader]{
	Name: frameDomain, EqualPolicy: equalRenderPolicy, EqualHeader: equalRenderHeader,
	ValidHeader: func(h renderHeader) bool {
		return h.Reads.Files != nil || h.Reads.Dirs != nil || h.Reads.Nodes != nil || h.Reads.Remotes != nil
	},
	Accept: func(h renderHeader, acceptance *sourcecache.RenderAcceptance) (any, []manifest.SourceEvidence) {
		content, ok := acceptance.Content.(renderContent)
		if !ok {
			return nil, nil
		}
		return content, collectRenderSources(&h.Reads, acceptance.Source, acceptance.Sources)
	},
})

type decodedRender struct {
	identity string
	frame    renderFrame
}

//nolint:gocritic // The decoded flag distinguishes a valid empty frame from a miss.
func (p *Preparation) readFrame(ctx context.Context, generation fs.FS) (renderFrame, bool, error) {
	identity := sourcecache.AcquisitionIdentity(generation)
	if p != nil && p.decoded != nil && identity != "" && p.decoded.identity == identity {
		return p.decoded.frame, true, nil
	}
	frame, decoded, err := renderDomain.Read(ctx, generation)
	if err != nil {
		return renderFrame{}, false, fmt.Errorf("read kustomize render cache: %w", err)
	}
	if !decoded {
		return renderFrame{}, false, nil
	}
	if p != nil && identity != "" {
		p.decoded = &decodedRender{identity: identity, frame: frame}
	}
	return frame, true, nil
}

//nolint:gocritic // Read evidence is an immutable stack snapshot.
func equalRenderHeader(left, right renderHeader) bool {
	x, y := &left.Reads, &right.Reads
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
			Lookups: prepared.addresses,
		},
		&sourcecache.Policy{MaxAge: sourcecache.Forever, Work: req.Work.History()},
		nil,
	)
	if err != nil {
		return nil, nil, false
	}
	frame, decoded, err := req.Preparation.readFrame(ctx, reader)
	if err != nil || !decoded {
		return nil, nil, false
	}
	read := func(header renderHeader) (bool, error) {
		if len(header.Reads.Remotes) != 0 {
			return false, nil
		}
		return header.Reads.localsValid(ctx, prepared.lower, req.Source.Content != "")
	}
	var binding *sourcecache.RenderBinding[renderPolicy]
	if req.Preparation != nil {
		binding = &req.Preparation.binding
	} else {
		// Standalone history owns only a binding snapshot, with no preparation to borrow.
		binding = new(prepared.binding)
	}
	validated, err := renderDomain.Validate(ctx, &frame, binding, sourcecache.RenderCheck[renderHeader]{
		History: true,
		Header:  read,
	})
	if err != nil || !validated.Valid {
		return nil, nil, false
	}
	frame.Lookup = lookup
	return validated.Documents, renderDomain.Receipt(frame), true
}

// RenderHistory returns complete accepted inventory matching the current final inputs.
func (*TreeCache) RenderHistory(
	owner manifest.NamedResource,
	receipt *manifest.RenderReceipt,
	content any,
) (iter.Seq2[manifest.NamedResource, manifest.DiscoveryRecord], bool) {
	if receipt == nil {
		return nil, false
	}
	frame, ok := receipt.Value().(renderFrame)
	if !ok {
		return nil, false
	}
	return frame.History(owner, content)
}

func collectRenderSources(
	reads *readSetSnapshot,
	source manifest.SourceIdentity,
	sources []manifest.SourceEvidence,
) []manifest.SourceEvidence {
	if sources == nil {
		return nil
	}
	sources = slices.Clone(sources)
	for _, file := range reads.Files {
		sources = append(sources, manifest.SourceEvidence{Source: source, Path: file.Path})
	}
	for _, node := range reads.Nodes {
		if node.Kind == kindFile {
			sources = append(sources, manifest.SourceEvidence{Source: source, Path: node.Path})
		}
	}
	return sources
}
