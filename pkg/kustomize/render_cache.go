package kustomize

import (
	"bytes"
	"cmp"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"iter"
	"slices"

	"github.com/home-operations/flate/pkg/manifest"
	sourcecache "github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/store"
	"github.com/home-operations/flate/pkg/values"
)

const frameDomain = "flateks5"

type renderHeader struct {
	EntrySpec *manifest.Kustomization `json:",omitempty"`
	Key       string                  `json:"Key"`
	Binding   string                  `json:"Binding"`
	Output    string                  `json:"Output"`
	Size      int                     `json:"Size"`
	Reads     readSetSnapshot         `json:"Reads"`
	Entry     *entryApplication       `json:",omitempty"`
}

type entryApplication struct {
	Application map[string]any  `json:"Application"`
	Reads       readSetSnapshot `json:"Reads"`
	Inventory   acceptedRender  `json:"Inventory"`
	Inputs      []EntryInput    `json:"Inputs"`
	Complete    bool            `json:"Complete"`
}

const frameHeaderLimit = 16 << 20

const acceptedRenderVersion = 2

type renderBinding struct {
	Render       string                         `json:"Render"`
	Source       manifest.NamedResource         `json:"Source"`
	Kind         string                         `json:"Kind"`
	URL          string                         `json:"URL"`
	Path         string                         `json:"Path"`
	Root         string                         `json:"Root"`
	Key          string                         `json:"Key"`
	Revision     string                         `json:"Revision"`
	Digest       string                         `json:"Digest"`
	Alias        bool                           `json:"Alias"`
	Strict       bool                           `json:"Strict"`
	Skip         []string                       `json:",omitempty"`
	Dependencies values.DependencySubstitutions `json:"Dependencies"`
}

func (a *renderBinding) equal(b *renderBinding) bool {
	return a.Render == b.Render && a.Source == b.Source && a.Kind == b.Kind && a.URL == b.URL && a.Path == b.Path &&
		a.Root == b.Root &&
		a.Key == b.Key &&
		a.Revision == b.Revision &&
		a.Digest == b.Digest &&
		a.Alias == b.Alias &&
		a.Strict == b.Strict &&
		slices.Equal(a.Skip, b.Skip) &&
		a.Dependencies.Equal(b.Dependencies)
}

type renderFrame struct {
	lookup, raw     string
	inventory       string
	header          renderHeader
	binding         renderBinding
	output, encoded []byte
	accepted        *acceptedRender
	acceptedKey     string
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

type acceptedRoot struct {
	Source manifest.NamedResource `json:"Source"`
	Paths  []string               `json:"Paths"`
}

type acceptedRecord struct {
	ID     manifest.NamedResource   `json:"ID"`
	Record manifest.DiscoveryRecord `json:"Record"`
	Roots  []acceptedRoot           `json:"Roots"`
}

type acceptedRender struct {
	Version  int                    `json:"Version"`
	Complete bool                   `json:"Complete"`
	Owner    manifest.NamedResource `json:"Owner"`
	Raw      string                 `json:"Raw"`
	Final    string                 `json:"Final"`
	Records  []acceptedRecord       `json:"Records"`
}

func (a *acceptedRender) equal(b *acceptedRender) bool {
	if a == nil || b == nil {
		return a == b
	}
	if a.Version != b.Version || a.Complete != b.Complete || a.Owner != b.Owner || a.Raw != b.Raw ||
		a.Final != b.Final {
		return false
	}
	if (a.Records == nil) != (b.Records == nil) || len(a.Records) != len(b.Records) {
		return false
	}
	{
		xValues := a.Records
		for i := range xValues {
			x := xValues[i]
			y := b.Records[i]
			if x.ID != y.ID || !x.Record.Equal(y.Record) || (x.Roots == nil) != (y.Roots == nil) ||
				len(x.Roots) != len(y.Roots) {
				return false
			}
			for j, root := range x.Roots {
				other := y.Roots[j]
				if root.Source != other.Source || (root.Paths == nil) != (other.Paths == nil) ||
					!slices.Equal(root.Paths, other.Paths) {
					return false
				}
			}
		}
	}
	return true
}

func encodeFrame(input *renderHeader, output []byte) ([]byte, error) {
	header := *input

	if output == nil {
		output = []byte{}
	}
	header.Size, header.Output = len(output), manifest.Fingerprint(output)
	data, err := json.Marshal(header)
	if err != nil {
		return nil, fmt.Errorf("encode render frame: %w", err)
	}
	if len(data) > frameHeaderLimit {
		return nil, errors.New("render header exceeds frame limit")
	}
	frame := make([]byte, framePrefixSize+len(data)+len(output))
	copy(frame, frameDomain)
	binary.BigEndian.PutUint32(
		frame[8:framePrefixSize],
		uint32(len(data)), //nolint:gosec // frameHeaderLimit bounds the header length below uint32 capacity.
	)
	copy(frame[framePrefixSize:], data)
	copy(frame[framePrefixSize+len(data):], output)
	return frame, nil
}

func decodeFrame(data []byte) (renderFrame, error) {
	if len(data) < framePrefixSize || string(data[:8]) != frameDomain {
		return renderFrame{}, errors.New("invalid render frame domain or version")
	}
	size := int(binary.BigEndian.Uint32(data[8:framePrefixSize]))
	if size > frameHeaderLimit || size > len(data)-framePrefixSize {
		return renderFrame{}, errors.New("invalid render header length")
	}
	var header renderHeader
	if err := json.Unmarshal(data[framePrefixSize:framePrefixSize+size], &header); err != nil {
		return renderFrame{}, fmt.Errorf("decode render frame: %w", err)
	}
	if header.Entry != nil && (!header.Entry.Complete || header.Entry.Inputs == nil) {
		return renderFrame{}, errors.New("incomplete entry qualification evidence")
	}
	begin := framePrefixSize + size
	if header.Size < 0 || header.Size > len(data)-begin {
		return renderFrame{}, errors.New("invalid render payload length")
	}
	end := begin + header.Size
	output := data[begin:end]
	if header.Output == "" || manifest.Fingerprint(output) != header.Output {
		return renderFrame{}, errors.New("invalid render payload identity")
	}
	frame := renderFrame{
		header: header,
		output: bytes.Clone(output),
		raw:    manifest.Fingerprint(data[framePrefixSize:begin]),
	}
	tail := data[end:]
	if len(tail) == 0 || len(tail) > frameHeaderLimit {
		return frame, nil
	}
	var accepted acceptedRender
	if err := json.Unmarshal(tail, &accepted); err != nil {
		//nolint:nilerr // Invalid cache metadata must remain a miss so the renderer can rebuild.
		return frame, nil
	}
	if accepted.Version != acceptedRenderVersion || accepted.Final == "" || !accepted.Complete ||
		accepted.Raw != frame.raw {
		return frame, nil
	}
	if !accepted.ordered() {
		return frame, nil
	}

	canonical, err := json.Marshal(accepted)
	if err != nil {
		//nolint:nilerr // Invalid cache metadata must remain a miss so the renderer can rebuild.
		return frame, nil
	}
	frame.accepted, frame.acceptedKey = &accepted, manifest.Fingerprint(canonical)
	return frame, nil
}

func prepareAccepted(
	owner manifest.NamedResource,
	frame *renderFrame,
	fingerprint string,
	records iter.Seq2[manifest.NamedResource, manifest.DiscoveryRecord],
) (acceptedRender, error) {
	accepted := acceptedRender{
		Version:  acceptedRenderVersion,
		Complete: true,
		Owner:    owner,
		Raw:      frame.raw,
		Final:    fingerprint,
	}
	//nolint:gocritic // The iterator supplies immutable value snapshots.
	for id, record := range records {
		item := acceptedRecord{ID: id, Record: record}
		item.Record.ContentRoots = nil
		item.Record.Inputs = slices.Clone(record.Inputs)
		item.Record.Refs = slices.Clone(record.Refs)
		slices.SortFunc(item.Record.Refs, manifest.NamedResource.Compare)
		item.Record.Refs = slices.Compact(item.Record.Refs)
		item.Record.Productions = slices.Clone(record.Productions)
		slices.SortFunc(item.Record.Productions, func(a, b manifest.Production) int {
			return cmp.Or(
				a.Owner.Compare(b.Owner),
				a.Declaration.Compare(b.Declaration),
				a.Authored.Compare(b.Authored),
				a.Source.Compare(b.Source),
				cmp.Compare(a.Path, b.Path),
				cmp.Compare(a.File, b.File),
			)
		})
		item.Record.Productions = slices.Compact(item.Record.Productions)
		for source, paths := range record.ContentRoots {
			paths = slices.Clone(paths)
			slices.Sort(paths)
			item.Roots = append(item.Roots, acceptedRoot{Source: source, Paths: slices.Compact(paths)})
		}
		slices.SortFunc(item.Roots, func(a, b acceptedRoot) int { return a.Source.Compare(b.Source) })
		accepted.Records = append(accepted.Records, item)
	}
	slices.SortFunc(accepted.Records, func(a, b acceptedRecord) int { return a.ID.Compare(b.ID) })
	for i := 1; i < len(accepted.Records); i++ {
		if accepted.Records[i-1].ID == accepted.Records[i].ID {
			return acceptedRender{}, errors.New("duplicate accepted render identity")
		}
	}
	return accepted, nil
}

//nolint:gocritic // Keep immutable value-receiver semantics.
func (f renderFrame) history(
	owner manifest.NamedResource,
) (iter.Seq2[manifest.NamedResource, manifest.DiscoveryRecord], bool) {
	if f.accepted == nil || f.accepted.Owner != owner {
		return nil, false
	}
	return func(yield func(manifest.NamedResource, manifest.DiscoveryRecord) bool) {
		{
			itemValues2 := f.accepted.Records
			for itemIndex := range itemValues2 {
				item := itemValues2[itemIndex]
				record := item.Record
				record.Inputs = slices.Clone(record.Inputs)
				record.Refs = slices.Clone(record.Refs)
				record.Productions = slices.Clone(record.Productions)
				if len(item.Roots) > 0 {
					record.ContentRoots = make(map[manifest.NamedResource][]string, len(item.Roots))
				}
				for _, root := range item.Roots {
					record.ContentRoots[root.Source] = slices.Clone(root.Paths)
				}
				if !yield(item.ID, record) {
					return
				}
			}
		}
	}, true
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
			Content: prepared.inventory,
			Render:  sourcecache.RenderAccepted,
		},
		&sourcecache.Policy{MaxAge: sourcecache.Forever, Work: req.Work.History()},
		nil,
	)
	if err != nil {
		return nil, nil, false
	}
	data, err := fs.ReadFile(reader, "content")
	if err != nil {
		return nil, nil, false
	}
	frame, err := decodeFrame(data)
	validBinding := err == nil && frame.header.Key == prepared.key &&
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
	frame.binding, frame.lookup, frame.inventory = prepared.binding, lookup, prepared.inventory
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
	if !ok || frame.accepted == nil || frame.accepted.Final == "" ||
		frame.accepted.Final != manifest.Fingerprint(content) {
		return nil, false
	}
	return frame.history(owner)
}

type renderPublication struct {
	frame    renderFrame
	accepted acceptedRender
	data     []byte
}

// WriteTo writes the render frame and accepted inventory.
//
//nolint:gocritic // A publication is a value snapshot so encoding never mutates its accepted frame.
func (p renderPublication) WriteTo(w io.Writer) (int64, error) {
	if p.frame.encoded == nil {
		encoded, err := encodeFrame(&p.frame.header, p.frame.output)
		if err != nil {
			return 0, manifest.NewError(manifest.ErrFlux, fmt.Errorf("encode render frame: %w", err), "")
		}
		p.frame.encoded = encoded
	}
	if !p.accepted.Complete {
		//nolint:wrapcheck // Cache publication preserves the writer's original error and byte count.
		return io.Copy(w, bytes.NewReader(p.frame.encoded))
	}
	data := p.data
	if data == nil {
		encoded, err := json.Marshal(p.accepted)
		if err != nil {
			return 0, manifest.NewError(manifest.ErrFlux, fmt.Errorf("encode accepted kustomize render: %w", err), "")
		}
		data = encoded
	}
	if len(data) > frameHeaderLimit {
		return 0, manifest.NewError(manifest.ErrFlux, nil, "accepted kustomize render exceeds frame limit")
	}
	//nolint:wrapcheck // Cache publication preserves the writer's original error and byte count.
	return io.Copy(w, io.MultiReader(bytes.NewReader(p.frame.encoded), bytes.NewReader(data)))
}

// RenderAcceptance binds accepted output and optional entry inventory to its final owner.
type RenderAcceptance struct {
	Work *manifest.RenderWork `json:"-"`
	// Source identifies the admitted filesystem used by the receipt.
	Source  manifest.SourceIdentity   `json:"Source"`
	Sources []manifest.SourceEvidence `json:"Sources"`
	Owner   manifest.NamedResource    `json:"Owner"`
	Receipt *manifest.RenderReceipt   `json:"Receipt"`
	Entry   *manifest.RenderReceipt   `json:"Entry"`
	// Records is supplied for opaque output or the entry applier receipt.
	Records                 iter.Seq2[manifest.NamedResource, manifest.DiscoveryRecord] `json:"Records"`
	DependencySubstitutions values.DependencySubstitutions                              `json:"DependencySubstitutions"`
	Content                 any                                                         `json:"Content"`
	Reads                   *store.ReadSet                                              `json:"Reads"`
	OutputReads             *store.ReadSet                                              `json:"OutputReads"`
}

// RecordRender persists accepted output and optional entry applier evidence.
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
		finalContent.Dependencies = acceptance.DependencySubstitutions
	}
	frame, ok := receipt.Value().(renderFrame)
	if !ok || frame.header.Key == "" {
		return manifest.NewError(manifest.ErrFlux, nil, "invalid kustomize receipt")
	}
	if acceptance.Entry != nil {
		if application, ok := acceptance.Entry.Value().(entryApplication); ok {
			frame.header.Entry = &application
			frame.encoded, frame.accepted, frame.raw, frame.acceptedKey = nil, nil, "", ""
		}
	}
	if !frame.binding.Dependencies.Equal(acceptance.DependencySubstitutions) {
		frame.binding.Dependencies = acceptance.DependencySubstitutions
		frame.header.Binding, frame.lookup, frame.raw = "", "", ""
		frame.encoded, frame.accepted, frame.acceptedKey = nil, nil, ""
	}
	if records == nil && frame.accepted != nil {
		//nolint:wrapcheck // Cancellation must preserve the caller-visible sentinel identity.
		return ctx.Err()
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
	publication, content, err := prepareRenderPublication(&frame, owner, finalContent, records)
	if err != nil {
		return err
	}
	return sourcecache.RecordInventory(
		ctx,
		c.owner,
		sourcecache.AcceptedInventory[renderPublication]{
			Key:        cmp.Or(frame.inventory, frame.header.Key),
			Lookup:     frame.lookup,
			Acceptance: content,
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

func prepareRenderPublication(
	f *renderFrame,
	owner manifest.NamedResource,
	finalContent renderContent,
	records iter.Seq2[manifest.NamedResource, manifest.DiscoveryRecord],
) (renderPublication, string, error) {
	var err error
	if f.header.Binding == "" {
		f.header.Binding = manifest.Fingerprint(f.binding)
	}
	if f.encoded == nil && f.raw == "" {
		f.encoded, err = encodeFrame(&f.header, f.output)
		if err != nil {
			return renderPublication{}, "", manifest.NewError(
				manifest.ErrFlux,
				fmt.Errorf("encode render frame: %w", err),
				"",
			)
		}
	}
	if f.raw == "" {
		f.raw = manifest.Fingerprint(
			f.encoded[12 : framePrefixSize+int(binary.BigEndian.Uint32(f.encoded[8:framePrefixSize]))],
		)
	}
	var accepted acceptedRender
	var data []byte
	content := f.raw
	if records != nil {
		accepted, err = prepareAccepted(owner, f, manifest.Fingerprint(finalContent), records)
		if err != nil {
			return renderPublication{}, "", manifest.NewError(
				manifest.ErrFlux,
				fmt.Errorf("encode accepted kustomize render: %w", err),
				"",
			)
		}
		content = f.acceptedKey
		if f.accepted == nil || content == "" || !f.accepted.equal(&accepted) {
			data, err = json.Marshal(accepted)
			if err != nil {
				return renderPublication{}, "", manifest.NewError(
					manifest.ErrFlux,
					fmt.Errorf("encode accepted kustomize render: %w", err),
					"",
				)
			}
			content = manifest.Fingerprint(data)
		}
	}
	return renderPublication{frame: *f, accepted: accepted, data: data}, content, nil
}

const framePrefixSize = 12

func (a *acceptedRender) ordered() bool {
	itemValues := a.Records
	for i := range itemValues {
		item := itemValues[i]
		if i > 0 && a.Records[i-1].ID.Compare(item.ID) >= 0 {
			return false
		}
		for j, root := range item.Roots {
			if j > 0 && item.Roots[j-1].Source.Compare(root.Source) >= 0 {
				return false
			}
		}
	}
	return true
}
