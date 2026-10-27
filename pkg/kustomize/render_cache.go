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
	"maps"
	"slices"

	"oras.land/oras-go/v2/errdef"

	"github.com/home-operations/flate/pkg/manifest"
	sourcecache "github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/values"
)

const frameDomain = "flateks2"

const acceptedRenderAnnotation = "flate.accepted-render"

type renderHeader struct {
	Key, Binding, Output string
	Size                 int
	Reads                readSetSnapshot
}

const frameHeaderLimit = 16 << 20

type renderBinding struct {
	Render                                       string
	Source                                       manifest.NamedResource
	Kind, URL, Path, Root, Key, Revision, Digest string
	Alias                                        bool
	Substitute                                   map[string]string `json:",omitempty"`
	Wipe, Strict                                 bool
	Skip                                         []string `json:",omitempty"`
	Dependencies                                 values.DependencySubstitutions
}

func (a renderBinding) equal(b renderBinding) bool {
	return a.Render == b.Render && a.Source == b.Source && a.Kind == b.Kind && a.URL == b.URL && a.Path == b.Path && a.Root == b.Root && a.Key == b.Key && a.Revision == b.Revision && a.Digest == b.Digest && a.Alias == b.Alias && maps.Equal(a.Substitute, b.Substitute) && a.Wipe == b.Wipe && a.Strict == b.Strict && slices.Equal(a.Skip, b.Skip) && a.Dependencies.Equal(b.Dependencies)
}

type renderFrame struct {
	lookup, raw           string
	header                renderHeader
	binding               renderBinding
	output, encoded       []byte
	accepted              *acceptedRender
	acceptedKey, identity string
}

func equalRenderFrame(a, b any) bool {
	left, ok := a.(renderFrame)
	right, other := b.(renderFrame)
	if !ok || !other || left.header.Key != right.header.Key || !left.binding.equal(right.binding) || !bytes.Equal(left.output, right.output) {
		return false
	}
	x, y := left.header.Reads, right.header.Reads
	return slices.Equal(x.Files, y.Files) && slices.Equal(x.Dirs, y.Dirs) && slices.Equal(x.Nodes, y.Nodes) && slices.EqualFunc(x.Remotes, y.Remotes, func(a, b remoteEntry) bool {
		return a.Input == b.Input && a.Identity == b.Identity && slices.Equal(a.Parents, b.Parents)
	})
}

type acceptedRoot struct {
	Source manifest.NamedResource
	Paths  []string
}

type acceptedRecord struct {
	ID     manifest.NamedResource
	Record manifest.DiscoveryRecord
	Roots  []acceptedRoot
}

type acceptedRender struct {
	Version  int
	Complete bool
	Owner    manifest.NamedResource
	Raw      string
	Records  []acceptedRecord
}

func (a *acceptedRender) equal(b *acceptedRender) bool {
	if a == nil || b == nil {
		return a == b
	}
	if a.Version != b.Version || a.Complete != b.Complete || a.Owner != b.Owner || a.Raw != b.Raw || (a.Records == nil) != (b.Records == nil) || len(a.Records) != len(b.Records) {
		return false
	}
	for i, x := range a.Records {
		y := b.Records[i]
		p, q := x.Record, y.Record
		if x.ID != y.ID || p.Authored != q.Authored || p.Path != q.Path || p.File != q.File || p.Parent != q.Parent || p.Builder != q.Builder || (p.Inputs == nil) != (q.Inputs == nil) || !slices.Equal(p.Inputs, q.Inputs) || p.Pinned != q.Pinned || (p.Refs == nil) != (q.Refs == nil) || !slices.Equal(p.Refs, q.Refs) || (p.Productions == nil) != (q.Productions == nil) || !slices.Equal(p.Productions, q.Productions) || (x.Roots == nil) != (y.Roots == nil) || len(x.Roots) != len(y.Roots) {
			return false
		}
		for j, root := range x.Roots {
			other := y.Roots[j]
			if root.Source != other.Source || (root.Paths == nil) != (other.Paths == nil) || !slices.Equal(root.Paths, other.Paths) {
				return false
			}
		}
	}
	return true
}

func encodeFrame(header renderHeader, output []byte) ([]byte, error) {
	if output == nil {
		output = []byte{}
	}
	header.Size, header.Output = len(output), manifest.Fingerprint(output)
	data, err := json.Marshal(header)
	if err != nil {
		return nil, err
	}
	if len(data) > frameHeaderLimit {
		return nil, errors.New("render header exceeds frame limit")
	}
	frame := make([]byte, 12+len(data)+len(output))
	copy(frame, frameDomain)
	binary.BigEndian.PutUint32(frame[8:12], uint32(len(data))) //nolint:gosec // The header is bounded by frameHeaderLimit.
	copy(frame[12:], data)
	copy(frame[12+len(data):], output)
	return frame, nil
}

func decodeFrame(data []byte) (renderFrame, error) {
	if len(data) < 12 || string(data[:8]) != frameDomain {
		return renderFrame{}, errors.New("invalid render frame domain or version")
	}
	size := int(binary.BigEndian.Uint32(data[8:12]))
	if size > frameHeaderLimit || size > len(data)-12 {
		return renderFrame{}, errors.New("invalid render header length")
	}
	var header renderHeader
	if err := json.Unmarshal(data[12:12+size], &header); err != nil {
		return renderFrame{}, err
	}
	begin := 12 + size
	if header.Size < 0 || header.Size > len(data)-begin {
		return renderFrame{}, errors.New("invalid render payload length")
	}
	end := begin + header.Size
	output := data[begin:end]
	if header.Output == "" || manifest.Fingerprint(output) != header.Output {
		return renderFrame{}, errors.New("invalid render payload identity")
	}
	frame := renderFrame{header: header, output: bytes.Clone(output), raw: manifest.Fingerprint(data[12:begin])}
	tail := data[end:]
	if len(tail) == 0 || len(tail) > frameHeaderLimit {
		return frame, nil
	}
	var accepted acceptedRender
	if err := json.Unmarshal(tail, &accepted); err != nil || accepted.Version != 1 || !accepted.Complete || accepted.Raw != frame.raw {
		return frame, nil
	}
	for i, item := range accepted.Records {
		if i > 0 && accepted.Records[i-1].ID.Compare(item.ID) >= 0 {
			return frame, nil
		}
		for j, root := range item.Roots {
			if j > 0 && item.Roots[j-1].Source.Compare(root.Source) >= 0 {
				return frame, nil
			}
		}
	}
	canonical, err := json.Marshal(accepted)
	if err != nil {
		return frame, nil
	}
	frame.accepted, frame.acceptedKey = &accepted, manifest.Fingerprint(canonical)
	return frame, nil
}

func prepareAccepted(owner manifest.NamedResource, frame renderFrame, records iter.Seq2[manifest.NamedResource, manifest.DiscoveryRecord]) (acceptedRender, error) {
	accepted := acceptedRender{Version: 1, Complete: true, Owner: owner, Raw: frame.raw}
	for id, record := range records {
		item := acceptedRecord{ID: id, Record: record}
		item.Record.ContentRoots = nil
		item.Record.Inputs = slices.Clone(record.Inputs)
		item.Record.Refs = slices.Clone(record.Refs)
		slices.SortFunc(item.Record.Refs, manifest.NamedResource.Compare)
		item.Record.Refs = slices.Compact(item.Record.Refs)
		item.Record.Productions = slices.Clone(record.Productions)
		slices.SortFunc(item.Record.Productions, func(a, b manifest.Production) int {
			return cmp.Or(a.Owner.Compare(b.Owner), a.Declaration.Compare(b.Declaration), a.Authored.Compare(b.Authored), a.Source.Compare(b.Source), cmp.Compare(a.Path, b.Path), cmp.Compare(a.File, b.File))
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

func (f renderFrame) history(owner manifest.NamedResource) (iter.Seq2[manifest.NamedResource, manifest.DiscoveryRecord], bool) {
	if f.accepted == nil || f.accepted.Owner != owner {
		return nil, false
	}
	return func(yield func(manifest.NamedResource, manifest.DiscoveryRecord) bool) {
		for _, item := range f.accepted.Records {
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
	}, true
}

// PreviousRender returns complete accepted history without rendering or acquiring remote inputs.
func (c *TreeCache) PreviousRender(ctx context.Context, req RenderRequest, owner manifest.NamedResource) (iter.Seq2[manifest.NamedResource, manifest.DiscoveryRecord], bool) {
	if c == nil || ctx.Err() != nil || !c.owner.CanPersistRenders() {
		return nil, false
	}
	prepared, err := c.prepareRender(ctx, req)
	if err != nil {
		return nil, false
	}
	reader, _, _, err := c.owner.Blob(ctx, sourcecache.BlobKey{Content: prepared.key, Render: sourcecache.RenderAccepted}, sourcecache.Policy{MaxAge: sourcecache.Forever}, nil)
	if err != nil {
		return nil, false
	}
	data, err := fs.ReadFile(reader, "content")
	if err != nil {
		return nil, false
	}
	frame, err := decodeFrame(data)
	if err != nil || frame.header.Key != prepared.key || frame.header.Binding != manifest.Fingerprint(prepared.binding) || len(frame.header.Reads.Remotes) != 0 {
		return nil, false
	}
	valid, err := frame.header.Reads.localsValid(ctx, prepared.lower, req.Source.Content != "")
	if err != nil || !valid {
		return nil, false
	}
	if _, err := manifest.DecodeRender(frame.output); err != nil {
		return nil, false
	}
	if ctx.Err() != nil {
		return nil, false
	}
	return frame.history(owner)
}

// RecordRender persists a receipt only with a complete accepted inventory.
func (c *TreeCache) RecordRender(ctx context.Context, owner manifest.NamedResource, receipt *manifest.RenderReceipt, records iter.Seq2[manifest.NamedResource, manifest.DiscoveryRecord]) error {
	if receipt == nil || records == nil {
		return nil
	}
	frame, ok := receipt.Value().(renderFrame)
	if !ok || frame.header.Key == "" {
		return fmt.Errorf("%w: invalid kustomize receipt", manifest.ErrFlux)
	}
	descriptor, identity, enabled, err := c.owner.RenderMetadata(ctx, frame.header.Key, frame.lookup)
	if err != nil && !errors.Is(err, errdef.ErrNotFound) {
		return fmt.Errorf("lookup kustomize render cache: %w", err)
	}
	if !enabled {
		return nil
	}
	if frame.header.Binding == "" {
		frame.header.Binding = manifest.Fingerprint(frame.binding)
	}
	if frame.encoded == nil && frame.raw == "" {
		frame.encoded, err = encodeFrame(frame.header, frame.output)
		if err != nil {
			return fmt.Errorf("%w: encode render frame: %w", manifest.ErrFlux, err)
		}
	}
	if frame.raw == "" {
		frame.raw = manifest.Fingerprint(frame.encoded[12 : 12+int(binary.BigEndian.Uint32(frame.encoded[8:12]))])
	}
	accepted, err := prepareAccepted(owner, frame, records)
	if err != nil {
		return fmt.Errorf("%w: encode accepted kustomize render: %w", manifest.ErrFlux, err)
	}
	if frame.accepted != nil && frame.identity != "" && identity == frame.identity && frame.acceptedKey != "" && descriptor.Annotations[acceptedRenderAnnotation] == frame.acceptedKey && frame.accepted.equal(&accepted) {
		return nil
	}
	data, err := json.Marshal(accepted)
	if err != nil {
		return fmt.Errorf("%w: encode accepted kustomize render: %w", manifest.ErrFlux, err)
	}
	if len(data) > frameHeaderLimit {
		return fmt.Errorf("%w: accepted kustomize render exceeds frame limit", manifest.ErrFlux)
	}
	content := manifest.Fingerprint(data)
	if descriptor.Annotations[acceptedRenderAnnotation] == content {
		return nil
	}
	if frame.encoded == nil {
		frame.encoded, err = encodeFrame(frame.header, frame.output)
		if err != nil {
			return fmt.Errorf("%w: encode render frame: %w", manifest.ErrFlux, err)
		}
	}
	key := sourcecache.BlobKey{Content: frame.header.Key, Render: sourcecache.RenderAccepted}
	raw := frame.encoded
	result := sourcecache.ProducerResult{Annotations: map[string]string{acceptedRenderAnnotation: content}}
	_, _, _, err = c.owner.Blob(ctx, key, sourcecache.Policy{MaxAge: 0, Retry: sourcecache.RetryConfig{Attempts: 1}}, func(_ context.Context, w sourcecache.ProducerInput) (sourcecache.ProducerResult, error) {
		_, err := io.Copy(w, io.MultiReader(bytes.NewReader(raw), bytes.NewReader(data)))
		return result, err
	})
	if err != nil {
		return fmt.Errorf("kustomize render cache publish: %w", err)
	}
	return nil
}
