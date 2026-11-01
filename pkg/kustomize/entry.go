package kustomize

import (
	"context"
	"io/fs"
	"iter"
	"log/slog"
	"path/filepath"
	"slices"

	"github.com/home-operations/flate/pkg/manifest"
	sourcecache "github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/store"
)

// EntryEvidence retains the accepted producer inventory that qualified an entry application.
type EntryEvidence struct {
	Application map[string]any
	Owner       manifest.NamedResource
	Records     iter.Seq2[manifest.NamedResource, manifest.DiscoveryRecord]
	Receipt     *manifest.RenderReceipt
}

// ApplicationReceipt captures the local inputs that qualified a rendered entry applier.
func (*TreeCache) ApplicationReceipt(ctx context.Context, source *store.SourceArtifact, evidence EntryEvidence, files []string) (*manifest.RenderReceipt, error) {
	reads := newReadSet(source.LocalPath)
	for _, file := range slices.Sorted(slices.Values(files)) {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if !fs.ValidPath(file) {
			return nil, nil
		}
		data, err := fs.ReadFile(source.FS, file)
		if err != nil {
			return nil, nil
		}
		reads.recordFile(filepath.Join(source.LocalPath, filepath.FromSlash(file)), data, nil)
	}
	inventory, err := prepareAccepted(evidence.Owner, renderFrame{}, evidence.Records)
	if err != nil {
		return nil, err
	}
	slog.Debug("kustomize: entry application captured", "producer", evidence.Owner, "files", len(files))
	return manifest.NewRenderReceipt(entryApplication{Application: evidence.Application, Reads: reads.snapshot(), Inventory: inventory}, equalEntryApplication), nil
}

func equalEntryApplication(a, b any) bool {
	left, ok := a.(entryApplication)
	right, other := b.(entryApplication)
	return ok && other && manifest.ValueEqual(left.Application, right.Application) && slices.Equal(left.Reads.Files, right.Reads.Files) && left.Inventory.equal(&right.Inventory)
}

// PreviousEntry returns qualified application evidence independently of changed entry output.
func (c *TreeCache) PreviousEntry(ctx context.Context, req RenderRequest) EntryEvidence {
	if c == nil || ctx.Err() != nil || !c.owner.CanPersistRenders() {
		return EntryEvidence{}
	}
	prepared, err := c.prepareRender(ctx, req)
	if err != nil {
		return EntryEvidence{}
	}
	reader, _, _, err := c.owner.Blob(ctx, sourcecache.BlobKey{Content: prepared.key, Render: sourcecache.RenderAccepted}, sourcecache.Policy{MaxAge: sourcecache.Forever}, nil)
	if err != nil {
		return EntryEvidence{}
	}
	data, err := fs.ReadFile(reader, "content")
	if err != nil {
		return EntryEvidence{}
	}
	frame, err := decodeFrame(data)
	if err != nil || frame.accepted == nil || frame.accepted.Owner != req.Spec.Named() || frame.header.Key != prepared.key || frame.header.Binding != manifest.Fingerprint(prepared.binding) || frame.header.Entry == nil {
		return EntryEvidence{}
	}
	application := *frame.header.Entry
	valid, err := application.Reads.localsValid(ctx, req.Source.FS, false)
	if err != nil || !valid {
		return EntryEvidence{}
	}
	records, _ := (renderFrame{accepted: &application.Inventory}).history(application.Inventory.Owner)
	slog.Debug("kustomize: entry application reused", "producer", application.Inventory.Owner)
	return EntryEvidence{Application: application.Application, Owner: application.Inventory.Owner, Records: records, Receipt: manifest.NewRenderReceipt(application, equalEntryApplication)}
}
