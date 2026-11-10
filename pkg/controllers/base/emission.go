package base

import (
	"context"
	"fmt"

	"github.com/home-operations/flate/pkg/selection"

	"github.com/home-operations/flate/pkg/loader"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

// ParseChildren selects output and retains authoritative sanitized child documents.
// The returned objects and maps MUST remain immutable after publication.
func (e *Execution) ParseChildren(
	parent manifest.BaseManifest,
	docs []manifest.DocOccurrence,
	opts manifest.ParseDocOptions,
) store.ChildDocuments {
	parseChildrenResult1 := loader.ParseChildren(parent, docs, opts, e.SkipKinds)
	children, visible, failures := parseChildrenResult1.Children, parseChildrenResult1.Visible, parseChildrenResult1.Failures
	for _, doc := range docs {
		for warningIndex := range doc.Warnings {
			warning := doc.Warnings[warningIndex]
			e.Reads.AddWarning(warning)
		}
	}
	if e.Store != nil {
		childValues := children
		for i := range childValues {
			child := childValues[i]
			if existing := e.Store.GetObject(
				child.Obj.Named(),
			); existing != nil &&
				manifest.Equal(existing, child.Obj) {
				children[i].Obj = existing
			}
		}
	}
	return store.ChildDocuments{Children: children, Visible: visible, Failures: failures}
}

func (e *Execution) emit(
	ctx context.Context,
	id manifest.NamedResource,
	next *store.Emission,
	payloads map[manifest.NamedResource]*store.SourceArtifact,
) error {
	if len(next.Children) != 0 && next.Reads.Valid() {
		if _, err := e.KeepEmitted(ctx, id, selection.PrepareRequest{
			Generation: next.Children, Reads: next.Reads, Preparation: &e.AdmissionPreparation,
		}); err != nil {
			return err
		}
	}
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("context: %w", err)
	}
	e.Store.RecordEmitted(id, next, payloads)
	if next.Complete {
		if info, ok := e.Store.GetStatus(id); ok && info.Status == store.StatusPending {
			message := store.ApplyingStatus(len(next.Manifests))
			if info != message {
				e.Store.UpdateStatus(id, message)
			}
		}
	}
	return nil
}
