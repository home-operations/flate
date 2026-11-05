package base

import (
	"context"
	"fmt"
	"slices"

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

func (c *Controller) admitChildren(
	ctx context.Context,
	id manifest.NamedResource,
	next *store.Emission,
) ([]store.Child, bool, error) {
	children := next.Children
	cloned := false
	if !next.Reads.Valid() {
		return children, cloned, nil
	}
	childValues2 := next.Children
	for i := range childValues2 {
		child := childValues2[i]
		if child.Obj == nil || child.Obj.Named() == id {
			continue
		}
		obj := manifest.MapDependsOn(child.Obj, c.DependencySubstitutions.Resolve, nil)
		if obj != child.Obj {
			if !cloned {
				next.Children = slices.Clone(children)
				cloned = true
			}
			next.Children[i].Obj = obj
		}
		if _, err := c.KeepEmitted(ctx, id, next.Children[i], next.Reads); err != nil {
			return nil, false, err
		}
	}
	return children, cloned, nil
}

func (c *Controller) emit(
	ctx context.Context,
	id manifest.NamedResource,
	next *store.Emission,
	payloads map[manifest.NamedResource]*store.SourceArtifact,
) error {
	children, cloned, err := c.admitChildren(ctx, id, next)
	if err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("context: %w", err)
	}
	c.Store.RecordEmitted(id, next, payloads)
	if cloned && !next.Complete {
		next.Children = children
	}
	if next.Complete {
		if info, ok := c.Store.GetStatus(id); ok && info.Status == store.StatusPending {
			message := store.ApplyingStatus(len(next.Manifests))
			if info != message {
				c.Store.UpdateStatus(id, message)
			}
		}
	}
	return nil
}
