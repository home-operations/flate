package loader

import (
	"cmp"
	"context"
	"log/slog"

	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

// PromoteOrphans MUST run after index finalization.
func (l *Loader) PromoteOrphans(ctx context.Context) error {
	var reads store.ReadSet
	reads.Init(l.Store, manifest.NamedResource{}, nil)
	defer reads.Retire()
	query := rootQuery{loader: l, objects: &reads}
	//nolint:gocritic // The iterator supplies independent immutable record snapshots.
	for id, record := range l.Index.Files() {
		promoted, err := query.promote(ctx, id, &record, &reads)
		if err != nil {
			return err
		}
		if !promoted {
			slog.Debug("loader: orphan promotion failed", "id", id.String())
		}
	}
	return nil
}

func (q *rootQuery) promote(
	ctx context.Context,
	id manifest.NamedResource,
	record *manifest.DiscoveryRecord,
	reads *store.ReadSet,
) (bool, error) {
	l := q.loader
	if object, policy := reads.Lookup(id); object != nil && policy.Root {
		return true, nil
	}
	if record.Declaration != nil && !q.admitsRoot(record.Declaration, record.Path) {
		return false, nil
	}
	children, err := Promote(ctx, l.Store, id, PromoteOptions{
		Record: manifest.Production{Authored: record.Authored, Path: record.Path, File: record.File},
		Reads:  reads, Documents: l.PromotionDocuments(id),
	})
	if err != nil {
		return false, err
	}
	for i := range children {
		child := children[i]
		sibling, known := l.Index.Lookup(child.Obj.Named())
		if !known || cmp.Or(sibling.Builder, sibling.File) != cmp.Or(record.Builder, record.File) ||
			!q.admitsRoot(child.Obj, record.Path) {
			continue
		}
		if ctx.Err() != nil || !reads.Valid() {
			break
		}
		for j := range child.Warnings {
			warning := child.Warnings[j]
			warning.Resource = child.Obj.Named()
			l.Store.AddWarning(warning)
		}
		reads.Admit(child, child.SourceSupport, manifest.NamedResource{})
	}
	return reads.Valid() && l.Store.GetObject(id) != nil, nil
}
