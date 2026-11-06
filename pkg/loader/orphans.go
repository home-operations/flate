package loader

import (
	"cmp"
	"context"
	"log/slog"

	"github.com/home-operations/flate/pkg/manifest"
)

// PromoteOrphans MUST run after index finalization.
func (l *Loader) PromoteOrphans(ctx context.Context) error {
	index := l.Index
	query := rootQuery{loader: l}
	for id, record := range index.Files() { //nolint:gocritic // The iterator supplies independent value snapshots.
		if obj, policy := l.Store.Lookup(id); obj != nil && policy.Root ||
			record.Declaration != nil && !query.admitsRoot(record.Declaration, record.Path) {
			continue
		}
		admit := func(obj manifest.BaseManifest) (bool, error) {
			query.live = nil
			sibling, ok := index.Lookup(obj.Named())
			return ok && cmp.Or(sibling.Builder, sibling.File) == cmp.Or(record.Builder, record.File) &&
				query.admitsRoot(obj, record.Path), nil
		}
		opts := PromoteOptions{
			Record: manifest.Production{Authored: record.Authored, Path: record.Path, File: record.File},
			Admit:  admit,
		}
		opts.Documents = l.PromotionDocuments(id)
		promoted, err := Promote(ctx, l.Store, id, opts)
		if err != nil {
			return err
		}
		query.live = nil
		if !promoted {
			slog.Debug("loader: orphan promotion failed", "id", id.String())
		}
	}
	return nil
}
