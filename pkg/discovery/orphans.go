package discovery

import (
	"cmp"
	"context"
	"log/slog"

	"github.com/home-operations/flate/pkg/loader"
	"github.com/home-operations/flate/pkg/manifest"
)

func (d *Session) promoteOrphans(ctx context.Context) error {
	index := d.loader.Index
	for id, record := range index.Files() { //nolint:gocritic // The iterator supplies independent value snapshots.
		if obj, policy := d.cfg.Store.Lookup(id); obj != nil && policy.Root ||
			record.Declaration != nil && !d.loader.AdmitsRoot(record.Declaration, record.Path) {
			continue
		}
		admit := func(obj manifest.BaseManifest) (bool, error) {
			sibling, ok := index.Lookup(obj.Named())
			return ok && cmp.Or(sibling.Builder, sibling.File) == cmp.Or(record.Builder, record.File) &&
				d.loader.AdmitsRoot(obj, record.Path), nil
		}
		opts := loader.PromoteOptions{
			Record: manifest.Production{Authored: record.Authored, Path: record.Path, File: record.File},
			Admit:  admit,
		}
		opts.Documents = d.loader.PromotionDocuments(id)
		promoted, err := loader.Promote(ctx, d.cfg.Store, id, opts)
		if err != nil {
			return err
		}
		if !promoted {
			slog.Debug("discovery: orphan promotion failed", "id", id.String())
		}
	}
	return nil
}
