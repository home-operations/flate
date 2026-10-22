package discovery

import (
	"context"
	"log/slog"
	"slices"

	"github.com/home-operations/flate/pkg/loader"
	"github.com/home-operations/flate/pkg/manifest"
)

func (d *discoverer) promoteOrphans(ctx context.Context, standaloneSecrets []manifest.NamedResource) {
	index := d.loader.Index
	// All admitted stand-ins MUST be removed before promotion so real sibling Secrets take precedence.
	slices.SortFunc(standaloneSecrets, manifest.NamedResource.Compare)
	var saved []*manifest.Secret
	for _, id := range standaloneSecrets {
		record, _ := index.Lookup(id)
		if obj, ok := d.cfg.Store.GetObject(id).(*manifest.Secret); ok && d.admitOrphan(id, record.Path) {
			saved = append(saved, obj)
			d.cfg.Store.DeleteObject(id)
		}
	}
	for id, record := range index.Files() {
		if d.cfg.Store.GetObject(id) != nil || !d.admitOrphan(id, record.Path) {
			continue
		}
		admit := func(obj manifest.BaseManifest) bool { return d.admitOrphan(obj.Named(), record.Path) }
		if !loader.Promote(ctx, d.cfg.Store, id, loader.PromoteOptions{Record: manifest.Production{Authored: record.Authored, Path: record.Path, File: record.File}, WipeSecrets: d.cfg.WipeSecrets, Admit: admit}) {
			slog.Debug("discovery: orphan promotion failed", "id", id.String())
		}
	}
	for _, obj := range saved {
		if d.cfg.Store.GetObject(obj.Named()) == nil {
			d.cfg.Store.AddObject(obj)
		}
	}
}

func (d *discoverer) admitOrphan(id manifest.NamedResource, path string) bool {
	record, ok := d.loader.Index.Lookup(id)
	if !ok || record.Path == "" || record.Path != path {
		return false
	}
	owners, ancestors := d.loader.Index.OwnersOfFile(record.File)
	return !slices.ContainsFunc(owners, func(owner manifest.NamedResource) bool { return owner != id }) &&
		!slices.ContainsFunc(ancestors, func(owner manifest.NamedResource) bool { return owner != id })
}
