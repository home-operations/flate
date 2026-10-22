package discovery

import (
	"cmp"
	"context"
	"log/slog"
	"slices"

	"github.com/home-operations/flate/pkg/loader"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

func (d *discoverer) promoteOrphans(ctx context.Context, standaloneSecrets []manifest.NamedResource) {
	index := d.loader.Index
	// All admitted stand-ins MUST be removed before promotion so real sibling Secrets take precedence.
	slices.SortFunc(standaloneSecrets, manifest.NamedResource.Compare)
	var saved []store.Child
	for _, id := range standaloneSecrets {
		record, _ := index.Lookup(id)
		if obj, ok := d.cfg.Store.GetObject(id).(*manifest.Secret); ok && d.loader.AdmitsRoot(id, record.Path) {
			_, doc, emission := d.cfg.Store.GetEmission(id)
			emission.Finish()
			saved = append(saved, store.Child{Obj: obj, Doc: doc})
			d.cfg.Store.DeleteObject(id)
		}
	}
	for id, record := range index.Files() {
		if d.cfg.Store.GetObject(id) != nil || !d.loader.AdmitsRoot(id, record.Path) {
			continue
		}
		admit := func(obj manifest.BaseManifest) bool {
			sibling, ok := index.Lookup(obj.Named())
			return ok && cmp.Or(sibling.Builder, sibling.File) == cmp.Or(record.Builder, record.File) && d.loader.AdmitsRoot(obj.Named(), record.Path)
		}
		opts := loader.PromoteOptions{Record: manifest.Production{Authored: record.Authored, Path: record.Path, File: record.File}, WipeSecrets: d.cfg.WipeSecrets, Admit: admit}
		if record.Path == "" {
			opts.Documents = d.loader.BuiltDocuments(id)
		}
		if !loader.Promote(ctx, d.cfg.Store, id, opts) {
			slog.Debug("discovery: orphan promotion failed", "id", id.String())
		}
	}
	for _, child := range saved {
		obj := child.Obj
		if d.cfg.Store.GetObject(obj.Named()) == nil {
			d.cfg.Store.AddObject(obj, child.Doc)
		}
	}
}
