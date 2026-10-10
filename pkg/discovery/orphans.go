package discovery

import (
	"log/slog"
	"slices"

	"github.com/home-operations/flate/pkg/loader"
	"github.com/home-operations/flate/pkg/manifest"
)

// promoteOrphans materializes file-indexed objects outside every local
// Kustomization's path and resource graph. Owned objects must await transformed
// render emission; the admission callback applies that boundary to siblings too.
func (d *discoverer) promoteOrphans(standaloneSecrets []manifest.NamedResource) {
	index := d.loader.Index
	owned := func(id manifest.NamedResource, file string) bool {
		owners, ancestors := index.OwnersOfFile(file)
		return slices.ContainsFunc(owners, func(owner manifest.NamedResource) bool { return owner != id }) ||
			slices.ContainsFunc(ancestors, func(owner manifest.NamedResource) bool { return owner != id })
	}
	admit := func(obj manifest.BaseManifest) bool {
		r, ok := index.Lookup(obj.Named())
		return ok && r.Path != "" && !owned(obj.Named(), r.File)
	}
	// Pre-build absence identifies builder materialization. Remove all admitted
	// stand-ins before any file promotion so real sibling Secrets take precedence.
	slices.SortFunc(standaloneSecrets, manifest.NamedResource.Compare)
	var saved []*manifest.Secret
	for _, id := range standaloneSecrets {
		if obj, ok := d.cfg.Store.GetObject(id).(*manifest.Secret); ok && admit(obj) {
			saved = append(saved, obj)
			d.cfg.Store.DeleteObject(id)
		}
	}
	for id, record := range index.Files() {
		if d.cfg.Store.GetObject(id) != nil {
			continue
		}
		if owned(id, record.File) {
			continue
		}
		if !loader.Promote(d.cfg.Store, id, record.Path, loader.PromoteOptions{WipeSecrets: d.cfg.WipeSecrets, Admit: admit}) {
			slog.Debug("discovery: orphan promotion failed", "id", id.String())
		}
	}
	for _, obj := range saved {
		if d.cfg.Store.GetObject(obj.Named()) == nil {
			d.cfg.Store.AddObject(obj)
		}
	}
}
