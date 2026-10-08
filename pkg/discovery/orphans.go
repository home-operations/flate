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
func (d *discoverer) promoteOrphans(prefixes []loader.KSPathPrefix, selfProduce *loader.SelfProduceIndex, entries map[manifest.NamedResource]string, standaloneSecrets []manifest.NamedResource) {
	if d.loader.Existence == nil {
		return
	}
	owned := func(id manifest.NamedResource, file string) bool {
		if _, covered := loader.LongestParent(prefixes, file, id); covered {
			return true
		}
		for _, owner := range selfProduce.OwnersOfFile(file) {
			if owner != id {
				return true
			}
		}
		return false
	}
	admit := func(obj manifest.BaseManifest) bool {
		file, ok := d.sourceFiles[obj.Named()]
		return ok && !owned(obj.Named(), file)
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
	for id := range entries {
		if d.cfg.Store.GetObject(id) != nil {
			continue
		}
		if file, ok := d.sourceFiles[id]; ok && owned(id, file) {
			continue
		}
		if !d.loader.Existence.Promote(d.cfg.Store, id, d.cfg.WipeSecrets, admit) {
			slog.Debug("discovery: orphan promotion failed", "id", id.String())
		}
	}
	for _, obj := range saved {
		if d.cfg.Store.GetObject(obj.Named()) == nil {
			d.cfg.Store.AddObject(obj)
		}
	}
}
