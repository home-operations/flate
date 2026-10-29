package discovery

import (
	"cmp"
	"context"
	"log/slog"
	"maps"
	"slices"

	"github.com/home-operations/flate/pkg/loader"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

func (d *discoverer) promoteOrphans(ctx context.Context, standaloneSecrets []manifest.NamedResource) {
	index := d.loader.Index
	var prerequisites map[manifest.NamedResource]bool
	if root, ok := d.cfg.Store.GetObject(d.binding.Producer).(*manifest.Kustomization); ok && (len(root.DependsOn) != 0 || root.PostBuild != nil && len(root.PostBuild.SubstituteFrom) != 0) {
		prerequisites = map[manifest.NamedResource]bool{root.Named(): true}
		pending := []manifest.NamedResource{root.Named()}
		add := func(id manifest.NamedResource) {
			if id != (manifest.NamedResource{}) && id != root.Named() && !prerequisites[id] {
				prerequisites[id] = true
				pending = append(pending, id)
			}
		}
		ids := slices.SortedFunc(maps.Keys(index.Records), manifest.NamedResource.Compare)
		outputs := make(map[manifest.NamedResource]bool)
		for _, id := range ids {
			for _, production := range index.Records[id].Productions {
				if production.Owner != id {
					outputs[production.Owner] = true
				}
			}
		}
		for len(pending) != 0 {
			id := pending[0]
			pending = pending[1:]
			record := index.Records[id]
			if id != root.Named() {
				add(record.Parent)
				for _, production := range record.Productions {
					add(production.Owner)
					add(production.Declaration)
				}
			}
			obj := record.Declaration
			if obj == nil {
				obj = d.cfg.Store.GetObject(id)
			}
			for ref := range manifest.References(obj, manifest.ReferenceRelations) {
				add(ref.NamedResource)
				if _, exists := index.Lookup(ref.NamedResource); exists || d.cfg.Store.GetObject(ref.NamedResource) != nil {
					continue
				}
				for _, candidate := range ids {
					if record := index.Records[candidate]; record.Parent == root.Named() && candidate.KindInfo().Render && !outputs[candidate] {
						add(candidate)
					}
					if candidate.Group == ref.Group && candidate.Kind == ref.Kind && candidate.Namespace == ref.Namespace {
						add(index.Records[candidate].Parent)
					}
				}
			}
		}
		delete(prerequisites, root.Named())
		for id := range prerequisites {
			record, ok := index.Lookup(id)
			if !ok || record.Declaration == nil {
				delete(prerequisites, id)
				continue
			}
			record.Productions = append(record.Productions, manifest.Production{Authored: record.Declaration.Named(), Path: record.Path, File: record.File, Source: manifest.BootstrapSourceID})
			index.Records[id] = record
		}
	}
	// All admitted stand-ins MUST be removed before promotion so real sibling Secrets take precedence.
	slices.SortFunc(standaloneSecrets, manifest.NamedResource.Compare)
	var saved []store.Child
	for _, id := range standaloneSecrets {
		record, _ := index.Lookup(id)
		if obj, ok := d.cfg.Store.GetObject(id).(*manifest.Secret); ok && d.loader.AdmitsRoot(obj, record.Path) {
			_, doc, emission := d.cfg.Store.GetEmission(id)
			emission.Finish()
			saved = append(saved, store.Child{Obj: obj, Doc: doc})
			d.cfg.Store.DeleteObject(id)
		}
	}
	for id, record := range index.Files() {
		if d.cfg.Store.GetObject(id) != nil || record.Declaration != nil && !d.loader.AdmitsRoot(record.Declaration, record.Path) && !prerequisites[id] {
			continue
		}
		admit := func(obj manifest.BaseManifest) bool {
			id := obj.Named()
			sibling, ok := index.Lookup(id)
			return ok && cmp.Or(sibling.Builder, sibling.File) == cmp.Or(record.Builder, record.File) && (d.loader.AdmitsRoot(obj, record.Path) || prerequisites[id] && manifest.AdmitsRoot(obj))
		}
		opts := loader.PromoteOptions{Record: manifest.Production{Authored: record.Authored, Path: record.Path, File: record.File}, WipeSecrets: d.cfg.WipeSecrets, Admit: admit}
		opts.Documents = d.loader.PromotionDocuments(id)
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
