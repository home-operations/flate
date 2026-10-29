package orchestrator

import (
	"context"
	"iter"
	"log/slog"
	"maps"
	"slices"

	"github.com/home-operations/flate/pkg/store"

	"github.com/home-operations/flate/pkg/change"
	"github.com/home-operations/flate/pkg/manifest"
)

func (o *Orchestrator) buildChangeFilter(ctx context.Context) error {
	defer func() { o.entryDiscovery = nil }()
	if err := ctx.Err(); err != nil {
		return err
	}
	if o.cfg.BaseInput == o.cfg.Input && o.cfg.ExternalChanges == nil {
		o.store.AddWarning(manifest.Warning{Category: manifest.WarnPathConfig, Message: "--path and --path-orig resolve to the same root; ignoring --path-orig", Detail: []string{o.repoRoot}})
		return nil
	}
	changes := o.cfg.ExternalChanges
	if changes == nil {
		return nil
	}
	history := map[manifest.NamedResource]iter.Seq2[manifest.NamedResource, manifest.DiscoveryRecord]{}
	narrow := map[manifest.NamedResource]bool{}
	defer func() {
		for range o.store.Sweep() {
		}
	}()
	o.configureControllers()
	objects := o.store.Objects()
	for id, record := range o.index.Records {
		if record.Declaration != nil && id.KindInfo().Render && o.store.GetObject(id) == nil {
			objects = append(objects, record.Declaration)
		}
	}
	slices.SortFunc(objects, func(a, b manifest.BaseManifest) int { return a.Named().Compare(b.Named()) })
	for _, obj := range objects {
		if err := ctx.Err(); err != nil {
			return err
		}
		id := obj.Named()
		if !manifest.KindOf(obj).Render {
			continue
		}
		reads := &store.ReadSet{}
		reads.Init(o.store, id, nil)
		func() {
			_, _, output := reads.GetEmission(id)
			defer output.Finish()
			defer reads.Retire()
			records, canNarrow := o.controllers.PreviousRender(ctx, obj, reads)
			narrow[id] = canNarrow
			if records == nil {
				return
			}
			materialized := maps.Collect(records)
			if reads.Valid() {
				for child, record := range materialized {
					o.admission.AddHistory(child, record)
				}
				history[id] = func(yield func(manifest.NamedResource, manifest.DiscoveryRecord) bool) {
					for child, record := range materialized {
						if !yield(child, record) {
							return
						}
					}
				}
			}
		}()
	}
	if o.entryDiscovery != nil {
		if err := o.entryDiscovery.PrepareEntry(ctx); err != nil {
			return err
		}
		o.index, o.entryBinding = o.entryDiscovery.Index, o.entryDiscovery.Binding
	}
	f := change.NewFilter(changes, o.index, o.store, o.repoRoot)
	f.RefineHistory(history, narrow)
	if err := ctx.Err(); err != nil {
		return err
	}
	if changes.Len() == 0 && len(f.KeepNames()) == 0 && o.cfg.BaseInput != nil {
		o.store.AddWarning(manifest.Warning{
			Category: manifest.WarnPathConfig,
			Message:  "no changes detected between --path and --path-orig; output will be empty; Flux-excluded files are not counted; verify both paths reference distinct snapshots",
		})
	}
	o.filter = f
	slog.Debug("orchestrator: changed-only keep set", "filter", o.filter)
	return nil
}

// computeChangeSet compares direct inputs in source-root coordinates.
// PathOrig is a source root; an admitted BaseInput may have a synthetic label.
