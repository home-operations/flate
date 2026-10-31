package orchestrator

import (
	"context"
	"iter"
	"log/slog"
	"maps"
	"slices"

	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

func (o *Orchestrator) buildChangeFilter(ctx context.Context) error {
	defer func() { o.entryDiscovery = nil }()
	if err := ctx.Err(); err != nil {
		return err
	}
	if o.cfg.BaseInput == o.cfg.Input && o.cfg.ExternalChanges == nil {
		o.store.AddWarning(manifest.Warning{Category: manifest.WarnPathConfig, Message: "--path and --path-orig resolve to the same root; ignoring --path-orig", Detail: []string{o.repoRoot}})
		if o.loader == nil {
			return nil
		}
	}
	if o.loader == nil {
		return nil
	}
	o.loader.Options.Changes = o.cfg.ExternalChanges
	proposal := o.preparation
	if proposal == nil {
		var err error
		proposal, err = o.loader.Prepare(ctx, store.Child{}, nil, nil)
		if err != nil {
			return err
		}
	}
	o.preparation = proposal
	history := map[manifest.NamedResource]struct {
		Records  iter.Seq2[manifest.NamedResource, manifest.DiscoveryRecord]
		Complete bool
	}{}
	defer func() {
		for range o.store.Sweep() {
		}
	}()
	o.configureControllers()
	for _, id := range proposal.Needed {
		if err := ctx.Err(); err != nil {
			return err
		}
		if !id.KindInfo().Render {
			continue
		}
		reads := &store.ReadSet{}
		reads.Init(o.store, id, nil)
		func() {
			_, _, output := reads.GetEmission(id)
			defer reads.Retire()
			defer output.Finish()
			obj := reads.GetDeclaration(id)
			if obj == nil {
				obj = o.index.Records[id].Declaration
			}
			if obj == nil {
				return
			}
			records, canNarrow := o.controllers.PreviousRender(ctx, obj, reads)
			if records == nil {
				return
			}
			materialized := make(map[manifest.NamedResource]manifest.DiscoveryRecord)
			for child, record := range records {
				record.Refs = slices.Clone(record.Refs)
				record.Productions = slices.Clone(record.Productions)
				record.Inputs = slices.Clone(record.Inputs)
				record.ContentRoots = maps.Clone(record.ContentRoots)
				for source, roots := range record.ContentRoots {
					record.ContentRoots[source] = slices.Clone(roots)
				}
				materialized[child] = record
			}
			if reads.Valid() {
				for child, record := range materialized {
					o.admission.AddHistory(o.store, child, record)
				}
				entry := history[id]
				entry.Complete = canNarrow
				entry.Records = func(yield func(manifest.NamedResource, manifest.DiscoveryRecord) bool) {
					for child, record := range materialized {
						if !yield(child, record) {
							return
						}
					}
				}
				history[id] = entry
			}
		}()
	}
	if o.entryDiscovery != nil {
		if err := o.entryDiscovery.PrepareEntry(ctx); err != nil {
			return err
		}
		o.index, o.entryBinding = o.entryDiscovery.Index, o.entryDiscovery.Binding
	}
	o.history = history
	if err := ctx.Err(); err != nil {
		return err
	}

	return nil
}

func (o *Orchestrator) installAdmission(ctx context.Context) error {
	o.loader.Options.Changes = o.cfg.ExternalChanges
	proposal, err := o.loader.Prepare(ctx, store.Child{}, nil, o.history)
	if err != nil {
		return err
	}
	added, retry := o.filter.AddEmitted(proposal.Needed, proposal.Selected, proposal.Dirty, proposal.Revision)
	if retry {
		return manifest.NewError(manifest.ErrInput, nil, "startup membership changed during preparation")
	}
	for _, id := range added {
		o.store.SetIncluded(id, true)
	}
	for _, child := range proposal.Objects {
		if err := ctx.Err(); err != nil {
			return err
		}
		o.store.AddObject(child.Obj, child.Doc)
	}
	o.changeNamespaces = proposal.Namespaces
	o.preparation, o.history = nil, nil
	if changes := o.cfg.ExternalChanges; changes != nil && changes.Len() == 0 && len(o.filter.KeepNames()) == 0 && o.cfg.BaseInput != nil {
		o.store.AddWarning(manifest.Warning{Category: manifest.WarnPathConfig, Message: "no changes detected between --path and --path-orig; output will be empty; Flux-excluded files are not counted; verify both paths reference distinct snapshots"})
	}
	slog.Debug("orchestrator: changed-only keep set", "filter", o.filter)
	return nil
}
