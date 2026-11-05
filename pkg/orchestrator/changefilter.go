package orchestrator

import (
	"context"
	"fmt"
	"iter"
	"log/slog"
	"maps"
	"slices"

	"github.com/home-operations/flate/pkg/change"
	"github.com/home-operations/flate/pkg/loader"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

func (o *Orchestrator) buildChangeFilter(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("context: %w", err)
	}
	if o.cfg.BaseInput == o.cfg.Input && o.cfg.ExternalChanges == nil {
		o.store.AddWarning(
			manifest.Warning{
				Category: manifest.WarnPathConfig,
				Message:  "--path and --path-orig resolve to the same root; ignoring --path-orig",
				Detail:   []string{o.repoRoot},
			},
		)
		if o.loader == nil {
			return nil
		}
	}
	if o.loader == nil {
		return nil
	}
	o.loader.Options.Changes = o.cfg.ExternalChanges
	if err := o.lookupHistory(ctx); err != nil {
		return err
	}
	proposal := o.preparation
	if proposal == nil {
		var err error
		proposal, err = o.loader.Prepare(ctx, store.Child{}, nil, o.history)
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
	if err := o.prepareNeededHistory(ctx, proposal.Needed, history); err != nil {
		return err
	}
	if _, err := o.admitEntry(ctx); err != nil {
		return err
	}
	o.history = history
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("context: %w", err)
	}

	return nil
}

func (o *Orchestrator) prepareNeededHistory(
	ctx context.Context,
	needed []manifest.NamedResource,
	history map[manifest.NamedResource]struct {
		Records  iter.Seq2[manifest.NamedResource, manifest.DiscoveryRecord]
		Complete bool
	},
) error {
	for _, id := range needed {
		if err := ctx.Err(); err != nil {
			return fmt.Errorf("context: %w", err)
		}
		if !id.KindInfo().Render {
			continue
		}
		records, complete := o.prepareProducerHistory(ctx, id)
		if records != nil {
			entry := history[id]
			entry.Complete, entry.Records = complete, records
			history[id] = entry
		}
	}
	return nil
}

func (o *Orchestrator) prepareProducerHistory(
	ctx context.Context,
	id manifest.NamedResource,
) (iter.Seq2[manifest.NamedResource, manifest.DiscoveryRecord], bool) {
	reads := &store.ReadSet{}
	reads.Init(o.store, id, nil)

	_, _, output := reads.GetEmission(id)
	defer reads.Retire()
	defer reads.ReleaseCapture()
	defer output.Finish()
	obj := reads.GetDeclaration(id)
	if obj == nil {
		obj = o.index.Records[id].Declaration
	}
	currentInput := o.cfg.Input == o.store.GetArtifact(manifest.BootstrapSourceID, store.ArtifactContent)
	if obj == nil || currentInput && o.index.Records[id].StaticOutput {
		return nil, false
	}
	reads.BeginExecution(obj)
	records, canNarrow := o.controllers.PreviousRender(ctx, obj, reads)
	if records == nil {
		return nil, false
	}
	materialized := make(map[manifest.NamedResource]manifest.DiscoveryRecord)
	for child, record := range records { //nolint:gocritic // The iterator yields records by value.
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
		if canNarrow {
			for child, record := range materialized { //nolint:gocritic // Map iteration preserves a value snapshot without an extra lookup.
				o.admission.AddHistory(o.store, child, record)
			}
		}
		return maps.All(materialized), canNarrow
	}
	return nil, false
}

func (o *Orchestrator) lookupHistory(ctx context.Context) error {
	if o.loader.Options.HistoryOwners != nil {
		return ctx.Err() //nolint:wrapcheck // History-owner reuse returns the original cancellation error.
	}
	var query []manifest.SourceEvidence
	changes := o.cfg.ExternalChanges
	for source := range changes.Sources() {
		query = o.appendHistoryQuery(query, source, changes)
	}
	producers, err := o.cache.ProducersForPaths(ctx, query)
	if err != nil {
		return err
	}
	o.loader.Options.HistoryOwners = append([]manifest.NamedResource{}, producers...)
	return nil
}

func (o *Orchestrator) appendHistoryQuery(
	query []manifest.SourceEvidence,
	source manifest.NamedResource,
	changes *change.Set,
) []manifest.SourceEvidence {
	delta, inputs, _ := changes.ForSource(source)
	paths := delta.Paths()
	if len(paths) == 0 {
		return query
	}
	found := false
	for _, input := range inputs {
		if input != nil && input.FS != nil && input.Reads.Valid() {
			for _, file := range paths {
				query = append(query, manifest.SourceEvidence{Source: input.HistoryIdentity(source), Path: file})
			}
			found = true
		}
	}
	if !found && source == manifest.BootstrapSourceID {
		if input := o.cfg.Input; input != nil && input.FS != nil && input.Reads.Valid() {
			for _, file := range paths {
				query = append(query, manifest.SourceEvidence{Source: input.HistoryIdentity(source), Path: file})
			}
		}
	}
	return query
}

func (o *Orchestrator) installAdmission(ctx context.Context) error {
	o.loader.Options.Changes = o.cfg.ExternalChanges
	if entry := o.entryBinding.Entry; entry != nil {
		if o.history == nil {
			o.history = map[manifest.NamedResource]struct {
				Records  iter.Seq2[manifest.NamedResource, manifest.DiscoveryRecord]
				Complete bool
			}{}
		}
		history := o.history[entry.Owner]
		history.Records, history.Complete = entry.Records, true
		o.history[entry.Owner] = history
	}
	var proposal *loader.Admission
	var err error
	if inputs := o.admission.EntryInputs(); len(inputs) != 0 {
		var reads store.ReadSet
		reads.Init(o.store, manifest.NamedResource{}, nil)
		for _, id := range inputs {
			reads.RequireData(id)
		}
		proposal, err = o.loader.Prepare(ctx, store.Child{}, &reads, nil)
		reads.Retire()
	} else {
		proposal, err = o.loader.Prepare(ctx, store.Child{}, nil, o.history)
	}
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
			return fmt.Errorf("context: %w", err)
		}
		o.store.AddObject(child.Obj, child.Doc)
	}
	o.changeNamespaces = proposal.Namespaces
	o.preparation = nil
	if len(o.admission.EntryInputs()) == 0 {
		o.history = nil
	}
	o.loader.Options.HistoryOwners = nil
	if changes := o.cfg.ExternalChanges; changes != nil && changes.Len() == 0 && len(o.filter.KeepNames()) == 0 &&
		o.cfg.BaseInput != nil {
		o.store.AddWarning(
			manifest.Warning{
				Category: manifest.WarnPathConfig,
				Message:  "no changes detected between --path and --path-orig; output will be empty; Flux-excluded files are not counted; verify both paths reference distinct snapshots",
			},
		)
	}
	slog.Debug("orchestrator: changed-only keep set", "filter", o.filter)
	return nil
}
