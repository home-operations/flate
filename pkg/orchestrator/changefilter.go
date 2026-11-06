package orchestrator

import (
	"context"
	"fmt"
	"iter"
	"log/slog"
	"maps"
	"slices"

	"github.com/home-operations/flate/pkg/change"
	"github.com/home-operations/flate/pkg/kustomize"
	"github.com/home-operations/flate/pkg/loader"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

func (o *Orchestrator) buildChangeFilter(ctx context.Context) error {
	defer func() { o.preparation.Retire(); o.preparation = nil }()
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
		if !o.filter.Enabled() {
			return nil
		}
	}
	if !o.filter.Enabled() {
		return nil
	}
	o.loader.Options.Changes = o.cfg.ExternalChanges
	if err := o.lookupHistory(ctx); err != nil {
		return err
	}
	proposal := o.preparation
	if proposal == nil {
		var err error
		proposal, err = o.loader.Prepare(ctx, loader.PrepareRequest{Invocation: true, History: o.history})
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
	if obj == nil && len(o.loader.Options.HistoryOwners[id]) != 0 {
		obj = o.loader.CatalogObject(id)
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
	if producers == nil {
		producers = map[manifest.NamedResource][]manifest.SourceEvidence{}
	}
	o.loader.Options.HistoryOwners = producers
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
	if !o.filter.Enabled() && len(o.admission.EntryProducers()) == 0 {
		return nil
	}
	o.loader.Options.Changes = o.cfg.ExternalChanges
	root, binding, err := o.admission.BindEntry(ctx, o.store)
	if err != nil {
		return err
	}
	defer o.store.Retire(binding.Reads)
	defer binding.Reads.ReleaseCapture()
	current := o.entryBinding
	if binding.Reads != nil {
		current = binding
	}

	o.setEntryHistory(current.Entry)
	request := loader.PrepareRequest{Invocation: true, History: o.history, Reads: binding.Reads}
	if binding.Reads != nil && root != nil && root != o.store.GetDeclaration(root.Named()) {
		application := manifest.EntryApplication{Source: current.Source, Path: current.Path}
		request.Incoming = store.Child{Obj: root, Entry: &application}
	}
	var reads store.ReadSet
	if inputs := o.admission.EntryProducers(); binding.Reads == nil && len(inputs) != 0 {
		request.Invocation = false
		reads.Init(o.store, manifest.BootstrapSourceID, nil)
		defer o.store.Retire(&reads)
		for _, id := range inputs {
			reads.RequireQualification(id)
		}
		request.Reads = &reads
	}
	proposal, err := o.loader.Prepare(ctx, request)
	if err != nil {
		return err
	}
	committed, err := o.loader.Commit(ctx, proposal, loader.Execution)
	if err != nil {
		return err
	}
	if !committed {
		return nil
	}
	if binding.Reads != nil {
		accepted, acceptErr := o.acceptEntry(ctx, &binding)
		if acceptErr != nil || !accepted {
			return acceptErr
		}
	}

	o.changeNamespaces = proposal.Namespaces
	o.preparation = nil
	if len(o.admission.EntryProducers()) == 0 {
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

func (o *Orchestrator) setEntryHistory(entry *kustomize.EntryEvidence) {
	if entry != nil {
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
}
