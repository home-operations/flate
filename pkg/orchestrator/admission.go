package orchestrator

import (
	"context"
	"fmt"
	"iter"
	"log/slog"
	"maps"
	"slices"

	"github.com/home-operations/flate/pkg/change"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/schedule"
	"github.com/home-operations/flate/pkg/selection"
	"github.com/home-operations/flate/pkg/store"
)

func (o *Orchestrator) prepareInvocation(ctx context.Context) error {
	prepared := false
	defer func() {
		o.preparation = nil
		if !prepared {
			o.admissionPreparation.Retire()
			o.admissionPreparation = nil
			o.selection.DiscardConstruction()
			for range o.store.Sweep() {
			}
		}
	}()
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
	}
	if o.selection == nil {
		prepared = true
		return nil
	}
	if err := o.lookupHistory(ctx); err != nil {
		return err
	}
	o.configureControllers()
	o.loader.Options.Changes = o.cfg.ExternalChanges
	o.loader.Options.ConfiguredInput = o.cfg.Input
	options := selection.AdmissionOptions{
		Request: selection.PrepareRequest{Invocation: true}, Previous: o.preparation, Stage: selection.Construction,
	}
	if options.Previous == nil {
		options.Request.Preparation = &o.admissionPreparation
	}
	result, accepted, err := o.prepareAdmission(ctx, options)
	if err != nil || !accepted {
		return err
	}
	o.preparation = &result
	for range o.store.Sweep() {
	}
	prepared = true
	return nil
}

//nolint:gocritic // Admission setup MUST leave the caller's request snapshot unchanged.
func (o *Orchestrator) prepareAdmission(
	ctx context.Context,
	options selection.AdmissionOptions,
) (selection.AdmissionResult, bool, error) {
	o.admissionRequest(&options.Request)
	result, accepted, err := o.selection.Admit(ctx, options)
	if accepted {
		o.history = result.Request.History
	}
	return result, accepted, err
}

func (o *Orchestrator) admissionRequest(request *selection.PrepareRequest) {
	o.loader.Options.Changes = o.cfg.ExternalChanges
	o.loader.Options.ConfiguredInput = o.cfg.Input
	request.Changes, request.HistoryOwners = o.cfg.ExternalChanges, o.historyOwners
	request.Missing = o.cfg.missing
	request.History = o.history
	request.ReadHistory = o.readProducerHistory
}

func (o *Orchestrator) readProducerHistory(
	ctx context.Context,
	demand *selection.Demand,
) (iter.Seq2[manifest.NamedResource, manifest.DiscoveryRecord], bool) {
	id, obj := demand.Identity, demand.Witness
	if obj == nil {
		return nil, false
	}
	slot := o.probeSlot(id)
	previous := *slot
	history := o.controllers.PreviousRender(ctx, obj, schedule.Result{Assessment: previous})
	if history.Assessment != nil {
		defer o.store.Retire(&history.Assessment.Reads)
		if previous != nil {
			history.Assessment.ProbeLeft, history.Assessment.ProbeRight = previous.ProbeLeft, previous.ProbeRight
			previous.ProbeLeft, previous.ProbeRight = nil, nil
			previous.Retire()
		}
		*slot = history.Assessment
	}
	records, canNarrow := history.Records, history.Complete
	if records == nil {
		return nil, false
	}
	reads := &history.Assessment.Reads
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
	if o.historyOwners != nil {
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
	o.historyOwners = producers
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
	if o.selection == nil && len(o.admission.EntryProducers()) == 0 {
		return nil
	}
	o.loader.Options.Changes = o.cfg.ExternalChanges
	root, binding, err := o.admission.BindEntry(ctx, o.store)
	if err != nil {
		return err
	}
	if o.selection == nil {
		if binding.Reads != nil {
			_, err = o.commitEntry(ctx, root, &binding)
		}
		return err
	}
	defer o.store.Retire(binding.Reads)
	defer binding.Reads.ReleaseCapture()
	current := o.entryBinding
	if binding.Reads != nil {
		current = binding
	}

	request := selection.PrepareRequest{Invocation: true, History: o.history, Reads: binding.Reads}
	if binding.Reads != nil && root != nil && root != o.store.GetDeclaration(root.Named()) {
		application := manifest.EntryApplication{Source: current.Source, Path: current.Path}
		request.Generation = []store.Child{{Obj: root, Entry: &application}}
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
	request.Preparation = &o.admissionPreparation
	options := selection.AdmissionOptions{Request: request, Stage: selection.Execution}
	if current.Entry != nil {
		options.EntryHistory.Owner, options.EntryHistory.Records = current.Entry.Owner, current.Entry.Records
	}
	result, committed, err := o.prepareAdmission(ctx, options)
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

	o.recordAdmission(&result)
	return nil
}

func (o *Orchestrator) recordAdmission(result *selection.AdmissionResult) {
	o.changeNamespaces = result.Namespaces
	o.preparation = nil
	o.admissionPreparation.Retire()
	o.admissionPreparation = nil
	if len(o.admission.EntryProducers()) == 0 {
		o.history = nil
	}
	o.historyOwners = nil
	if changes := o.cfg.ExternalChanges; changes != nil && changes.Len() == 0 && len(result.Needed) == 0 &&
		o.cfg.BaseInput != nil {
		o.store.AddWarning(
			manifest.Warning{
				Category: manifest.WarnPathConfig,
				Message:  "no changes detected between --path and --path-orig; output will be empty; Flux-excluded files are not counted; verify both paths reference distinct snapshots",
			},
		)
	}
	slog.Debug("orchestrator: changed-only keep set", "needed", result.Needed)
}
