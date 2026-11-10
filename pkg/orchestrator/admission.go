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
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/schedule"
	"github.com/home-operations/flate/pkg/selection"
	"github.com/home-operations/flate/pkg/store"
)

func (o *Orchestrator) prepareInvocation(ctx context.Context) error {
	prepared := false
	defer func() {
		o.preparation.Retire()
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
	proposal := o.preparation
	if proposal == nil {
		var err error
		proposal, err = o.prepareAdmission(
			ctx,
			selection.PrepareRequest{Invocation: true, Preparation: &o.admissionPreparation},
		)
		if err != nil {
			return err
		}
	} else {
		var err error
		proposal, err = o.prepareHistory(ctx, proposal)
		if err != nil {
			return err
		}
	}
	o.preparation = proposal
	for range o.store.Sweep() {
	}
	prepared = true
	return nil
}

//nolint:gocritic // The driver MUST retain an independent caller request while acquiring successive proposals.
func (o *Orchestrator) prepareAdmission(
	ctx context.Context,
	request selection.PrepareRequest,
) (*selection.Admission, error) {
	o.admissionRequest(&request)
	proposal, err := o.selection.Prepare(ctx, request)
	if err != nil {
		return nil, err
	}
	return o.acquireAdmission(ctx, proposal, selection.Construction, o.selection.Prepare)
}

func (o *Orchestrator) admissionRequest(request *selection.PrepareRequest) {
	o.loader.Options.Changes = o.cfg.ExternalChanges
	o.loader.Options.ConfiguredInput = o.cfg.Input
	request.Changes, request.HistoryOwners = o.cfg.ExternalChanges, o.historyOwners
	request.Missing = o.cfg.missing
	if request.Preparation == nil {
		request.Preparation = &o.admissionPreparation
	}
	if o.history == nil {
		o.history = map[manifest.NamedResource]struct {
			Records  iter.Seq2[manifest.NamedResource, manifest.DiscoveryRecord]
			Complete bool
		}{}
	}
	request.History = o.history
}

func (o *Orchestrator) prepareHistory(
	ctx context.Context,
	previous *selection.Admission,
) (*selection.Admission, error) {
	var preparation *selection.Preparation
	defer func() { preparation.Retire() }()
	request := selection.PrepareRequest{Invocation: true, Preparation: &preparation}
	o.admissionRequest(&request)
	prepare := func(ctx context.Context, request selection.PrepareRequest) (*selection.Admission, error) {
		return o.selection.RefreshHistory(ctx, previous, request)
	}
	proposal, err := prepare(ctx, request)
	previous.Retire()
	if err != nil {
		return nil, err
	}
	proposal, err = o.acquireAdmission(ctx, proposal, selection.Construction, prepare)
	proposal.Retire()
	return proposal, err
}

func (o *Orchestrator) acquireAdmission(
	ctx context.Context,
	proposal *selection.Admission,
	stage selection.CommitStage,
	prepare func(context.Context, selection.PrepareRequest) (*selection.Admission, error),
) (*selection.Admission, error) {
	for {
		request := proposal.Request
		request.Acquired = nil
		progressed := false
		var err error
		for demand := range proposal.Demands() {
			result := selection.Acquisition{Demand: demand, Reads: demand.Reads}
			switch {
			case demand.History && demand.Root == "":
				records, complete := o.readProducerHistory(ctx, &demand)
				o.history[demand.Identity] = struct {
					Records  iter.Seq2[manifest.NamedResource, manifest.DiscoveryRecord]
					Complete bool
				}{records, complete}
				result.Available = records != nil
				request.Acquired = append(request.Acquired, result)
			case demand.History:
				result.Root, result.Available, err = o.loader.ResolveRoot(ctx, demand.Reads, demand.Source, demand.Root)
				request.Acquired = append(request.Acquired, result)
			case demand.Occurrence.Identity != (manifest.NamedResource{}):
				result.Child, err = o.loader.OccurrenceObject(ctx, &demand.Occurrence, demand.Artifact)
				result.Available = result.Child.Obj != nil
				request.Acquired = append(request.Acquired, result)
			case stage == selection.Execution:
				request.Acquired = append(request.Acquired, result)
			default:
				continue
			}
			if err != nil {
				proposal.Retire()
				return nil, err
			}
			progressed = true
		}
		if !progressed {
			return proposal, nil
		}
		next, err := prepare(ctx, request)
		proposal.Retire()
		if err != nil {
			return nil, err
		}
		proposal = next
	}
}

func (o *Orchestrator) commitAdmission(
	ctx context.Context,
	proposal *selection.Admission,
) (*selection.Admission, bool, error) {
	for {
		var err error
		proposal, err = o.acquireAdmission(ctx, proposal, selection.Execution, o.selection.Prepare)
		if err != nil {
			return nil, false, err
		}
		var committed bool
		proposal, committed, err = o.selection.Commit(ctx, proposal, selection.Execution)
		if err != nil || committed || proposal == nil {
			return proposal, committed, err
		}
	}
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

	o.setEntryHistory(current.Entry)
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
	proposal, err := o.prepareAdmission(ctx, request)
	if err != nil {
		return err
	}
	proposal, committed, err := o.commitAdmission(ctx, proposal)
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

	o.recordAdmission(proposal)
	return nil
}

func (o *Orchestrator) recordAdmission(proposal *selection.Admission) {
	o.changeNamespaces = proposal.Namespaces
	o.preparation = nil
	o.admissionPreparation.Retire()
	o.admissionPreparation = nil
	if len(o.admission.EntryProducers()) == 0 {
		o.history = nil
	}
	o.historyOwners = nil
	if changes := o.cfg.ExternalChanges; changes != nil && changes.Len() == 0 && len(proposal.Needed) == 0 &&
		o.cfg.BaseInput != nil {
		o.store.AddWarning(
			manifest.Warning{
				Category: manifest.WarnPathConfig,
				Message:  "no changes detected between --path and --path-orig; output will be empty; Flux-excluded files are not counted; verify both paths reference distinct snapshots",
			},
		)
	}
	slog.Debug("orchestrator: changed-only keep set", "needed", proposal.Needed)
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
