package discovery

import (
	"cmp"
	"context"
	"slices"

	"github.com/home-operations/flate/internal/engine/change/inputprojection"
	"github.com/home-operations/flate/internal/engine/selection"
	"github.com/home-operations/flate/internal/engine/source/safepath"
	"github.com/home-operations/flate/internal/engine/store"
	"github.com/home-operations/flate/pkg/manifest"
)

func (w *qualificationWalk) advance(ctx context.Context, hadCandidates bool) error {
	a, q := w.admission, w.q
	w.prepareProjectedOperands()
	if !q.closed && a.entryDemand && (len(q.packages) != 0 || len(q.active) != 0) {
		if err := w.advanceOperands(ctx); err != nil {
			return err
		}
		if q.pending || w.enoughMatches() {
			if q.pending {
				q.matches, q.winners = nil, nil
			}
			return nil
		}
	}
	if err := w.visitPublishedRoots(ctx); err != nil {
		return err
	}
	lostMatch := !q.pending && len(q.matches) == 0 && hadCandidates
	fullMatch := w.enoughMatches() && q.loader.Options.Session.Snapshot().Output(manifest.NamedResource{})
	if q.closed || lostMatch || fullMatch {
		if err := w.observe(ctx); err != nil {
			return err
		}
	}
	return nil
}

func (w *qualificationWalk) advanceOperands(ctx context.Context) error {
	q := w.q
	if err := w.observe(ctx); err != nil {
		return err
	}
	if q.pending || w.selectedTargetsReached() {
		return nil
	}
	q.operandsSettled = false
	if q.session != nil && !q.session.finalized {
		q.pending = true
		return nil
	}
	before := len(q.active)
	for _, operands := range q.packages {
		ranked, err := w.rankEntryOperands(ctx, operands)
		if err != nil {
			return err
		}
		w.admitOperandPackage(ranked)
	}
	q.packages = nil
	if len(q.active) > before {
		if q.changedOnly || len(w.admission.entryTargets) != 0 {
			q.candidates = []manifest.NamedResource{q.active[before]}
		} else {
			q.candidates = slices.Clone(q.active[before:])
		}
		q.pending = true
		return nil
	}
	failed, err := w.visitActiveOperands(ctx)
	if err != nil || q.pending {
		return err
	}
	q.operandsSettled = true
	if failed != (manifest.NamedResource{}) && len(q.matches) == 0 {
		q.pending = true
		q.candidates = []manifest.NamedResource{failed}
		w.reads.RequireQualification(failed)
		return nil
	}
	return nil
}

func (w *qualificationWalk) rankEntryOperands(
	ctx context.Context, operands []selection.OccurrenceFacts,
) ([]selection.OccurrenceFacts, error) {
	if !w.q.changedOnly || len(operands) <= 1 {
		return operands, nil
	}
	hints := make(map[manifest.NamedResource]int)
	for i := range operands {
		operand := &operands[i]
		_, target, applies := operand.SourcePath()
		if !applies || !operand.Identity.KindInfo().Structural {
			continue
		}
		hint, err := w.q.loader.EntryProducerHint(ctx, w.admission.input, target)
		if err != nil {
			return nil, err
		}
		if hint {
			hints[operand.Identity] = 1
		}
	}
	if len(hints) == 0 {
		return operands, nil
	}
	ranked := slices.Clone(operands)
	slices.SortStableFunc(ranked, func(a, b selection.OccurrenceFacts) int {
		return cmp.Compare(hints[b.Identity], hints[a.Identity])
	})
	return ranked, nil
}

func (w *qualificationWalk) prepareProjectedOperands() {
	a, q := w.admission, w.q
	if len(a.entryTargets) != 0 {
		a.entryChanged = slices.ContainsFunc(a.changedApplications, w.projectedApplication)
	}
	if q.closed || len(q.active) != 0 {
		return
	}
	changedPath := a.entryDemand && len(q.packages) == 0 && q.session != nil && q.session.cfg.Selection.Changed
	if len(a.entryTargets) == 0 && !changedPath {
		return
	}
	qualifies := w.projectedApplication
	if len(a.entryTargets) == 0 {
		direct := *w
		direct.projectedTargets = w.changedDefinitionTargets()
		qualifies = func(object manifest.BaseManifest) bool {
			projection := projectedPatch(object)
			if !projection.valid {
				return false
			}
			if projection.patches {
				return w.projectedApplication(object)
			}
			return len(direct.projectedTargets) != 0 && direct.projectedApplication(object)
		}
	}
	if operands := w.projectedOperands(qualifies); len(operands) != 0 {
		q.packages = [][]selection.OccurrenceFacts{operands}
	}
}

func (w *qualificationWalk) projectedOperands(qualifies func(manifest.BaseManifest) bool) []selection.OccurrenceFacts {
	loader := w.q.loader
	projections := loader.SourceFacts(manifest.BootstrapSourceID, w.reads).Context.TemplateProjections
	if len(projections) == 0 {
		return nil
	}
	entry := w.admission.input.OriginPath(w.admission.path)
	var operands []selection.OccurrenceFacts
	seen := make(map[manifest.NamedResource]bool)
	visited := make(map[manifest.NamedResource]bool)
	for producer, projection := range projections {
		if !slices.ContainsFunc(projection.Objects, qualifies) &&
			(len(projection.Objects) == 0 || !w.projectedValuesMayChange(loader.CatalogObject(producer))) {
			continue
		}
		clear(visited)
		id := producer
		for !visited[id] {
			record, known := loader.Index.Lookup(id)
			if !known || record.Parent == (manifest.NamedResource{}) || record.Parent.ReportHidden() {
				break
			}
			visited[id] = true
			id = record.Parent
		}
		if visited[id] || seen[id] {
			continue
		}
		seen[id] = true
		occurrence, known := loader.Occurrence(id)
		if !known || !occurrence.Located || occurrence.Mounted == nil ||
			!containsDeclaration(entry, &occurrence.Declaration, occurrence.Mounted, nil) {
			continue
		}
		operands = append(operands, occurrence)
	}
	slices.SortFunc(operands, func(a, b selection.OccurrenceFacts) int { return a.Identity.Compare(b.Identity) })
	return operands
}

func (w *qualificationWalk) visitActiveOperands(ctx context.Context) (manifest.NamedResource, error) {
	q := w.q
	var failed manifest.NamedResource
	for _, id := range q.active {
		if q.changedOnly && w.coveredOperand(id) {
			continue
		}
		obj := w.reads.GetDeclaration(id)
		if obj == nil || obj.Suspended() {
			continue
		}
		status, _ := w.reads.GetStatus(id)
		switch status.Outcome() {
		case store.OutcomeFailed, store.OutcomeBlocked:
			if failed == (manifest.NamedResource{}) {
				failed = id
			}
			continue
		case store.OutcomeSkipped, store.OutcomeSuspended:
			continue
		case store.OutcomePending, store.OutcomePassed, store.OutcomeUnchanged:
		}
		if q.session != nil {
			q.pending = true
			q.candidates = []manifest.NamedResource{id}
			break
		}
		q.root = id
		evidence := applicationEvidence{object: obj, root: id}
		w.captureOwner(&evidence)
		_, err := w.visit(ctx, &evidence)
		if err != nil || q.pending || w.selectedTargetsReached() {
			return failed, err
		}
	}
	return failed, nil
}

func (w *qualificationWalk) coveredOperand(id manifest.NamedResource) bool {
	if !id.KindInfo().Structural || len(w.q.matches) == 0 {
		return false
	}
	record, _ := w.q.loader.Index.Lookup(id)
	if record.Path == "" || !safepath.Contains(w.admission.input.LocalPath, record.Path) {
		return false
	}
	file := w.admission.input.OriginPath(record.Path)
	for i := range w.q.matches {
		match := &w.q.matches[i]
		owner, ok := match.object.(*manifest.Kustomization)
		if !ok || !match.visibleGeneration || match.source.artifact == nil {
			continue
		}
		output, accepted := acceptedApplicationOutput(w.reads, owner.Named())
		if accepted && slices.ContainsFunc(output.Children, func(child store.Child) bool {
			return child.Obj != nil && child.Obj.Named() == id
		}) && w.q.loader.EntryPackageReached(match.source.artifact, match.application.Path, owner, file) {
			return true
		}
	}
	return false
}

func (w *qualificationWalk) visitPublishedRoots(ctx context.Context) error {
	q := w.q
	roots := q.loader.EntryRoots(q.published(w.store, w.admission), w.reads)
	for i := range roots {
		evidence, err := w.root(ctx, &roots[i])
		if err != nil || q.pending {
			return err
		}
		if evidence == nil {
			continue
		}
		q.root = evidence.object.Named()
		_, err = w.visit(ctx, evidence)
		if err != nil || w.enoughMatches() || q.pending {
			return err
		}
	}
	if q.closed || !w.admission.entryDemand {
		return nil
	}
	inventories, err := w.admission.generatorInventories(ctx)
	if err != nil {
		return err
	}
	for _, frame := range inventories {
		batch := qualificationBatch{children: frame.children, private: true}
		_, err := w.follow(ctx, &batch, manifest.NamedResource{})
		if err != nil || w.enoughMatches() || q.pending {
			return err
		}
	}
	return nil
}

func (a *Admission) generatorInventories(ctx context.Context) ([]*declarationRender, error) {
	// An enclosing aggregate applies its transformations after the nested package builds.
	for _, dir := range slices.Backward(a.generatorPackages) {
		owner := manifest.NewEntryKustomization(manifest.BootstrapSourceID, dir, manifest.NamedResource{})
		frame, err := a.packageInventory(ctx, owner, inputprojection.Authored(a.Input()))
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		if err != nil {
			continue
		}
		var generators []store.Child
		for i := range frame.children {
			child := &frame.children[i]
			if _, applies := manifest.ApplicationFor(child.Obj); applies && !manifest.KindOf(child.Obj).Runnable {
				generators = append(generators, *child)
			}
		}
		a.privateInventories = append(a.privateInventories, &declarationRender{owner: owner, children: generators})
	}
	a.generatorPackages = nil
	return a.privateInventories, ctx.Err()
}

// Physical package members start work without publishing an aggregate.
func (w *qualificationWalk) admitOperandPackage(operands []selection.OccurrenceFacts) {
	var decision *selection.Decision
	if w.q.changedOnly {
		decision = w.q.loader.Options.Session.Snapshot()
	}
	for i := range operands {
		operand := &operands[i]
		obj := occurrenceObject(operand)
		if obj == nil || obj.Suspended() || operand.RequiresBuild {
			continue
		}
		id := obj.Named()
		evaluationOnly := decision.ReadinessDemand(id) && decision.EvaluationOnly(id)
		hasPackageEvidence := operand.Structural != nil || operand.HasPath
		if evaluationOnly && !hasPackageEvidence && len(w.q.loader.ProjectedObjects(obj)) == 0 {
			continue
		}
		if w.store.GetDeclaration(id) == nil {
			w.reads.AdmitOperand(obj, manifest.BootstrapSourceID)
		}
		if manifest.KindOf(obj).Parent {
			w.q.active = append(w.q.active, id)
		}
	}
}
