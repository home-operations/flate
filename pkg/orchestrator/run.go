package orchestrator

import (
	"context"
	"fmt"
	"iter"
	"maps"
	"slices"

	"github.com/home-operations/flate/pkg/controllers/base"
	"github.com/home-operations/flate/pkg/controllers/kustomization"
	sourcectrl "github.com/home-operations/flate/pkg/controllers/source"
	"github.com/home-operations/flate/pkg/loader"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/resourceset"
	"github.com/home-operations/flate/pkg/selection"
	"github.com/home-operations/flate/pkg/store"
)

type orchestratorExistence struct {
	*manifest.DiscoveryIndex

	o *Orchestrator
}

func (e *orchestratorExistence) IsFileIndexed(id manifest.NamedResource) bool {
	if e.o.loader != nil {
		return e.o.loader.IsFileIndexed(id)
	}
	return e.DiscoveryIndex.IsFileIndexed(id)
}

//nolint:gocritic // The admission contract requires an independent request snapshot.
func (e *orchestratorExistence) Promote(
	ctx context.Context,
	request selection.PrepareRequest,
	id manifest.NamedResource,
) (bool, error) {
	if e.o.preparation != nil {
		return e.promotePrepared(ctx, request, id)
	}
	if e.o.selection != nil {
		return e.promoteSelected(ctx, &request, id)
	}
	if object := e.o.store.GetObject(id); object != nil {
		for range selection.Declarations([]store.Child{{Obj: object}}) {
			return true, nil
		}
		return false, nil
	}
	return e.promoteFull(ctx, &request, id)
}

func (e *orchestratorExistence) promoteSelected(
	ctx context.Context,
	request *selection.PrepareRequest,
	id manifest.NamedResource,
) (bool, error) {
	reads := request.Reads
	captured := reads
	if captured == nil {
		captured = &store.ReadSet{}
		captured.Init(e.o.store, id, nil)
		defer captured.Retire()
	}
	request.Reads = captured
	_, err := e.o.ksc.KeepEmitted(ctx, manifest.NamedResource{}, *request)
	if err != nil {
		return false, err
	}
	if captured.GetObject(id) == nil {
		return e.promoteAdmission(ctx, *request, id)
	}
	return e.o.selection.Snapshot().Reconcile(id), nil
}

func (e *orchestratorExistence) promoteFull(
	ctx context.Context,
	request *selection.PrepareRequest,
	id manifest.NamedResource,
) (bool, error) {
	reads := request.Reads
	addWarning := e.o.store.AddWarning
	if reads != nil {
		addWarning = reads.AddWarning
	}
	if reads == nil {
		reads = &store.ReadSet{}
		reads.Init(e.o.store, id, nil)
		defer reads.Retire()
	}
	request.Reads = reads
	productions := reads.Promotions(id, e.o.index)
	for i := range productions {
		production := &productions[i]
		children, err := loader.Promote(ctx, e.o.store, id, loader.PromoteOptions{Record: *production, Reads: reads})
		if err != nil {
			return false, err
		}
		for child := range selection.Declarations(children) {
			if err := ctx.Err(); err != nil {
				return false, fmt.Errorf("context: %w", err)
			}
			if !reads.Valid() {
				return false, nil
			}
			for i := range child.Warnings {
				warning := child.Warnings[i]
				warning.Resource = child.Obj.Named()
				addWarning(warning)
			}
			reads.Admit(child, child.SourceSupport, production.Owner)
		}
		if e.o.store.GetObject(id) != nil {
			reads.Productions(id, e.o.index)
			return true, nil
		}
	}

	return e.promoteAdmission(ctx, *request, id)
}

//nolint:gocritic // The admission contract requires an independent request snapshot.
func (e *orchestratorExistence) promotePrepared(
	ctx context.Context,
	request selection.PrepareRequest,
	id manifest.NamedResource,
) (bool, error) {
	reads := request.Reads
	if err := ctx.Err(); err != nil {
		return false, fmt.Errorf("context: %w", err)
	}
	obj := e.o.store.GetDeclaration(id)
	if reads != nil {
		obj = reads.GetDeclaration(id)
	}
	if obj == nil {
		return e.promoteAdmission(ctx, request, id)
	}
	return slices.Contains(e.o.preparation.Needed, id), nil
}

//nolint:gocritic // The admission contract requires an independent request snapshot.
func (e *orchestratorExistence) promoteAdmission(
	ctx context.Context,
	request selection.PrepareRequest,
	id manifest.NamedResource,
) (bool, error) {
	reads := request.Reads
	if e.o.admission == nil || reads == nil || reads.GetDeclaration(reads.ID()) == nil {
		return false, nil
	}
	return e.o.admission.Publish(ctx, id, reads, func(child store.Child) (bool, error) {
		if !reads.Valid() {
			return false, nil
		}
		request.Generation = slices.Collect(selection.Declarations([]store.Child{child}))
		if len(request.Generation) == 0 {
			return false, nil
		}
		return e.o.ksc.KeepEmitted(ctx, manifest.NamedResource{}, request)
	})
}

func (o *Orchestrator) run(ctx context.Context) error {
	o.cfg.work.Scope().Reserve(len(o.index.Records))
	if o.selection == nil {
		o.configureControllers()
	}
	// Cycle failures MUST publish before scheduler admission of emitted objects.
	unsubCycles := o.store.AddListener(store.EventObjectAdded, func(id manifest.NamedResource, _ any) {
		if !id.KindInfo().Render {
			return
		}
		o.updateDependencyGraphFor(id)
	}, false)
	defer unsubCycles()

	return o.runDAG(ctx)
}

// Configure MUST precede dispatch; controllers freeze shaping config on start.
func (o *Orchestrator) configureControllers() {
	o.src.Configure(sourcectrl.FetchOptions{
		Session:             o.selection,
		Loader:              o.loader,
		AllowMissingSecrets: o.cfg.allowMissingSecrets,
		Admission:           o.admission,
	}, o.cfg.SourceRetry)
	existence := &orchestratorExistence{DiscoveryIndex: o.index, o: o}
	common := base.Options{
		SkipKinds: o.cfg.HelmOptions.SkipKinds,
		Session:   o.selection,
		Loader:    o.loader,
		Index:     o.index,
		Existence: existence,
		Missing:   o.cfg.missing,
	}
	o.ksc.Configure(kustomization.Options{
		Discovery:           o.discoveryResult,
		Options:             common,
		StrictSubstitutions: o.cfg.StrictSubstitutions,
	}, o.cfg.SourceRetry)
	o.hrc.Configure(common)
	o.rsc.Configure(common)
	o.agc.Configure(common)
}

func (o *Orchestrator) collectResult(runErr error) (*Result, error) {
	var warnings []manifest.Warning
	if o.admission != nil {
		warnings = o.admission.Warnings()
	} else {
		warnings = o.store.Warnings()
	}
	res := &Result{
		Work:     o.freezeWork(),
		Failed:   map[manifest.NamedResource]store.StatusInfo{},
		Orphans:  map[manifest.NamedResource]string{},
		Blocked:  map[manifest.NamedResource][]manifest.NamedResource{},
		Warnings: warnings,
	}
	// Completed inventory survives a failed successor; diagnostics remain attached to its input.
	failed := o.store.FailedResources()
	res.Manifests, res.emissions = o.collectCompletedManifests()
	res.catalog = o.loader.DetachCatalog()
	o.index = selection.ProjectIndex(o.index, o.selection.Snapshot())
	o.store.SetDiscoveryIndex(o.index)
	maps.Copy(res.Failed, failed)
	maps.Copy(res.Orphans, o.orphans)
	// Tag the derived (dependency-blocked) failures so a consumer can collapse
	// the cascade under its root cause. A failure with no blockers is primary.
	for id, info := range res.Failed {
		if info.IsDependencyFailure() {
			res.Blocked[id] = info.BlockedBy()
		}
	}
	return res, runErr
}

// Accepted completed output MUST remain available after a failed successor.
func (o *Orchestrator) collectCompletedManifests() ([]manifest.DocumentBatch, []selection.EmissionFacts) {
	var emissions []selection.EmissionFacts
	var objects []manifest.BaseManifest
	for info := range manifest.Kinds() {
		if info.Runnable {
			objects = append(objects, o.store.ListObjects(info.Group, info.Kind)...)
		}
	}
	slices.SortFunc(objects, func(a, b manifest.BaseManifest) int { return a.Named().Compare(b.Named()) })
	out := make([]manifest.DocumentBatch, 0, len(objects))
	positions := make(map[manifest.NamedResource]int, len(objects))
	for _, obj := range objects {
		id := obj.Named()
		parentAttributed := !manifest.KindOf(obj).Render
		parent, eligible := o.collectionParent(obj, id, parentAttributed)
		if !eligible {
			continue
		}
		effective, _, attempt := o.store.GetEmission(id)
		attempt.Finish()
		if effective == nil {
			continue
		}
		var emission selection.EmissionFacts
		if o.selection != nil {
			emission.Emitter, emission.Parent = obj, o.store.GetObject(parent)
		}
		documents := o.collectionDocuments(obj, parentAttributed, &attempt)
		for doc := range documents {
			identity := resourceset.DedupKey(doc)
			if o.selection != nil {
				emission.Documents = append(
					emission.Documents,
					selection.DocumentFact{Document: doc, Identity: identity},
				)
				continue
			}
			position, ok := positions[parent]
			if !ok {
				position = len(out)
				positions[parent] = position
				out = append(out, manifest.DocumentBatch{Parent: o.store.GetObject(parent)})
			}
			out[position].Documents = append(out[position].Documents, doc)
		}
		if len(emission.Documents) != 0 {
			emissions = append(emissions, emission)
		}
	}
	snapshot := selection.Snapshot{Decision: o.selection.Snapshot(), Manifests: out, Emissions: emissions}
	snapshot.CollectEmissions()
	return snapshot.Manifests, emissions
}

func (o *Orchestrator) collectionDocuments(
	obj manifest.BaseManifest,
	parentAttributed bool,
	attempt *store.Emission,
) iter.Seq[map[string]any] {
	return func(yield func(map[string]any) bool) {
		id := obj.Named()
		filterOwnership := emissionOwnership(obj, parentAttributed)
		selected := make(map[manifest.NamedResource]bool, len(attempt.Children))
		o.collectionChildren(id, parentAttributed, attempt.Children, selected)
		for _, occurrence := range attempt.Manifests {
			doc := occurrence.Document
			target := resourceset.DedupKey(doc)
			target, identifiable := collectionDocumentTarget(doc, target, id.Namespace, parentAttributed)
			if filterOwnership && identifiable && !selected[target] &&
				!o.collectionOwnershipAllows(target, parentAttributed) {
				continue
			}
			if !yield(doc) {
				return
			}
		}
	}
}

func (o *Orchestrator) collectionOwnershipAllows(target manifest.NamedResource, parentAttributed bool) bool {
	return !parentAttributed && o.store.EmissionOwner(target) == (manifest.NamedResource{})
}

func collectionDocumentTarget(
	doc map[string]any,
	target manifest.NamedResource,
	namespace string,
	parentAttributed bool,
) (manifest.NamedResource, bool) {
	apiVersion := manifest.DocAPIVersion(doc)
	identifiable := apiVersion != "" && target.Kind != "" && target.Name != "" &&
		target.Group != manifest.KustomizeDomain
	if parentAttributed && identifiable && target.Namespace == "" &&
		manifest.KindOf(&manifest.RawObject{APIVersion: apiVersion, Kind: target.Kind}).Namespaced() {
		target.Namespace = namespace
	}
	return target, identifiable
}

func (o *Orchestrator) collectionChildren(
	id manifest.NamedResource,
	parentAttributed bool,
	children []store.Child,
	selected map[manifest.NamedResource]bool,
) {
	childValues := children
	for childIndex := range childValues {
		child := childValues[childIndex]
		if child.Doc != nil {
			target := child.Obj.Named()
			selected[target] = (!parentAttributed || !manifest.KindOf(child.Obj).Render) &&
				o.store.EmissionOwner(target) == id
		}
	}
}

func (o *Orchestrator) collectionParent(
	obj manifest.BaseManifest,
	id manifest.NamedResource,
	parentAttributed bool,
) (manifest.NamedResource, bool) {
	parent := id
	if parentAttributed {
		var ok bool
		parent, ok = o.parentOf(id)
		if !ok {
			if manifest.KindOf(obj).Parent {
				return parent, false
			}
			parent = id
		}
		if o.store.GetObject(parent) == nil {
			return parent, false
		}
	}
	return parent, true
}

func emissionOwnership(obj manifest.BaseManifest, parentAttributed bool) bool {
	if manifest.KindOf(obj).Parent {
		return parentAttributed || obj.EmissionPolicy().DocumentScope
	}
	return parentAttributed
}

func (o *Orchestrator) stop() {
	o.ksc.Trees.Close()
	o.inputs.Close()
	o.tasks.BlockTillDone()
	o.sweepProbes(true)
	o.preparation = nil
	o.admissionPreparation.Retire()
	o.admissionPreparation = nil
	o.discoveryResult.Close()
	o.discoveryResult = nil
	o.session.Close()
	o.loader.Close()
	if o.unsubStatus != nil {
		o.unsubStatus()
		o.unsubStatus = nil
	}
	o.cfg.StatusListener = nil
}
