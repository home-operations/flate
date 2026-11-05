package orchestrator

import (
	"context"
	"fmt"
	"maps"
	"slices"

	"github.com/home-operations/flate/pkg/controllers/base"
	"github.com/home-operations/flate/pkg/controllers/kustomization"
	sourcectrl "github.com/home-operations/flate/pkg/controllers/source"
	"github.com/home-operations/flate/pkg/discovery"
	"github.com/home-operations/flate/pkg/loader"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/resourceset"
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

func (e *orchestratorExistence) Promote(
	ctx context.Context,
	reads *store.ReadSet,
	id manifest.NamedResource,
) (bool, error) {
	if e.o.preparation != nil {
		return e.promotePrepared(ctx, reads, id)
	}
	if e.o.loader != nil {
		captured := reads
		if captured == nil {
			captured = &store.ReadSet{}
			captured.Init(e.o.store, id, nil)
			defer captured.Retire()
		}
		_, err := e.o.ksc.KeepEmitted(ctx, manifest.NamedResource{}, store.Child{}, captured)
		if err != nil {
			return false, err
		}
		return e.o.filter.ShouldReconcile(id) && captured.GetObject(id) != nil, nil
	}
	obj := e.o.store.GetObject(id)
	var productions []manifest.Production
	if obj == nil || e.o.filter.Enabled() {
		if reads != nil {
			productions = reads.Promotions(id, e.o.index)
		} else {
			productions = e.o.store.Productions(id, e.o.index)
		}
	}
	if obj != nil {
		return e.admit(ctx, store.Child{Obj: obj, Productions: productions}, reads)
	}
	{
		pValues := productions
		for pIndex := range pValues {
			p := pValues[pIndex]

			admit := func(obj manifest.BaseManifest) (bool, error) {
				return e.admit(ctx, store.Child{Obj: obj, Productions: []manifest.Production{p}}, reads)
			}
			promoted, err := loader.Promote(
				ctx,
				e.o.store,
				id,
				loader.PromoteOptions{Record: p, Reads: reads, Admit: admit},
			)
			if err != nil {
				return false, err
			}
			if !promoted {
				continue
			}
			if reads != nil {
				reads.Productions(id, e.o.index)
			}
			return true, nil
		}
	}
	return e.promoteAdmission(ctx, reads, id)
}

func (e *orchestratorExistence) promotePrepared(
	ctx context.Context,
	reads *store.ReadSet,
	id manifest.NamedResource,
) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, fmt.Errorf("context: %w", err)
	}
	obj := e.o.store.GetDeclaration(id)
	if reads != nil {
		obj = reads.GetDeclaration(id)
	}
	return obj != nil && slices.Contains(e.o.preparation.Needed, id), nil
}

func (e *orchestratorExistence) promoteAdmission(
	ctx context.Context,
	reads *store.ReadSet,
	id manifest.NamedResource,
) (bool, error) {
	if e.o.admission == nil || reads == nil || reads.GetDeclaration(reads.ID()) == nil {
		return false, nil
	}
	resolved, err := e.o.admission.Resolve(ctx, reads, discovery.ResolveRequest{ID: id, Work: reads.Work().Scope()})
	if err != nil {
		return false, err
	}
	if resolved.Resolve == nil {
		return false, nil
	}
	artifact, err := resolved.Resolve(ctx, manifest.DrainNone)
	if err == nil && resolved.Object != nil && artifact != nil && reads.Valid() {
		accepted, admissionErr := e.admit(ctx, store.Child{Obj: resolved.Object}, reads)
		if admissionErr != nil {
			return false, admissionErr
		}
		if accepted {
			e.o.store.AddObject(resolved.Object)
			e.o.store.SetArtifact(id, artifact)
			e.o.store.UpdateStatus(id, store.StatusInfo{Status: store.StatusReady, Message: "local entry source"})
			return true, nil
		}
	}
	return false, nil
}

//nolint:gocritic // Immutable value inputs preserve snapshot semantics.
func (e *orchestratorExistence) admit(ctx context.Context, child store.Child, reads *store.ReadSet) (bool, error) {
	info := manifest.KindOf(child.Obj)
	if !info.Tracked || info.Group != manifest.SourceDomain && info.Runnable {
		return false, nil
	}
	return e.o.ksc.KeepEmitted(ctx, manifest.NamedResource{}, child, reads)
}

func (o *Orchestrator) run(ctx context.Context) error {
	o.cfg.work.Scope().Reserve(len(o.index.Records))
	if o.loader == nil {
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
		Filter:              o.filter,
		Loader:              o.loader,
		AllowMissingSecrets: slices.Contains(o.cfg.AllowMissingKinds, manifest.KindSecret),
		Admission:           o.admission,
	}, o.cfg.SourceRetry)
	existence := &orchestratorExistence{DiscoveryIndex: o.index, o: o}
	common := base.Options{
		SkipKinds:               o.cfg.HelmOptions.SkipResourceKinds(),
		DependencySubstitutions: o.dependencySubstitutions,
		Filter:                  o.filter,
		Loader:                  o.loader,
		Index:                   o.index,
		Existence:               existence,
		PreflightFailure:        o.preflightFailure,
		AllowMissingKinds:       o.cfg.AllowMissingKinds,
	}
	o.ksc.Configure(kustomization.Options{
		Options:             common,
		StrictSubstitutions: o.cfg.StrictSubstitutions,
	}, o.cfg.SourceRetry)
	o.hrc.Configure(common)
	o.rsc.Configure(common)
	o.agc.Configure(common)
}

func (o *Orchestrator) collectResult(runErr error) (*Result, error) {
	res := &Result{
		Work:    o.freezeWork(),
		Failed:  map[manifest.NamedResource]store.StatusInfo{},
		Orphans: map[manifest.NamedResource]string{},
		Blocked: map[manifest.NamedResource][]manifest.NamedResource{},
		// Render advisories collected by producers during Run (HR stale values,
		// empty scan, …). Nil when none, sorted deterministically by the store.
		Warnings: o.store.Warnings(),
	}
	// Completed inventory survives a failed successor; diagnostics remain attached to its input.
	failed := o.store.FailedResources()
	res.Manifests = o.collectManifests()
	maps.Copy(res.Failed, failed)
	maps.Copy(res.Orphans, o.orphans)
	// Tag the derived (dependency-blocked) failures so a consumer can collapse
	// the cascade under its root cause. A failure with no blockers is primary.
	for id, info := range res.Failed {
		if info.IsDependencyFailure() {
			res.Blocked[id] = info.BlockedBy()
		}
	}
	if o.loader != nil {
		o.loader.Retire()
	}

	return res, runErr
}

// collectManifests filters completed output without changing stored documents needed by value consumers.
// Accepted completed output MUST remain available after a failed successor.
func (o *Orchestrator) collectManifests() []manifest.DocumentBatch {
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
		ownership := emissionOwnership(obj, parentAttributed)
		documentScope, filterOwnership := ownership.document, ownership.filter
		selected := make(map[manifest.NamedResource]bool, len(attempt.Children))
		o.selectCollectionChildren(id, parentAttributed, attempt.Children, selected)
		for _, occurrence := range attempt.Manifests {
			doc := occurrence.Document
			target := resourceset.DedupKey(doc)
			if !o.collectionDocumentSelected(id, target, documentScope) {
				continue
			}
			target, identifiable := collectionDocumentTarget(doc, target, id.Namespace, parentAttributed)
			if filterOwnership && identifiable && !selected[target] &&
				!o.collectionOwnershipAllows(target, parentAttributed) {
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
	}
	return out
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

func (o *Orchestrator) selectCollectionChildren(
	id manifest.NamedResource,
	parentAttributed bool,
	children []store.Child,
	selected map[manifest.NamedResource]bool,
) {
	for _, child := range children {
		if child.Doc != nil {
			target := child.Obj.Named()
			selected[target] = (!parentAttributed || !manifest.KindOf(child.Obj).Render) &&
				o.store.EmissionOwner(target) == id
		}
	}
}

func (o *Orchestrator) collectionDocumentSelected(id, target manifest.NamedResource, documentScope bool) bool {
	if o.filter != nil && !documentScope {
		ownerSelected := o.filter.Membership(id).Selected
		if !ownerSelected && !o.filter.ShouldReconcile(target) {
			return false
		}
	}
	return true
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

type emissionScope struct {
	document bool
	filter   bool
}

func emissionOwnership(obj manifest.BaseManifest, parentAttributed bool) emissionScope {
	documentScope := false
	filterOwnership := parentAttributed
	if producer, ok := obj.(interface {
		EmissionPolicy() manifest.EmissionPolicy
	}); ok {
		documentScope = producer.EmissionPolicy().DocumentScope
		filterOwnership = filterOwnership || documentScope
	}
	return emissionScope{document: documentScope, filter: filterOwnership}
}

func (o *Orchestrator) stop() {
	o.ksc.Trees.Close()
	o.inputs.Close()
	if o.unsubStatus != nil {
		o.unsubStatus()
		o.unsubStatus = nil
	}
	o.cfg.StatusListener = nil
}
