package orchestrator

import (
	"context"
	"maps"
	"slices"

	"github.com/home-operations/flate/pkg/controllers/base"
	"github.com/home-operations/flate/pkg/controllers/kustomization"
	sourcectrl "github.com/home-operations/flate/pkg/controllers/source"
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

func (e *orchestratorExistence) Promote(ctx context.Context, reads *store.ReadSet, id manifest.NamedResource) (bool, error) {
	if e.o.preparation != nil {
		if err := ctx.Err(); err != nil {
			return false, err
		}
		obj := e.o.store.GetDeclaration(id)
		if reads != nil {
			obj = reads.GetDeclaration(id)
		}
		return obj != nil && slices.Contains(e.o.preparation.Needed, id), nil
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
	for _, p := range productions {
		admit := func(obj manifest.BaseManifest) (bool, error) {
			return e.admit(ctx, store.Child{Obj: obj, Productions: []manifest.Production{p}}, reads)
		}
		promoted, err := loader.Promote(ctx, e.o.store, id, loader.PromoteOptions{Record: p, Reads: reads, WipeSecrets: e.o.cfg.WipeSecrets, Admit: admit})
		if err != nil {
			return false, err
		}
		if promoted {
			if reads != nil {
				reads.Productions(id, e.o.index)
			}
			return true, nil
		}
	}
	if e.o.admission != nil && reads != nil && reads.GetDeclaration(reads.ID()) != nil {
		witness := reads.GetDeclaration(reads.ID())
		object, artifact, err := e.o.admission.Resolve(ctx, id, nil, []manifest.BaseManifest{witness})
		if err == nil && object != nil && artifact != nil && reads.Valid() {
			accepted, admissionErr := e.admit(ctx, store.Child{Obj: object}, reads)
			if admissionErr != nil {
				return false, admissionErr
			}
			if accepted {
				e.o.store.AddObject(object)
				e.o.store.SetArtifact(id, artifact)
				e.o.store.UpdateStatus(id, store.StatusInfo{Status: store.StatusReady, Message: "local entry source"})
				return true, nil
			}
		}
	}
	return false, nil
}

func (e *orchestratorExistence) admit(ctx context.Context, child store.Child, reads *store.ReadSet) (bool, error) {
	info := manifest.KindOf(child.Obj)
	if !info.Tracked || info.Group != manifest.SourceDomain && info.Runnable {
		return false, nil
	}
	return e.o.ksc.KeepEmitted(ctx, manifest.NamedResource{}, child, reads)
}

func (o *Orchestrator) run(ctx context.Context) error {
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
		parent := id
		parentAttributed := !manifest.KindOf(obj).Render
		if parentAttributed {
			var ok bool
			parent, ok = o.parentOf(id)
			if !ok {
				if manifest.KindOf(obj).Parent {
					continue
				}
				parent = id
			}
			if o.store.GetObject(parent) == nil {
				continue
			}
		}
		effective, _, attempt := o.store.GetEmission(id)
		attempt.Finish()
		if effective == nil {
			continue
		}
		documentScope := false
		filterOwnership := parentAttributed
		if producer, ok := obj.(interface {
			EmissionPolicy() manifest.EmissionPolicy
		}); ok {
			documentScope = producer.EmissionPolicy().DocumentScope
			filterOwnership = filterOwnership || documentScope
		}
		selected := make(map[manifest.NamedResource]bool, len(attempt.Children))
		for _, child := range attempt.Children {
			if child.Doc != nil {
				target := child.Obj.Named()
				selected[target] = (!parentAttributed || !manifest.KindOf(child.Obj).Render) && o.store.EmissionOwner(target) == id
			}
		}
		for _, occurrence := range attempt.Manifests {
			doc := occurrence.Document
			target := resourceset.DedupKey(doc)
			if o.filter != nil && !documentScope {
				_, ownerSelected, _, _ := o.filter.Membership(id)
				if !ownerSelected && !o.filter.ShouldReconcile(target) {
					continue
				}
			}
			apiVersion := manifest.DocAPIVersion(doc)
			identifiable := apiVersion != "" && target.Kind != "" && target.Name != "" && target.Group != manifest.KustomizeDomain
			if parentAttributed && identifiable && target.Namespace == "" && manifest.KindOf(&manifest.RawObject{APIVersion: apiVersion, Kind: target.Kind}).Namespaced() {
				target.Namespace = id.Namespace
			}
			if !filterOwnership || !identifiable || selected[target] || !parentAttributed && o.store.EmissionOwner(target) == (manifest.NamedResource{}) {
				position, ok := positions[parent]
				if !ok {
					position = len(out)
					positions[parent] = position
					out = append(out, manifest.DocumentBatch{Parent: o.store.GetObject(parent)})
				}
				out[position].Documents = append(out[position].Documents, doc)
			}
		}
	}
	return out
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
