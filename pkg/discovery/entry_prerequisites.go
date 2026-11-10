package discovery

import (
	"path/filepath"
	"slices"

	"github.com/home-operations/flate/pkg/selection"

	"github.com/home-operations/flate/pkg/loader"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

type entryPrerequisites struct {
	loader   *loader.Loader
	reads    *store.ReadSet
	owner    *manifest.Kustomization
	children []store.Child
	batch    map[manifest.NamedResource]manifest.BaseManifest
	roots    []selection.OccurrenceFacts
	seen     map[manifest.NamedResource]bool
	source   manifest.NamedResource
}

func (e *entryPrerequisites) eligible(owner *manifest.Kustomization, children []store.Child) bool {
	if len(owner.DependsOn) == 0 && (owner.PostBuild == nil || len(owner.PostBuild.SubstituteFrom) == 0) {
		return true
	}
	e.owner, e.children = owner, children
	e.roots = e.loader.EntryRoots(nil, e.reads)
	e.batch = make(map[manifest.NamedResource]manifest.BaseManifest, len(children))
	for i := range children {
		child := &children[i]
		e.batch[child.Obj.Named()] = child.Obj
	}
	e.seen = map[manifest.NamedResource]bool{}

	e.source = owner.EffectiveSourceID()
	return !e.prerequisites(owner)
}

func (e *entryPrerequisites) prerequisites(obj manifest.BaseManifest) bool {
	for ref := range manifest.References(obj, manifest.ReferenceRelations) {
		if ref.ContentInput() && ref.ArtifactStages == 0 && !ref.Acquisition {
			facts := e.reads.ReferenceFacts(ref.NamedResource, e.loader.Index, obj.Named())
			if facts.Data != nil {
				continue
			}
			_, _, producerAbsence := e.loader.Options.Missing.Missing(ref, facts)
			if _, accepted := acceptedApplicationOutput(e.reads, obj.Named()); accepted && producerAbsence {
				continue
			}
			if !ref.Optional {
				return true
			}
		}
		if (ref.Ordering || ref.ContentInput() || ref.ArtifactStages != 0 && e.reads.GetArtifact(ref.NamedResource, store.ArtifactContent) == nil) &&
			e.captured(ref.NamedResource) {
			return true
		}
	}
	return false
}

func (e *entryPrerequisites) captured(id manifest.NamedResource) bool {
	if id.Name == "" || id.Kind == "" || id == e.owner.Named() {
		return false
	}
	if previous, known := e.seen[id]; known {
		return previous
	}
	e.seen[id] = false
	result := e.captureDependency(id)
	e.seen[id] = result
	return result
}

func (e *entryPrerequisites) captureDependency(id manifest.NamedResource) bool {
	producer := e.reads.GetDeclaration(id)
	if producer == nil {
		producer = e.loader.CatalogObject(id)
	}
	var application manifest.EntryApplication
	applies := false
	if producer != nil {
		application.Source, application.Path, applies = producer.SourcePath()
		if application.Source.Name == "" || application.Source.Kind == "" {
			application.Source = manifest.BootstrapSourceID
		}
	}
	input, _ := e.reads.GetArtifact(e.source, store.ArtifactContent).(*store.SourceArtifact)
	if applies && input != nil && loader.AppliesToEntry(
		e.reads, &application, input, filepath.Join(input.LocalPath, manifest.NormalizeClaimBase(e.owner.Path)),
	) {
		return true
	}
	if e.exposed(id) {
		return false
	}
	if e.batch[id] != nil {
		return true
	}
	for i := range e.children {
		child := &e.children[i]
		if slices.ContainsFunc(manifest.ProducerTargets(child.Obj, id), func(target manifest.ProducerTarget) bool {
			return target.NamedResource == id
		}) {
			return true
		}
	}
	obj := e.reads.GetObject(id)
	if obj == nil {
		obj = e.loader.Index.Records[id].Declaration
	}
	if obj != nil && e.prerequisites(obj) {
		return true
	}
	return e.capturesProduction(id)
}

func (e *entryPrerequisites) capturesProduction(id manifest.NamedResource) bool {
	productions := e.reads.Productions(id, e.loader.Index)
	for i := range productions {
		production := &productions[i]
		if production.Source != (manifest.NamedResource{}) && production.Source != e.source {
			continue
		}
		if production.Owner == e.owner.Named() || e.captured(production.Materializer(id)) ||
			e.captured(production.Declaration) {
			return true
		}
	}
	return false
}

func (e *entryPrerequisites) exposed(id manifest.NamedResource) bool {
	if input, ok := e.reads.GetArtifact(id, store.ArtifactContent).(*store.SourceArtifact); ok && input.Reads.Valid() {
		return true
	}
	if object, policy := e.reads.Lookup(id); object != nil && policy.Origin == manifest.SuppliedDeclaration &&
		e.reads.GetDeclaration(id) != nil {
		return true
	}
	for i := range e.roots {
		if e.roots[i].Identity == id {
			return true
		}
	}
	object := e.reads.GetDeclaration(id)
	if object == nil {
		return false
	}
	productions := e.reads.Productions(id, e.loader.Index)
	for i := range productions {
		if productions[i].Owner == (manifest.NamedResource{}) {
			continue
		}
		if e.reads.Policy(id).Reconcile &&
			(productions[i].Owner == e.owner.Named() || e.captured(productions[i].Owner)) {
			continue
		}
		evidence := applicationEvidence{object: object, owner: productions[i].Owner}
		if accepted, _ := evidence.ownerAccepts(e.reads); accepted {
			return true
		}
	}
	return false
}
