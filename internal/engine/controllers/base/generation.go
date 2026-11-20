package base

import (
	"context"
	"slices"

	"github.com/home-operations/flate/internal/engine/kustomize/patch"
	"github.com/home-operations/flate/internal/engine/store"
	"github.com/home-operations/flate/pkg/manifest"
)

type preparedGeneration struct {
	object manifest.BaseManifest
	output store.ChildDocuments
}

func (e *Execution) prepare(ctx context.Context, obj manifest.BaseManifest) error {
	e.prepared = true
	if !e.generationKnown {
		output, err := e.generate(obj)
		if ctx.Err() != nil {
			return ctx.Err()
		}
		e.generationKnown, e.generated, e.generationErr = true, output, err
	}
	if e.generationErr != nil {
		return e.generationErr
	}
	if len(e.generated.Children) == 0 && !e.Store.HasGeneratedObjects(e.Reads.ID()) {
		return nil
	}
	_, _, inventory := e.Reads.GetEmission(e.Reads.ID())
	defer inventory.Finish()
	inventory.Children, inventory.Manifests, inventory.Complete = e.generated.Children, e.generated.Visible, false
	return e.Emit(ctx, e.Reads.ID(), &inventory, nil)
}

func (e *Execution) generate(object manifest.BaseManifest) (store.ChildDocuments, error) {
	scope := e.Reads.Work().Scope()
	output, err := patch.PrepareGeneration(object, scope)
	if err != nil || len(output.Children) != 0 {
		work := scope.Record(manifest.WorkPreparation, "generation")
		work.Add(&manifest.WorkCounts{Attempts: 1, OwnerPrepares: 1})
		work.Finish(err)
	}
	return output, err
}

func (e *Execution) generatedChildren(ctx context.Context, next *store.Emission) error {
	if err := e.prepareChildrenGeneration(ctx, next); err != nil {
		return err
	}
	if len(e.generated.Children) == 0 && len(e.childGeneration) == 0 &&
		!slices.ContainsFunc(next.Children, func(child store.Child) bool { return child.Generated }) {
		return nil
	}

	children := make([]store.Child, 0, len(next.Children)+len(e.generated.Children))
	for i := range next.Children {
		child := &next.Children[i]
		if !child.Generated {
			children = append(children, *child)
		}
	}
	next.Manifests = generationDocuments(next.Manifests, next.Children)
	ordinary := len(children)
	var visible []manifest.DocOccurrence
	var retained map[manifest.NamedResource]preparedGeneration
	producer := manifest.Production{Owner: e.Reads.ID()}
	if len(e.childGeneration) != 0 {
		producer.Source = e.generationSource()
	}
	for i := range children {
		child := &children[i]
		if child.Obj == nil {
			continue
		}
		id := child.Obj.Named()
		generation, ok := e.childGeneration[id]
		if !ok || !manifest.Equal(generation.object, child.Obj) {
			continue
		}
		production := producer
		production.Declaration = id
		evidence := []manifest.Production{production}
		for j := range generation.output.Children {
			generated := generation.output.Children[j]
			generated.Productions = evidence
			children = append(children, generated)
		}
		visible = append(visible, generation.output.Visible...)
		next.Failures = append(next.Failures, generation.output.Failures...)
		if retained == nil {
			retained = make(map[manifest.NamedResource]preparedGeneration, len(e.childGeneration))
		}
		retained[id] = generation
	}
	if next.Complete {
		e.childGeneration = retained
	}
	children = append(children, e.generated.Children...)
	next.Children = children
	if len(visible)+len(e.generated.Visible) != 0 {
		// A producer's own document survives replay even though the store omits its self-edge.
		next.Manifests = generationDocuments(next.Manifests, children[ordinary:])
		next.Manifests = slices.Concat(next.Manifests, visible, e.generated.Visible)
	}
	return nil
}

func generationDocuments(documents []manifest.DocOccurrence, children []store.Child) []manifest.DocOccurrence {
	owned := false
	for i := range children {
		child := &children[i]
		if !child.VisibleGeneration || child.Doc == nil {
			continue
		}
		if !owned {
			documents, owned = slices.Clone(documents), true
		}
		documents = slices.DeleteFunc(documents, func(document manifest.DocOccurrence) bool {
			return manifest.ValueEqual(document.Document, child.Doc)
		})
	}
	return documents
}

func (e *Execution) prepareChildrenGeneration(ctx context.Context, next *store.Emission) error {
	for i := range next.Children {
		child := &next.Children[i]
		if child.Generated || child.Obj == nil || manifest.KindOf(child.Obj).Runnable {
			continue
		}
		if err := e.prepareEffectiveGeneration(ctx, child.Obj); err != nil {
			return err
		}
	}
	return nil
}

func (e *Execution) prepareEffectiveGeneration(ctx context.Context, object manifest.BaseManifest) error {
	id := object.Named()
	if retained, ok := e.childGeneration[id]; ok && manifest.Equal(retained.object, object) {
		return nil
	}
	output, err := e.effectiveGeneration(object)
	if err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if output.Children == nil {
		return nil
	}
	if e.childGeneration == nil {
		e.childGeneration = map[manifest.NamedResource]preparedGeneration{}
	}
	e.childGeneration[id] = preparedGeneration{object: object, output: output}
	return nil
}

func (e *Execution) effectiveGeneration(object manifest.BaseManifest) (store.ChildDocuments, error) {
	if e.preparedGeneration != nil {
		if output, found, err := e.preparedGeneration(object); found {
			return output, err
		}
	}
	return e.generate(object)
}

func (e *Execution) generationSource() manifest.NamedResource {
	if e.declaration == nil {
		return manifest.NamedResource{}
	}
	for ref := range manifest.References(e.declaration, manifest.ReferenceRelations) {
		if ref.BindsSource() {
			return ref.NamedResource
		}
	}
	source, _, _ := e.declaration.SourcePath()
	return source
}
