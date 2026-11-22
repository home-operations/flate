package orchestrator

import (
	"cmp"
	"context"

	"github.com/home-operations/flate/internal/engine/change"
	"github.com/home-operations/flate/internal/engine/kustomize"
	"github.com/home-operations/flate/internal/engine/selection"
	"github.com/home-operations/flate/internal/engine/store"
	"github.com/home-operations/flate/pkg/manifest"
)

type kustomizeInputPair [2]*kustomizeInputSide

type kustomizeInputSide struct {
	store    *store.Store
	indexed  map[manifest.NamedResource]kustomizeInputDeclaration
	prepared map[manifest.NamedResource]*kustomize.LocalInputs
}

type kustomizeInputDeclaration struct {
	object    manifest.BaseManifest
	published bool
}

func (o *Orchestrator) bindKustomizeInputs(ctx context.Context, peer *Orchestrator, changes *change.Set) {
	if !changes.Paired() {
		return
	}
	artifacts := make(map[manifest.NamedResource][2]*store.SourceArtifact)
	for id := range changes.Sources() {
		delta, inputs, _ := changes.ForSource(id)
		if id != manifest.BootstrapSourceID && delta.Len() != 0 {
			if _, generated := changes.CompareSource(id, manifest.TreeBase, inputs[0], nil); generated {
				artifacts[id] = inputs
			}
		}
	}
	var pair kustomizeInputPair
	for _, current := range [2]*Orchestrator{o, peer} {
		side := current.cfg.ID.Side
		pair[side] = current.prepareKustomizeInputs(ctx, artifacts)
		current.kustomizeInputs = pair[side]
	}
	o.selection.BindKustomizeInputs(pair.read)
	peer.selection.BindKustomizeInputs(pair.read)
}

func (p *kustomizeInputPair) read(
	ctx context.Context, id manifest.NamedResource, object manifest.BaseManifest, facts *selection.SourceFacts,
) ([2]selection.SourceReadPaths, bool) {
	var paths [2]selection.SourceReadPaths
	complete := true
	for side, captured := range p {
		witness, known := object, object != nil
		if manifest.TreeSide(side) != facts.Context.Side || object == nil {
			witness, known = captured.declaration(id)
		}
		ks, ok := witness.(*manifest.Kustomization)
		if !ok || ks.IsFragment() || ks.EffectiveSourceID() != facts.Source {
			complete = complete && known
			continue
		}
		input := facts.Snapshots[side]
		if input == nil || !input.Reads.Valid() {
			complete = false
			continue
		}
		prepared := captured.observe(ctx, ks, input)
		if input.Reads.Valid() {
			paths[side] = selection.SourceReadPaths{Files: prepared.Files, Roots: prepared.Roots}
		}
		complete = complete && prepared.Complete && input.Reads.Valid()
	}
	return paths, complete
}

func (s *kustomizeInputSide) declaration(id manifest.NamedResource) (manifest.BaseManifest, bool) {
	if object := s.store.GetObject(id); object != nil {
		return object, true
	}
	indexed, found := s.indexed[id]
	if indexed.published {
		return nil, false
	}
	return indexed.object, !found || indexed.object != nil
}

func (s *kustomizeInputSide) observe(
	ctx context.Context, ks *manifest.Kustomization, input *store.SourceArtifact,
) *kustomize.LocalInputs {
	if s != nil {
		if prepared := s.prepared[ks.Named()]; prepared.Captures(ks, input) {
			return prepared
		}
	}
	return kustomize.ObserveLocalInputs(ctx, &kustomize.RenderRequest{Source: input, Spec: ks, SubPath: ks.Path})
}

func (o *Orchestrator) prepareKustomizeInputs(
	ctx context.Context, artifacts map[manifest.NamedResource][2]*store.SourceArtifact,
) *kustomizeInputSide {
	snapshot := &kustomizeInputSide{store: o.store}
	if len(artifacts) == 0 || o.index == nil {
		return snapshot
	}
	snapshot.indexed = make(map[manifest.NamedResource]kustomizeInputDeclaration)
	snapshot.prepared = make(map[manifest.NamedResource]*kustomize.LocalInputs)
	for id, record := range o.index.Records { //nolint:gocritic // Each independent record supplies one immutable declaration.
		if !id.KindInfo().Structural {
			continue
		}
		declaration := o.indexedKustomization(id, &record)
		snapshot.indexed[id] = declaration
		if ks, ok := declaration.object.(*manifest.Kustomization); ok && !ks.IsFragment() {
			if input := artifacts[ks.EffectiveSourceID()][o.cfg.ID.Side]; input != nil && input.FS != nil {
				snapshot.prepared[id] = o.kustomizeInputs.observe(ctx, ks, input)
			}
		}
	}
	return snapshot
}

func (o *Orchestrator) indexedKustomization(
	id manifest.NamedResource, record *manifest.DiscoveryRecord,
) kustomizeInputDeclaration {
	if current := o.store.GetObject(id); current != nil {
		return kustomizeInputDeclaration{object: current, published: true}
	}
	object := cmp.Or(o.loader.AdmissionCandidate(id).Obj, record.Declaration)
	if object == nil {
		if occurrence, _ := o.loader.Occurrence(id); occurrence.Located {
			if candidate := cmp.Or(occurrence.Object, occurrence.Catalog); candidate != nil {
				object = manifest.CatalogObject(candidate, id)
			}
		}
	}
	return kustomizeInputDeclaration{object: object}
}
