// Package artifactgenerator evaluates ArtifactGenerators and publishes their ExternalArtifacts.
package artifactgenerator

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io/fs"
	"maps"
	"strings"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/home-operations/flate/pkg/controllers/base"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/store"
	"github.com/home-operations/flate/pkg/task"
)

const generatedLabelCount = 2

// Controller evaluates generated sources through the shared publication lifecycle.
type Controller struct {
	*base.Controller

	cache *cache.Cache
}

// New constructs an ArtifactGenerator controller.
func New(s *store.Store, tasks *task.Service, owner *cache.Cache) *Controller {
	return &Controller{Controller: base.New(s, tasks), cache: owner}
}

type execution struct {
	*Controller

	run base.Execution
}

// Owns reports whether id belongs to the ArtifactGenerator controller.
func (*Controller) Owns(id manifest.NamedResource) bool {
	return id.Group == "source.extensions.fluxcd.io" && id.Kind == "ArtifactGenerator"
}

// ReconcileNode returns the captured assessment of ArtifactGenerator evaluation.
func (c *Controller) ReconcileNode(
	ctx context.Context,
	id manifest.NamedResource,
	level manifest.DrainLevel,
) *base.Execution {
	ex := &execution{Controller: c}
	ex.run.Init(c.Controller, id)
	ex.run.Dispatch(ctx, level, ex, (*execution).reconcile)
	return &ex.run
}

type inputIdentity struct {
	Alias  string
	Source manifest.NamedResource
	Input  store.ArtifactInput
}
type evaluation struct {
	owner    *cache.Cache
	inputs   map[string]*store.SourceArtifact
	identity []inputIdentity
	maxAge   time.Duration
}

func (c *execution) reconcile(ctx context.Context, obj *manifest.RawObject, level manifest.DrainLevel) error {
	id := obj.Named()
	selected, doc, attempt := c.run.Reads.GetEmission(id)
	defer attempt.Finish()
	if selected == nil {
		return nil
	}
	raw, err := validateGenerator(id, selected, doc)
	if err != nil {
		return err
	}
	uid, disabled, consumes := raw.ReconcileInput(doc)
	if !consumes {
		return manifest.NewError(manifest.ErrInput, nil, "expected ArtifactGenerator document")
	}
	if disabled {
		return nil
	}
	spec, err := decodeSpec(raw.Spec)
	if err != nil {
		return manifest.NewError(manifest.ErrInput, fmt.Errorf("generator spec: %w", err), "")
	}
	raw, err = c.run.RequireRefresh(ctx, raw, manifest.GateArtifacts, level)
	if err != nil {
		return err
	}
	localEvaluation, views := c.generatorInputs(raw, &spec)
	content := struct {
		Generator manifest.NamedResource
		Spec      specification
		UID       string
		Disabled  bool
		Inputs    []inputIdentity
	}{id, spec, uid, disabled, localEvaluation.identity}
	var replayContent any = content
	if localEvaluation.maxAge == 0 {
		replayContent = nil
	}
	fingerprint, handled, err := c.run.FingerprintDedup(ctx, id, replayContent, &store.Emission{})
	if handled || err != nil {
		return err
	}
	requests, err := expand(ctx, spec, views)
	if err != nil {
		return manifest.NewError(manifest.ErrInput, fmt.Errorf("expand outputs: %w", err), "")
	}
	publication := generatorPublication{
		execution:  c,
		id:         id,
		raw:        raw,
		spec:       &spec,
		uid:        uid,
		evaluation: &localEvaluation,
		attempt:    &attempt,
	}
	for _, req := range requests {
		if err := ctx.Err(); err != nil {
			return fmt.Errorf("context: %w", err)
		}
		if err := publication.publish(ctx, &req); err != nil {
			return err
		}
	}
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("context: %w", err)
	}
	attempt.Children, attempt.Manifests, attempt.Complete, attempt.Fingerprint = publication.desired, publication.documents, true, fingerprint
	return c.run.Emit(ctx, id, &attempt, nil)
}

func validateGenerator(
	id manifest.NamedResource,
	selected manifest.BaseManifest,
	doc map[string]any,
) (*manifest.RawObject, error) {
	raw, ok := selected.(*manifest.RawObject)
	if !ok {
		return nil, manifest.NewError(manifest.ErrInput, nil, "expected ArtifactGenerator document")
	}
	if doc == nil {
		return nil, manifest.NewError(manifest.ErrInput, nil, "missing authoritative document")
	}
	parsed, _, _, err := manifest.ParseDoc(doc, manifest.ParseDocOptions{})
	if err != nil {
		return nil, fmt.Errorf("parse generator: %w", err)
	}
	authored, ok := parsed.(*manifest.RawObject)
	if !ok || authored.Named().Group != id.Group || authored.Kind != raw.Kind || authored.Name != raw.Name {
		return nil, manifest.NewError(manifest.ErrInput, nil, "mismatched authoritative document")
	}
	actual, err := json.Marshal(raw.Spec)
	if err != nil {
		return nil, manifest.NewError(manifest.ErrInput, fmt.Errorf("marshal generator spec: %w", err), "")
	}
	expected, err := json.Marshal(authored.Spec)
	if err != nil || !bytes.Equal(actual, expected) {
		return nil, manifest.NewError(manifest.ErrInput, nil, "mismatched generator spec")
	}
	return raw, nil
}

func (c *execution) generatorInputs(raw *manifest.RawObject, spec *specification) (evaluation, map[string]fs.FS) {
	localEvaluation := evaluation{
		owner:  c.cache,
		inputs: make(map[string]*store.SourceArtifact, len(spec.Sources)),
		maxAge: cache.Forever,
	}
	views := make(map[string]fs.FS, len(spec.Sources))
	selections := generatorReadSelections(spec)
	for ref := range manifest.References(raw, manifest.ReferenceRelations) {
		alias := spec.Sources[ref.Position].Alias
		read := store.ArtifactContent | selections[alias]
		artifact := c.run.Reads.GetArtifact(ref.NamedResource, read).(*store.SourceArtifact) //nolint:errcheck // Required source dependencies resolve source artifacts before reconciliation.
		if read&store.ArtifactOrigin != 0 && artifact.Metadata["org.opencontainers.image.revision"] == "" {
			read |= store.ArtifactRevision
			c.run.Reads.GetArtifact(ref.NamedResource, read)
		}
		localEvaluation.inputs[alias], views[alias] = artifact, artifact.FS
		localEvaluation.identity = append(
			localEvaluation.identity,
			inputIdentity{Alias: alias, Source: ref.NamedResource, Input: artifact.Input(read)},
		)
		if artifact.Content == "" {
			localEvaluation.maxAge = 0
		}
	}
	return localEvaluation, views
}

func generatorReadSelections(spec *specification) map[string]store.ArtifactRead {
	selections := make(map[string]store.ArtifactRead, len(spec.Sources))
	for _, output := range spec.Artifacts {
		if alias := strings.TrimPrefix(output.Revision, "@"); alias != "" {
			selections[alias] |= store.ArtifactRevision
		}
		if alias := strings.TrimPrefix(output.OriginRevision, "@"); alias != "" {
			selections[alias] |= store.ArtifactOrigin
		}
	}
	return selections
}

type generatorPublication struct {
	execution  *execution
	id         manifest.NamedResource
	raw        *manifest.RawObject
	spec       *specification
	uid        string
	evaluation *evaluation
	attempt    *store.Emission
	desired    []store.Child
	documents  []manifest.DocOccurrence
}

func (p *generatorPublication) generatedSource(ctx context.Context, req *request) (*store.SourceArtifact, error) {
	key := cache.ArchiveKey{Policy: "fixed", Content: struct {
		Namespace string
		Generator manifest.NamedResource
		Request   request
		Inputs    []inputIdentity
	}{"artifactgenerator/output/v1", p.id, *req, p.evaluation.identity}}
	localEvaluation := p.evaluation
	input, descriptor, keyID, err := p.execution.cache.Acquire(
		ctx,
		key,
		cache.Policy{MaxAge: p.evaluation.maxAge},
		func(ctx context.Context, target cache.ProducerInput) (cache.ProducerResult, error) {
			return localEvaluation.produce(ctx, req, target)
		},
	)
	if err != nil {
		return nil, fmt.Errorf("evaluate %s: %w", req.Output.Name, err)
	}
	digest := descriptor.Annotations["flate.tree-digest"]
	if input == nil || keyID == "" || digest == "" {
		return nil, manifest.NewError(
			manifest.ErrInput,
			nil,
			fmt.Sprintf("incomplete generated source %q", req.Output.Name),
		)
	}
	payload := &store.SourceArtifact{
		Kind:     manifest.KindExternalArtifact,
		FS:       input,
		Key:      keyID,
		Content:  cache.AcquisitionIdentity(input),
		Digest:   digest,
		Revision: "latest@" + digest,
	}
	if alias := strings.TrimPrefix(req.Output.Revision, "@"); alias != "" {
		payload.Revision = p.evaluation.inputs[alias].Revision
	}
	if source := p.evaluation.inputs[strings.TrimPrefix(req.Output.OriginRevision, "@")]; source != nil {
		origin := source.Metadata["org.opencontainers.image.revision"]
		if origin == "" {
			origin = source.Revision
		}
		payload.Metadata = map[string]string{"org.opencontainers.image.revision": origin}
	}
	return payload, nil
}

func (p *generatorPublication) generatedDeclaration(req *request) (store.Child, error) {
	labels := maps.Clone(p.spec.CommonMetadata.Labels)
	if labels == nil {
		labels = make(map[string]string, len(req.Captures)+generatedLabelCount)
	}
	maps.Copy(labels, req.Captures)
	labels["app.kubernetes.io/managed-by"], labels["source.extensions.fluxcd.io/generator"] = "source-watcher", p.uid
	declaration := p.raw.ExternalArtifact(
		metav1.ObjectMeta{
			Name:        req.Output.Name,
			Namespace:   p.id.Namespace,
			Labels:      labels,
			Annotations: p.spec.CommonMetadata.Annotations,
		},
	)
	generated, err := declaration.RenderDocument()
	if err != nil {
		return store.Child{}, err
	}
	return store.Child{Obj: declaration, Doc: generated}, nil
}

func (p *generatorPublication) publish(ctx context.Context, req *request) error {
	payload, err := p.generatedSource(ctx, req)
	if err != nil {
		return err
	}
	child, err := p.generatedDeclaration(req)
	if err != nil {
		return err
	}

	children := []store.Child{child}
	docs := []manifest.DocOccurrence{{Document: child.Doc, Ordinal: len(p.documents)}}
	if visible, _ := (manifest.EmissionPolicy{}).SelectDocument(child.Doc, p.execution.SkipKinds); !visible {
		docs = nil
	}
	targetID := child.Obj.Named()
	p.attempt.Children, p.attempt.Manifests, p.attempt.Complete = children, docs, false
	if err := p.execution.run.Emit(
		ctx,
		p.id,
		p.attempt,
		map[manifest.NamedResource]*store.SourceArtifact{targetID: payload},
	); err != nil {
		return err
	}
	p.desired, p.documents = append(p.desired, children...), append(p.documents, docs...)
	return nil
}
