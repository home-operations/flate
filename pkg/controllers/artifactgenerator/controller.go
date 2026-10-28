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
func (c *Controller) ReconcileNode(ctx context.Context, id manifest.NamedResource, level manifest.DrainLevel) *base.Execution {
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
	raw, ok := selected.(*manifest.RawObject)
	if !ok {
		return fmt.Errorf("%w: expected ArtifactGenerator document", manifest.ErrInput)
	}
	if doc == nil {
		return fmt.Errorf("%w: missing authoritative document for %s", manifest.ErrInput, id)
	}
	parsed, _, _, err := manifest.ParseDoc(doc, manifest.ParseDocOptions{})
	if err != nil {
		return fmt.Errorf("parse generator: %w", err)
	}
	authored, ok := parsed.(*manifest.RawObject)
	if !ok || authored.Named().Group != id.Group || authored.Kind != raw.Kind || authored.Name != raw.Name {
		return fmt.Errorf("%w: mismatched authoritative document for %s", manifest.ErrInput, id)
	}
	actual, err := json.Marshal(raw.Spec)
	if err != nil {
		return fmt.Errorf("%w: marshal generator spec: %w", manifest.ErrInput, err)
	}
	expected, err := json.Marshal(authored.Spec)
	if err != nil || !bytes.Equal(actual, expected) {
		return fmt.Errorf("%w: mismatched generator spec", manifest.ErrInput)
	}
	uid, disabled, consumes := raw.ReconcileInput(doc)
	if !consumes {
		return fmt.Errorf("%w: expected ArtifactGenerator document", manifest.ErrInput)
	}
	if disabled {
		return nil
	}
	spec, err := decodeSpec(raw.Spec)
	if err != nil {
		return fmt.Errorf("%w: generator spec: %w", manifest.ErrInput, err)
	}
	raw, err = c.run.RequireRefresh(ctx, raw, manifest.GateArtifacts, level)
	if err != nil {
		return err
	}
	evaluation := evaluation{owner: c.cache, inputs: make(map[string]*store.SourceArtifact, len(spec.Sources)), maxAge: cache.Forever}
	views := make(map[string]fs.FS, len(spec.Sources))
	selections := make(map[string]store.ArtifactRead, len(spec.Sources))
	for _, output := range spec.Artifacts {
		if alias := strings.TrimPrefix(output.Revision, "@"); alias != "" {
			selections[alias] |= store.ArtifactRevision
		}
		if alias := strings.TrimPrefix(output.OriginRevision, "@"); alias != "" {
			selections[alias] |= store.ArtifactOrigin
		}
	}
	for ref := range manifest.References(raw, manifest.ReferenceRelations) {
		alias := spec.Sources[ref.Position].Alias
		read := store.ArtifactContent | selections[alias]
		artifact := c.run.Reads.GetArtifact(ref.NamedResource, read).(*store.SourceArtifact)
		if read&store.ArtifactOrigin != 0 && artifact.Metadata["org.opencontainers.image.revision"] == "" {
			read |= store.ArtifactRevision
			c.run.Reads.GetArtifact(ref.NamedResource, read)
		}
		evaluation.inputs[alias], views[alias] = artifact, artifact.FS
		evaluation.identity = append(evaluation.identity, inputIdentity{Alias: alias, Source: ref.NamedResource, Input: artifact.Input(read)})
		if artifact.Content == "" {
			evaluation.maxAge = 0
		}
	}
	content := struct {
		Generator manifest.NamedResource
		Spec      specification
		UID       string
		Disabled  bool
		Inputs    []inputIdentity
	}{id, spec, uid, disabled, evaluation.identity}
	var replayContent any = content
	if evaluation.maxAge == 0 {
		replayContent = nil
	}
	fingerprint, handled, err := c.run.FingerprintDedup(ctx, id, replayContent, &store.Emission{})
	if handled || err != nil {
		return err
	}
	requests, err := expand(ctx, spec, views)
	if err != nil {
		return fmt.Errorf("%w: expand outputs: %w", manifest.ErrInput, err)
	}
	var desired []store.Child
	var documents []manifest.DocOccurrence
	for _, req := range requests {
		if err := ctx.Err(); err != nil {
			return err
		}
		key := cache.ArchiveKey{Policy: "fixed", Content: struct {
			Namespace string
			Generator manifest.NamedResource
			Request   request
			Inputs    []inputIdentity
		}{"artifactgenerator/output/v1", id, req, evaluation.identity}}
		input, descriptor, keyID, err := c.cache.Acquire(ctx, key, cache.Policy{MaxAge: evaluation.maxAge}, func(ctx context.Context, target cache.ProducerInput) (cache.ProducerResult, error) {
			return evaluation.produce(ctx, req, target)
		})
		if err != nil {
			return fmt.Errorf("evaluate %s: %w", req.Output.Name, err)
		}
		digest := descriptor.Annotations["flate.tree-digest"]
		if input == nil || keyID == "" || digest == "" {
			return fmt.Errorf("%w: incomplete generated source %q", manifest.ErrInput, req.Output.Name)
		}
		payload := &store.SourceArtifact{Kind: manifest.KindExternalArtifact, FS: input, Key: keyID, Content: cache.AcquisitionIdentity(input), Digest: digest, Revision: "latest@" + digest}
		if alias := strings.TrimPrefix(req.Output.Revision, "@"); alias != "" {
			payload.Revision = evaluation.inputs[alias].Revision
		}
		if source := evaluation.inputs[strings.TrimPrefix(req.Output.OriginRevision, "@")]; source != nil {
			origin := source.Metadata["org.opencontainers.image.revision"]
			if origin == "" {
				origin = source.Revision
			}
			payload.Metadata = map[string]string{"org.opencontainers.image.revision": origin}
		}
		labels := maps.Clone(spec.CommonMetadata.Labels)
		if labels == nil {
			labels = make(map[string]string, len(req.Captures)+2)
		}
		maps.Copy(labels, req.Captures)
		labels["app.kubernetes.io/managed-by"], labels["source.extensions.fluxcd.io/generator"] = "source-watcher", uid
		declaration := raw.ExternalArtifact(metav1.ObjectMeta{Name: req.Output.Name, Namespace: id.Namespace, Labels: labels, Annotations: spec.CommonMetadata.Annotations})
		generated, err := declaration.RenderDocument()
		if err != nil {
			return err
		}
		children := []store.Child{{Obj: declaration, Doc: generated}}
		docs := []manifest.DocOccurrence{{Document: generated, Ordinal: len(documents)}}
		if visible, _ := (manifest.EmissionPolicy{}).SelectDocument(generated, c.SkipKinds); !visible {
			docs = nil
		}
		targetID := declaration.Named()
		attempt.Children, attempt.Manifests, attempt.Complete = children, docs, false
		if err := c.run.Emit(ctx, id, &attempt, map[manifest.NamedResource]*store.SourceArtifact{targetID: payload}); err != nil {
			return err
		}
		desired, documents = append(desired, children...), append(documents, docs...)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	attempt.Children, attempt.Manifests, attempt.Complete, attempt.Fingerprint = desired, documents, true, fingerprint
	return c.run.Emit(ctx, id, &attempt, nil)
}
