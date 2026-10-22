// Package artifactgenerator evaluates ArtifactGenerators and publishes their ExternalArtifacts.
package artifactgenerator

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io/fs"
	"strings"
	"time"

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

// ReconcileNode returns the captured assessment of ArtifactGenerator evaluation.
func (c *Controller) ReconcileNode(ctx context.Context, id manifest.NamedResource, level manifest.DrainLevel) *base.Execution {
	ex := &execution{Controller: c}
	ex.run.Init(c.Controller, id)
	ex.run.Dispatch(ctx, level, ex, (*execution).reconcile)
	return &ex.run
}

type inputIdentity struct {
	Alias                                              string
	Source                                             manifest.NamedResource
	Key, Digest, Revision, Root, Path, Content, Origin string
	OriginPresent                                      bool
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
	if !ok || !raw.ReconcileDocument() {
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
	metadata, _ := doc["metadata"].(map[string]any)
	annotations, _ := metadata["annotations"].(map[string]any)
	disabled, _ := annotations["source.extensions.fluxcd.io/reconcile"].(string)
	if strings.EqualFold(disabled, "disabled") {
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
	for ref := range manifest.References(raw, manifest.ReferenceRelations) {
		artifact := c.run.Reads.GetArtifact(ref.NamedResource).(*store.SourceArtifact)
		alias := spec.Sources[ref.Position].Alias
		evaluation.inputs[alias], views[alias] = artifact, artifact.FS
		origin, present := artifact.Metadata["org.opencontainers.image.revision"]
		evaluation.identity = append(evaluation.identity, inputIdentity{Alias: alias, Source: ref.NamedResource, Key: artifact.Key, Digest: artifact.Digest, Revision: artifact.Revision, Root: artifact.LocalRoot, Path: artifact.LocalPath, Content: cache.AcquisitionIdentity(artifact.FS), Origin: origin, OriginPresent: present})
		if artifact.Key == "" || artifact.Digest == "" && artifact.Revision == "" {
			evaluation.maxAge = 0
		}
	}
	uid, _ := metadata["uid"].(string)
	content := struct {
		Generator     manifest.NamedResource
		Spec          specification
		UID, Disabled string
		Inputs        []inputIdentity
	}{id, spec, uid, disabled, evaluation.identity}
	var replayContent any = content
	if evaluation.maxAge == 0 {
		replayContent = nil
	}
	fingerprint, handled, err := c.run.FingerprintDedup(id, replayContent)
	if handled || err != nil {
		return err
	}
	requests, err := expand(ctx, spec, views)
	if err != nil {
		return fmt.Errorf("%w: expand outputs: %w", manifest.ErrInput, err)
	}
	var desired []store.Child
	var documents []map[string]any
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
		payload := &store.SourceArtifact{Kind: manifest.KindExternalArtifact, FS: input, Key: keyID, Digest: digest, Revision: "latest@" + digest}
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
		outputMetadata := map[string]any{"name": req.Output.Name, "namespace": id.Namespace}
		generated := map[string]any{"apiVersion": "source.toolkit.fluxcd.io/v1", "kind": manifest.KindExternalArtifact, "metadata": outputMetadata, "spec": map[string]any{"sourceRef": map[string]any{"apiVersion": raw.APIVersion, "kind": raw.Kind, "name": id.Name, "namespace": id.Namespace}}}
		batch := []map[string]any{generated}
		base.Stamp(manifest.EmissionPolicy{Labels: spec.CommonMetadata.Labels, Annotations: spec.CommonMetadata.Annotations}, batch)
		labels, _ := outputMetadata["labels"].(map[string]any)
		if labels == nil {
			labels = make(map[string]any, len(req.Captures)+2)
			outputMetadata["labels"] = labels
		}
		for key, value := range req.Captures {
			labels[key] = value
		}
		labels["app.kubernetes.io/managed-by"], labels["source.extensions.fluxcd.io/generator"] = "source-watcher", uid
		children, docs := c.ParseChildren(raw, batch, manifest.ParseDocOptions{})
		targetID := manifest.NamedResource{Group: manifest.SourceDomain, Kind: manifest.KindExternalArtifact, Name: req.Output.Name, Namespace: id.Namespace}
		if len(children) != 1 || children[0].Obj.Named() != targetID || children[0].Doc == nil {
			return fmt.Errorf("%w: expected exactly one generated ExternalArtifact %s", manifest.ErrInput, targetID)
		}
		attempt.Children, attempt.Manifests, attempt.Complete = children, docs, false
		if err := c.run.Emit(id, &attempt, map[manifest.NamedResource]*store.SourceArtifact{targetID: payload}); err != nil {
			return err
		}
		desired, documents = append(desired, children...), append(documents, docs...)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	attempt.Children, attempt.Manifests, attempt.Complete, attempt.Fingerprint = desired, documents, true, fingerprint
	return c.run.Emit(id, &attempt, nil)
}
