package artifactgenerator

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/home-operations/flate/internal/engine/source/gittree"
	"github.com/home-operations/flate/internal/engine/store"
	"github.com/home-operations/flate/pkg/manifest"
)

type generatorPublication struct {
	execution   *execution
	id          manifest.NamedResource
	raw         *manifest.RawObject
	spec        *manifest.ArtifactGeneratorSpec
	policies    *namespacePolicies
	uid         string
	fingerprint string
	evaluation  *evaluation
	attempt     *store.Emission
	desired     []store.Child
	documents   []manifest.DocOccurrence
}

func (p *generatorPublication) render(
	ctx context.Context,
	requests []request,
) error {
	var targets namespaceTargets
	var payloads []*store.SourceArtifact
	if p.policies != nil {
		targets = collectNamespaceTargets(requests)
		total := len(targets.ordered) + len(requests)
		payloads = make([]*store.SourceArtifact, len(targets.ordered), total)
	} else if len(requests) > 1 {
		p.desired = make([]store.Child, 0, len(requests))
		p.documents = make([]manifest.DocOccurrence, 0, len(requests))
	}
	docs := make([]manifest.DocOccurrence, len(targets.ordered), len(targets.ordered)+len(requests))
	prototype := p.raw.ExternalArtifact(metav1.ObjectMeta{Annotations: p.spec.CommonMetadata.Annotations})
	for i := range requests {
		if err := ctx.Err(); err != nil {
			return fmt.Errorf("context: %w", err)
		}
		req := &requests[i]
		req.work.Scope = p.execution.run.Reads.Work().Scope()
		captureMetadata := targets.first[req.Namespace.Name] == req
		doc, payload, err := p.prepare(ctx, req, captureMetadata, prototype)
		if err != nil {
			return err
		}
		ordinal := len(docs)
		docs = append(docs, manifest.DocOccurrence{Document: doc, Ordinal: ordinal})
		if p.policies == nil {
			if err := p.publish(ctx, docs[ordinal:ordinal+1:ordinal+1],
				[]*store.SourceArtifact{payload}, false); err != nil {
				return err
			}
		} else {
			payloads = append(payloads, payload)
		}
	}
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("context: %w", err)
	}
	if p.policies == nil {
		return p.publish(ctx, nil, nil, true)
	}
	if err := p.prepareNamespaces(ctx, docs, targets, requests); err != nil {
		return manifest.NewError(manifest.ErrInput, err, "")
	}
	return p.publish(ctx, docs, payloads, true)
}

func (p *generatorPublication) prepare(
	ctx context.Context, req *request, captureMetadata bool, prototype *manifest.ExternalArtifact,
) (map[string]any, *store.SourceArtifact, error) {
	payload, inputs, err := p.generatedSource(ctx, req, captureMetadata)
	if err != nil {
		readErr, deferred := errors.AsType[*namespaceMetadataReadError](err)
		if !deferred || ctx.Err() != nil {
			return nil, nil, err
		}
		req.stagedMetadataError = readErr
	}
	doc, err := p.generatedDocument(req, prototype)
	if err != nil {
		return nil, nil, err
	}
	if len(inputs) != 0 {
		doc["status"] = map[string]any{"exportedInputs": inputs}
	}
	return doc, payload, nil
}

func (p *generatorPublication) generatedDocument(
	req *request, prototype *manifest.ExternalArtifact,
) (map[string]any, error) {
	labels := maps.Clone(p.spec.CommonMetadata.Labels)
	if labels == nil {
		labels = make(map[string]string, len(req.Captures)+generatedLabelCount)
	}
	maps.Copy(labels, req.Captures)
	labels = ownGeneratorLabels(labels, p.uid)
	declaration := *prototype
	declaration.Name, declaration.Namespace, declaration.Labels = req.Identity.Name, req.Identity.Namespace, labels
	return declaration.RenderDocument()
}

func (p *generatorPublication) publish(
	ctx context.Context,
	docs []manifest.DocOccurrence,
	sources []*store.SourceArtifact,
	complete bool,
) error {
	var payloads map[manifest.NamedResource]*store.SourceArtifact
	if len(docs) != 0 {
		parsed := p.execution.run.ParseChildren(
			p.raw, docs, manifest.ParseDocOptions{DefaultNamespace: p.id.Namespace},
		)
		if err := manifest.DocumentFailure(parsed.Failures); err != nil {
			return err
		}
		payloads = make(map[manifest.NamedResource]*store.SourceArtifact)
		firstOrdinal := docs[0].Ordinal
		for i := range parsed.Children {
			child := &parsed.Children[i]
			payload := sources[child.Ordinal-firstOrdinal]
			if payload == nil {
				continue
			}
			id := child.Obj.Named()
			// Reads use payload.FS, so the virtual coordinate has no host directory.
			label, _ := gittree.Label(gittree.LabelOptions{Root: "/", Key: payload.Digest, Source: id})
			payload.LocalPath, payload.Key = label, id.NamespacedName()
			payloads[id] = payload
		}
		p.attempt.Children, p.attempt.Manifests = parsed.Children, parsed.Visible
	}
	if !complete && len(docs) != 0 {
		if p.desired == nil {
			p.desired = slices.Clip(p.attempt.Children)
			p.documents = slices.Clip(p.attempt.Manifests)
		} else {
			p.desired = append(p.desired, p.attempt.Children...)
			p.documents = append(p.documents, p.attempt.Manifests...)
		}
	}
	if complete && len(docs) == 0 {
		p.attempt.Children, p.attempt.Manifests = p.desired, p.documents
	}
	p.attempt.Complete, p.attempt.Fingerprint = complete, ""
	if complete {
		p.attempt.Fingerprint = p.fingerprint
	}
	return p.execution.run.Emit(ctx, p.id, p.attempt, payloads)
}
