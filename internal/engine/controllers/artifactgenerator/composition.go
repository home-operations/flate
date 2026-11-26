package artifactgenerator

import (
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"slices"

	"github.com/home-operations/flate/internal/engine/source/cache"
	"github.com/home-operations/flate/internal/engine/store"
	"github.com/home-operations/flate/pkg/manifest"
)

const (
	inputsCapture   = "inputs"
	metadataCapture = "namespace-metadata"
)

func (e *evaluation) composition(req *request) (cache.Composition, error) {
	e.projectedInputs = slices.Grow(e.projectedInputs[:0], len(e.identity))[:len(e.identity)]
	e.projectedCopies = slices.Grow(e.projectedCopies[:0], len(req.Output.Copy))[:len(req.Output.Copy)]
	result := cache.Composition{
		Inputs: e.projectedInputs,
		Copies: e.projectedCopies,
		Policy: "fixed",
	}
	for i := range e.identity {
		identity := &e.identity[i]
		result.Inputs[i] = cache.CompositionInput{
			FS:      e.inputs[identity.Alias],
			Alias:   identity.Alias,
			Content: identity.Input.Content,
			Backing: identity.Input.LocalPath,
		}
	}
	for i := range req.Output.Copy {
		op := &req.Output.Copy[i]
		source, err := e.selectors.resolve(op.From, copySourceSelector, false)
		if sourceErr := cmp.Or(err, source.globErr); sourceErr != nil {
			return cache.Composition{}, fmt.Errorf("invalid copy source '%s': %w", op.From, sourceErr)
		}
		destination, err := e.selectors.resolve(op.To, destinationSelector, false)
		if err != nil {
			return cache.Composition{}, fmt.Errorf("invalid copy destination '%s': %w", op.To, err)
		}
		from := source.selector
		position := e.inputIndex(from.Alias)
		if position == -1 {
			return cache.Composition{}, fmt.Errorf("source alias '%s' not found", from.Alias)
		}
		result.Copies[i] = cache.Copy{
			Input:    position,
			From:     from.Path,
			To:       cmp.Or(destination.selector.Path, "."),
			Exclude:  op.Exclude,
			Optional: op.Optional,
			Mode:     e.copyModes[op.Strategy],
		}
	}
	return result, nil
}

func (e *evaluation) inputIndex(alias string) int {
	for i := range e.identity {
		if e.identity[i].Alias == alias {
			return i
		}
	}
	return -1
}

func (p *generatorPublication) generatedSource(
	ctx context.Context,
	req *request,
	captureMetadata bool,
) (*store.SourceArtifact, map[string]any, error) {
	composition, err := p.evaluation.composition(req)
	if err != nil {
		return nil, nil, manifest.NewError(manifest.ErrInput, fmt.Errorf("evaluate %s: %w", req.Identity.Name, err), "")
	}
	from := req.inputsFrom
	stagedInputs := from.StagedFile()
	if stagedInputs {
		composition.Captures = append(composition.Captures,
			cache.StagedCapture{Purpose: inputsCapture, Path: from.Text, Read: p.evaluation.captureInputs(from)})
	}
	metadata := req.namespaceMetadata
	stagedMetadata := captureMetadata && metadata.StagedFile()
	if stagedMetadata {
		composition.Captures = append(
			composition.Captures,
			cache.StagedCapture{
				Purpose: metadataCapture,
				Path:    metadata.Text,
				Read:    p.evaluation.captureNamespaceMetadata(metadata),
			},
		)
	}
	input, descriptor, _, composeErr := p.execution.cache.Compose(
		ctx,
		composition,
		&cache.Policy{MaxAge: p.evaluation.maxAge, Work: &req.work},
	)
	if composeErr != nil {
		if _, deferred := errors.AsType[*namespaceMetadataReadError](composeErr); !deferred || ctx.Err() != nil {
			return nil, nil, fmt.Errorf("evaluate %s: %w", req.Identity.Name, composeErr)
		}
	}
	var inputs map[string]any
	if !stagedInputs {
		inputs, err = p.evaluation.exportedInputs(ctx, from, nil)
	} else if composeErr == nil {
		var staged map[string]any
		captured := descriptor.Annotations[cache.StagedCaptureAnnotation(inputsCapture)]
		err = json.Unmarshal([]byte(captured), &staged)
		inputs = staged
	}
	if err != nil {
		return nil, nil, manifest.NewError(manifest.ErrInput, fmt.Errorf("evaluate %s: %w", req.Identity.Name, err), "")
	}
	if composeErr != nil {
		return nil, nil, composeErr
	}
	if stagedMetadata {
		captured := descriptor.Annotations[cache.StagedCaptureAnnotation(metadataCapture)]
		if err := json.Unmarshal([]byte(captured), &req.stagedMetadata); err != nil {
			return nil, nil, fmt.Errorf("decode captured namespace metadata: %w", err)
		}
	}
	digest := descriptor.Annotations["flate.tree-digest"]
	if input == nil || digest == "" {
		return nil, nil, manifest.NewError(
			manifest.ErrInput,
			nil,
			fmt.Sprintf("incomplete generated source %q", req.Identity.Name),
		)
	}
	return p.evaluation.generatedPayload(req, input, digest), inputs, nil
}

func (e *evaluation) generatedPayload(req *request, input fs.FS, digest string) *store.SourceArtifact {
	payload := &store.SourceArtifact{
		Provider: store.ProviderGenerator,
		Kind:     manifest.KindExternalArtifact,
		FS:       input,
		Content:  digest,
		Digest:   digest,
		Revision: "latest@" + digest,
	}
	if alias := e.selectors.revision(req.Output.Revision); alias != "" {
		payload.Revision = e.identity[e.inputIndex(alias)].Input.Revision
	}
	if position := e.inputIndex(e.selectors.revision(req.Output.OriginRevision)); position >= 0 {
		source := &e.identity[position].Input
		origin := source.Origin
		if origin == "" {
			origin = source.Revision
		}
		payload.Metadata = map[string]string{"org.opencontainers.image.revision": origin}
	}
	return payload
}
