// Package artifactgenerator evaluates ArtifactGenerators and publishes their outputs.
package artifactgenerator

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io/fs"
	"strings"
	"time"

	"github.com/home-operations/flate/internal/engine/schedule"

	"github.com/home-operations/flate/internal/engine/controllers/base"
	"github.com/home-operations/flate/internal/engine/source/cache"
	"github.com/home-operations/flate/internal/engine/store"
	"github.com/home-operations/flate/internal/engine/task"
	"github.com/home-operations/flate/pkg/manifest"
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
	return id.Group == manifest.SourceExtensionsDomain && id.Kind == manifest.KindArtifactGenerator
}

// ReconcileNode returns the captured assessment of ArtifactGenerator evaluation.
func (c *Controller) ReconcileNode(
	ctx context.Context,
	id manifest.NamedResource,
	level manifest.DrainLevel,
	previous schedule.Result,
) *base.Execution {
	ex := &execution{Controller: c}
	ex.run.Init(c.Controller, id)
	ex.run.Take(previous)
	ex.run.Dispatch(ctx, level, ex, (*execution).reconcile)
	return &ex.run
}

type inputIdentity struct {
	Alias  string                 `json:"Alias"`
	Source manifest.NamedResource `json:"Source"`
	Input  store.ArtifactInput    `json:"Input"`
}

type evaluation struct {
	inputs   map[string]fs.FS
	identity []inputIdentity
	maxAge   time.Duration
	// Source syntax is independent of input generations and output placement.
	lastFrom      string
	lastSelection sourceSelection
	// Projections are borrowed only until Compose returns; outputs reconcile serially.
	projectedInputs []cache.CompositionInput
	projectedCopies []cache.Copy
}

func (c *execution) reconcile(ctx context.Context, obj *manifest.RawObject, level manifest.DrainLevel) error {
	var err error
	obj, err = c.run.RequireRefresh(ctx, obj, manifest.GateParents, level)
	if err != nil {
		return err
	}
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
	reconciliation := raw.ReconcileInput(doc)
	uid, disabled, consumes := reconciliation.UID, reconciliation.Disabled, reconciliation.Consumes
	if !consumes {
		return manifest.NewError(manifest.ErrInput, nil, "expected ArtifactGenerator document")
	}
	if disabled {
		return nil
	}
	spec, err := raw.ArtifactGeneratorSpec()
	if err != nil {
		return manifest.NewError(manifest.ErrInput, fmt.Errorf("generator spec: %w", err), "")
	}
	policies, err := compileNamespacePolicies(spec.ManagedNamespaces)
	if err != nil {
		return manifest.NewError(manifest.ErrInput, err, "")
	}
	if specErr := validateSpec(&spec, id.Namespace); specErr != nil {
		return manifest.NewError(manifest.ErrInput, fmt.Errorf("generator spec: %w", specErr), "")
	}
	raw, err = c.run.RequireRefresh(ctx, raw, manifest.GateArtifacts, level)
	if err != nil {
		return err
	}
	localEvaluation := c.generatorInputs(raw, &spec)
	content := struct {
		Generator manifest.NamedResource
		Spec      manifest.ArtifactGeneratorSpec
		UID       string
		Disabled  bool
		Inputs    []inputIdentity
	}{id, spec, uid, disabled, localEvaluation.identity}
	var replayContent any = content
	if localEvaluation.maxAge == 0 {
		replayContent = nil
	}
	fingerprintDedupResult1, fingerprintDedupResult1Err := c.run.FingerprintDedup(
		ctx,
		id,
		replayContent,
		&store.Emission{},
	)
	fingerprint, handled, err := fingerprintDedupResult1.Fingerprint, fingerprintDedupResult1.Reused, fingerprintDedupResult1Err
	if handled || err != nil {
		return err
	}
	requests, err := expand(ctx, &spec, id.Namespace, localEvaluation.inputs)
	if err != nil {
		return manifest.NewError(manifest.ErrInput, fmt.Errorf("expand outputs: %w", err), "")
	}
	publication := generatorPublication{
		execution:   c,
		id:          id,
		raw:         raw,
		spec:        &spec,
		policies:    policies,
		uid:         uid,
		fingerprint: fingerprint,
		evaluation:  &localEvaluation,
		attempt:     &attempt,
	}
	return publication.render(ctx, requests)
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
	parsed2, err := manifest.ParseDoc(doc, manifest.ParseDocOptions{})
	parsed := parsed2.Object
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

func (c *execution) generatorInputs(raw *manifest.RawObject, spec *manifest.ArtifactGeneratorSpec) evaluation {
	localEvaluation := evaluation{
		inputs: make(map[string]fs.FS, len(spec.Sources)),
		maxAge: cache.Forever,
	}
	selections := generatorReadSelections(spec)
	for ref := range manifest.References(raw, manifest.ReferenceRelations) {
		alias := spec.Sources[ref.Position].Alias
		read := store.ArtifactContent | selections[alias]
		artifact := c.run.Reads.GetArtifact(ref.NamedResource, read).(*store.SourceArtifact) //nolint:errcheck // Required source dependencies resolve source artifacts before reconciliation.
		if read&store.ArtifactOrigin != 0 && artifact.Metadata["org.opencontainers.image.revision"] == "" {
			read |= store.ArtifactRevision
			c.run.Reads.GetArtifact(ref.NamedResource, read)
		}
		localEvaluation.inputs[alias] = artifact.FS
		localEvaluation.identity = append(
			localEvaluation.identity,
			inputIdentity{Alias: alias, Source: ref.NamedResource, Input: artifact.Input(read)},
		)
		if artifact.Content == "" {
			localEvaluation.maxAge = 0
		}
	}
	return localEvaluation
}

func generatorReadSelections(spec *manifest.ArtifactGeneratorSpec) map[string]store.ArtifactRead {
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
