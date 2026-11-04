// Package kustomization reconciles Flux Kustomizations: wait on
// dependsOn / sourceRef / structural parent, resolve postBuild
// substitutions, run the kustomize SDK, parse the result back into the
// Store, and publish an Emission. Failures bubble up to
// the orchestrator.
package kustomization

import (
	"context"
	"iter"

	"github.com/home-operations/flate/pkg/controllers/base"
	"github.com/home-operations/flate/pkg/depwait"
	"github.com/home-operations/flate/pkg/kustomize"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/schedule"
	"github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/store"
	"github.com/home-operations/flate/pkg/task"
	"github.com/home-operations/flate/pkg/values"
)

// Controller orchestrates Kustomization reconciliation. Reconcile-
// shaping state (Filter, ParentOf) flows in via Configure exactly once
// before StartLifecycle. The invariant — "config is read-only after StartLifecycle" — is
// encoded in the embedded *base.Controller, not just in code review.
type Controller struct {
	*base.Controller

	retry cache.RetryConfig

	// Trees shares remote fetches and persisted renders.
	Trees *kustomize.TreeCache

	strictSubstitutions bool
	restricted          bool
}

// Options carries the post-bootstrap state the orchestrator wires onto the
// controller before StartLifecycle. base.Options holds the common reconcile config.
type Options struct {
	base.Options

	// StrictSubstitutions rejects undefined variables without defaults.
	StrictSubstitutions bool
}

type execution struct {
	*Controller

	run  base.Execution
	data *values.StoreProvider
}

func (c *Controller) newExecution(id manifest.NamedResource) *execution {
	execution := &execution{Controller: c}
	execution.run.Init(c.Controller, id)
	execution.data = values.NewStoreProvider(&execution.run.Reads)
	return execution
}

// New constructs a Kustomization controller.
func New(s *store.Store, t *task.Service, trees *kustomize.TreeCache) *Controller {
	return &Controller{
		Controller: base.New(s, t),
		Trees:      trees,
	}
}

// Configure installs the post-bootstrap state. Panics if called after
// StartLifecycle — encodes the invariant that reconcile-shaping config is
// read-only once the controller is dispatching.
//
//nolint:gocritic // Configure snapshots the public value options before concurrent dispatch.
func (c *Controller) Configure(opts Options, retry cache.RetryConfig) {
	c.Controller.Configure(opts.Options)
	c.retry = retry
	c.strictSubstitutions = opts.StrictSubstitutions
	c.restricted = opts.Filter != nil
}

// ReconcileNode runs id's reconcile under the dag engine, returning the blocked
// dependency set (nil = terminalized). The
// orchestrator's scheduler Dispatcher calls this for Kustomization nodes.
func (c *Controller) ReconcileNode(
	ctx context.Context,
	id manifest.NamedResource,
	level manifest.DrainLevel,
) *base.Execution {
	ex := c.newExecution(id)
	ex.run.Dispatch(ctx, level, ex, (*execution).reconcile)
	return &ex.run
}

// Owns reports whether id belongs to the Kustomization controller.
func (*Controller) Owns(id manifest.NamedResource) bool {
	return id.Group == manifest.FluxKustomizeDomain && id.Kind == manifest.KindKustomization
}

func (c *execution) reconcile(ctx context.Context, ks *manifest.Kustomization, level manifest.DrainLevel) error {
	var err error
	ks, err = c.run.RequireRefresh(ctx, ks, manifest.GateInputs, level)
	if err != nil {
		return err
	}
	if _, localErr := c.run.RequireRefresh(ctx, ks, manifest.GateArtifacts, level); localErr != nil {
		return localErr
	}
	source, applyIgnore, err := kustomize.SourceInput(&c.run.Reads, ks)
	if err != nil {
		return err
	}
	inputs, err := c.run.DataInputs(ctx, ks, c.data)
	if err != nil {
		return err
	}
	return c.run.AwaitData(
		ctx,
		base.DataWait{
			Inputs: inputs,
			Resume: func(ctx context.Context, _ manifest.DrainLevel, _ []manifest.NamedResource) error {
				return c.render(ctx, ks, source, applyIgnore)
			},
		},
		level,
	)
}

func (c *execution) render(
	ctx context.Context,
	ks *manifest.Kustomization,
	source *store.SourceArtifact,
	applyIgnore bool,
) error {
	id := ks.Named()
	ks, err := kustomize.Prepare(ks, c.data)
	if err != nil {
		return err
	}
	var attempt store.Emission
	defer attempt.Finish()
	if c.restricted {
		fingerprintDedupResult1, fingerprintDedupResult1Err := c.run.FingerprintDedup(ctx, id, nil, &attempt)
		_, handled, innerLocalErr := fingerprintDedupResult1.Fingerprint, fingerprintDedupResult1.Reused, fingerprintDedupResult1Err
		if handled || innerLocalErr != nil {
			return innerLocalErr
		}
	} else if checkpointErr := c.run.Checkpoint(); checkpointErr != nil {
		return checkpointErr
	}

	_, _, attempt = c.run.Reads.GetEmission(id)
	request := kustomize.RenderRequest{
		Work:                    c.run.Reads.Work().Scope().RenderWork(),
		Source:                  source,
		ApplyIgnore:             applyIgnore,
		SubPath:                 ks.Path,
		Spec:                    ks,
		HeldSlot:                c.Tasks,
		SkipKinds:               c.SkipKinds,
		DependencySubstitutions: c.DependencySubstitutions,
		StrictSubstitutions:     c.strictSubstitutions,
	}
	docs, receipt, err := kustomize.Render(ctx, c.Trees, request, c.retry)
	if err != nil {
		return err
	}
	attempt.Render = receipt
	content := request.Content(docs)
	acceptance := kustomize.RenderAcceptance{
		Work:                    c.run.Reads.Work().Scope().RenderWork(),
		Owner:                   id,
		Entry:                   ks.EntryReceipt,
		Source:                  source.HistoryIdentity(ks.SourceID()),
		Content:                 content,
		DependencySubstitutions: c.DependencySubstitutions,
	}
	fingerprintDedupResult2, fingerprintDedupResult2Err := c.run.FingerprintDedup(ctx, id, content, &attempt)
	fp, handled, err := fingerprintDedupResult2.Fingerprint, fingerprintDedupResult2.Reused, fingerprintDedupResult2Err
	if handled || err != nil {
		if err != nil {
			return err
		}
		return c.recordRender(ctx, &attempt, &acceptance)
	}

	if err := kustomize.Shape(
		ctx,
		ks,
		docs,
		kustomize.ShapeOptions{
			Strict: c.strictSubstitutions,
			Work:   c.run.Reads.Work().Scope().RenderWork().Substitution(),
		},
	); err != nil {
		return err
	}

	parseChildrenResult3 := c.run.ParseChildren(
		ks,
		docs,
		manifest.ParseDocOptions{},
	)
	attempt.Children, attempt.Manifests, attempt.Failures = parseChildrenResult3.Children, parseChildrenResult3.Visible, parseChildrenResult3.Failures
	attempt.Fingerprint, attempt.Complete, attempt.Render = fp, true, receipt
	if err := c.run.Emit(ctx, id, &attempt, nil); err != nil {
		return err
	}
	return c.recordRender(ctx, &attempt, &acceptance)
}

func (c *execution) recordRender(
	ctx context.Context,
	attempt *store.Emission,
	acceptance *kustomize.RenderAcceptance,
) error {
	acceptance.Receipt, acceptance.Reads, acceptance.OutputReads = attempt.Render, attempt.Reads, attempt.OutputReads
	acceptance.Records = attempt.Footprint()
	acceptance.Sources = attempt.SourcePaths()
	if acceptance.Entry != nil {
		acceptance.Sources = c.Store.ChildSourcePaths(acceptance.Owner, attempt.Children, nil)
	}
	return c.Trees.RecordRender(ctx, *acceptance)
}

// PreviousRender prepares the immutable candidate with captured inputs without reconciling or acquiring sources.
func (c *Controller) PreviousRender(
	ctx context.Context,
	obj manifest.BaseManifest,
	reads *store.ReadSet,
) (iter.Seq2[manifest.NamedResource, manifest.DiscoveryRecord], bool) {
	ks, ok := obj.(*manifest.Kustomization)
	if !ok || ctx.Err() != nil {
		return nil, false
	}
	data := values.NewStoreProvider(reads)
	waiter := depwait.Waiter{Store: reads, Parent: reads.ID(), AllowMissingCRDs: c.MissingCRDsAllowed()}
	for ref := range manifest.References(ks, manifest.ReferenceRelations) {
		if ref.Use != "substituteFrom" {
			continue
		}
		availability := waiter.Available(
			ctx,
			depwait.DataDependency{
				DependencyRef: ref.DependencyRef,
				Input:         data.Lookup(ref.NamedResource),
				Optional:      ref.Optional,
			},
			manifest.DrainNone,
		)
		if availability.Err != nil || availability.Kind != schedule.Terminal {
			return nil, false
		}
	}
	prepared, err := kustomize.Prepare(ks, data)
	if err != nil {
		return nil, false
	}
	source, ignore, err := kustomize.SourceInput(reads, prepared)
	if err != nil {
		return nil, false
	}
	request := kustomize.RenderRequest{
		Work:                    reads.Work().Scope().RenderWork(),
		Source:                  source,
		ApplyIgnore:             ignore,
		SubPath:                 prepared.Path,
		Spec:                    prepared,
		SkipKinds:               c.SkipKinds,
		DependencySubstitutions: c.DependencySubstitutions,
		StrictSubstitutions:     c.strictSubstitutions,
	}
	docs, receipt, found := c.Trees.PreviousRender(ctx, request)
	if !found {
		return nil, false
	}
	content := request.Content(docs)
	return c.Trees.RenderHistory(ks.Named(), receipt, content)
}
