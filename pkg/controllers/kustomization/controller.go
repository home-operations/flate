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

	// WipeSecrets controls whether Secret cleartext is wiped when
	// parsing rendered manifests.
	WipeSecrets bool

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
	execution.data = values.NewStoreProvider(&execution.run.Reads, c.WipeSecrets)
	return execution
}

// New constructs a Kustomization controller.
func New(s *store.Store, t *task.Service, trees *kustomize.TreeCache, wipeSecrets bool) *Controller {
	return &Controller{
		Controller:  base.New(s, t),
		Trees:       trees,
		WipeSecrets: wipeSecrets,
	}
}

// Configure installs the post-bootstrap state. Panics if called after
// StartLifecycle — encodes the invariant that reconcile-shaping config is
// read-only once the controller is dispatching.
func (c *Controller) Configure(opts Options, retry cache.RetryConfig) {
	c.Controller.Configure(opts.Options)
	c.retry = retry
	c.strictSubstitutions = opts.StrictSubstitutions
	c.restricted = opts.Filter != nil

}

// ReconcileNode runs id's reconcile under the dag engine, returning the blocked
// dependency set (nil = terminalized). The
// orchestrator's scheduler Dispatcher calls this for Kustomization nodes.
func (c *Controller) ReconcileNode(ctx context.Context, id manifest.NamedResource, level manifest.DrainLevel) *base.Execution {
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
	if _, err := c.run.RequireRefresh(ctx, ks, manifest.GateArtifacts, level); err != nil {
		return err
	}
	source, applyIgnore, err := kustomize.SourceInput(&c.run.Reads, ks)
	if err != nil {
		return err
	}
	inputs, err := c.run.DataInputs(ctx, ks, c.data)
	if err != nil {
		return err
	}
	return c.run.AwaitData(ctx, base.DataWait{Inputs: inputs, Resume: func(ctx context.Context, _ manifest.DrainLevel, _ []manifest.NamedResource) error {
		return c.render(ctx, ks, source, applyIgnore)
	}}, level)
}

func (c *execution) render(ctx context.Context, ks *manifest.Kustomization, source *store.SourceArtifact, applyIgnore bool) error {
	id := ks.Named()
	ks, err := kustomize.Prepare(ks, c.data)
	if err != nil {
		return err
	}
	sourceContent := struct {
		Input       store.ArtifactInput
		ApplyIgnore bool
		Output      []manifest.DocOccurrence
	}{Input: source.Input(store.ArtifactContent), ApplyIgnore: applyIgnore}
	var attempt store.Emission
	defer attempt.Finish()
	var fp string
	if source.Content != "" || c.restricted {
		var handled bool
		var content any
		if source.Content != "" {
			content = ks.Content(sourceContent)
		}
		fp, handled, err = c.run.FingerprintDedup(ctx, id, content, &attempt)
		if handled || err != nil {
			if err != nil {
				return err
			}
			return c.Trees.RecordRender(ctx, kustomize.RenderAcceptance{Owner: id, Receipt: attempt.Render, Entry: ks.EntryReceipt, Records: attempt.Footprint(), Source: source.HistoryIdentity(ks.SourceID()), Sources: attempt.SourcePaths(), DependencySubstitutions: c.DependencySubstitutions})
		}
	} else if err := c.run.Checkpoint(); err != nil {
		return err
	}

	_, _, attempt = c.run.Reads.GetEmission(id)
	docs, receipt, err := kustomize.Render(ctx, c.Trees, kustomize.RenderRequest{
		Source: source, ApplyIgnore: applyIgnore, SubPath: ks.Path, Spec: ks, HeldSlot: c.Tasks,
		SkipKinds: c.SkipKinds, DependencySubstitutions: c.DependencySubstitutions, WipeSecrets: c.WipeSecrets, StrictSubstitutions: c.strictSubstitutions,
	}, c.retry)
	if err != nil {
		return err
	}
	if source.Content == "" {
		// Mutable reads MUST be validated before replay can suppress readiness transitions.
		var handled bool
		sourceContent.Output = docs
		fp, handled, err = c.run.FingerprintDedup(ctx, id, ks.Content(sourceContent), &attempt)
		if handled || err != nil {
			if err != nil {
				return err
			}
			return c.Trees.RecordRender(ctx, kustomize.RenderAcceptance{Owner: id, Receipt: attempt.Render, Entry: ks.EntryReceipt, Records: attempt.Footprint(), Source: source.HistoryIdentity(ks.SourceID()), Sources: attempt.SourcePaths(), DependencySubstitutions: c.DependencySubstitutions})
		}
	}
	if err := kustomize.Shape(ks, docs, c.strictSubstitutions); err != nil {
		return err
	}

	attempt.Children, attempt.Manifests, attempt.Failures = c.ParseChildren(ks, docs, manifest.ParseDocOptions{WipeSecrets: c.WipeSecrets})
	attempt.Fingerprint, attempt.Complete, attempt.Render = fp, true, receipt
	if err := c.run.Emit(ctx, id, &attempt, nil); err != nil {
		return err
	}
	return c.Trees.RecordRender(ctx, kustomize.RenderAcceptance{Owner: id, Receipt: attempt.Render, Entry: ks.EntryReceipt, Records: attempt.Footprint(), Source: source.HistoryIdentity(ks.SourceID()), Sources: attempt.SourcePaths(), DependencySubstitutions: c.DependencySubstitutions})
}

// PreviousRender prepares the immutable candidate with captured inputs without reconciling or acquiring sources.
func (c *Controller) PreviousRender(ctx context.Context, obj manifest.BaseManifest, reads *store.ReadSet) (iter.Seq2[manifest.NamedResource, manifest.DiscoveryRecord], bool) {
	ks, ok := obj.(*manifest.Kustomization)
	if !ok || ctx.Err() != nil {
		return nil, false
	}
	data := values.NewStoreProvider(reads, c.WipeSecrets)
	waiter := depwait.Waiter{Store: reads, Parent: reads.ID(), AllowMissingCRDs: c.MissingCRDsAllowed()}
	for ref := range manifest.References(ks, manifest.ReferenceRelations) {
		if ref.Use != "substituteFrom" {
			continue
		}
		availability := waiter.Available(ctx, depwait.DataDependency{DependencyRef: ref.DependencyRef, Input: data.Lookup(ref.NamedResource), Optional: ref.Optional}, manifest.DrainNone)
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
	records, found := c.Trees.PreviousRender(ctx, kustomize.RenderRequest{
		Source: source, ApplyIgnore: ignore, SubPath: prepared.Path, Spec: prepared,
		SkipKinds: c.SkipKinds, DependencySubstitutions: c.DependencySubstitutions, WipeSecrets: c.WipeSecrets, StrictSubstitutions: c.strictSubstitutions,
	}, ks.Named())
	if !found {
		return nil, false
	}
	return records, ks.PostBuild == nil || len(ks.PostBuild.SubstituteFrom) == 0
}
