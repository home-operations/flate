// Package helmrelease implements the HelmReleaseController.
package helmrelease

import (
	"context"
	"iter"
	"slices"

	"github.com/home-operations/flate/pkg/controllers/base"
	"github.com/home-operations/flate/pkg/depwait"
	"github.com/home-operations/flate/pkg/helm"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/schedule"
	"github.com/home-operations/flate/pkg/store"
	"github.com/home-operations/flate/pkg/values"
)

// Controller orchestrates HelmRelease reconciliation. Reconcile-shaping
// state (Filter, ParentOf) flows in via Configure exactly once before StartLifecycle.
type Controller struct {
	*base.Controller

	Helm *helm.Client

	// Options applied to every template call.
	Options helm.Options
}

type execution struct {
	*Controller

	run  base.Execution
	data interface {
		values.Provider
		Lookup(manifest.NamedResource) store.ReferenceFacts
	}
}

func (c *Controller) newExecution(id manifest.NamedResource) *execution {
	execution := &execution{Controller: c}
	execution.run.Init(c.Controller, id)
	execution.data = values.NewStoreProvider(&execution.run.Reads)
	return execution
}

// New constructs a HelmRelease controller.
func New(controller *base.Controller, h *helm.Client, opts helm.Options) *Controller {
	return &Controller{
		Controller: controller,
		Helm:       h,
		Options:    opts,
	}
}

// ReconcileNode runs id's reconcile under the dag engine, returning the blocked
// dependency set (nil = terminalized). The
// orchestrator's scheduler Dispatcher calls this for HelmRelease nodes.
func (c *Controller) ReconcileNode(
	ctx context.Context,
	id manifest.NamedResource,
	level manifest.DrainLevel,
) *base.Execution {
	ex := c.newExecution(id)
	ex.run.Dispatch(ctx, level, ex, (*execution).reconcile)
	return &ex.run
}

// Owns reports whether id belongs to the HelmRelease controller.
func (*Controller) Owns(id manifest.NamedResource) bool {
	return id.Group == manifest.HelmReleaseDomain && id.Kind == manifest.KindHelmRelease
}

func (c *execution) reconcile(ctx context.Context, hr *manifest.HelmRelease, level manifest.DrainLevel) error {
	var err error
	hr, err = c.run.RequireRefresh(ctx, hr, manifest.GateInputs, level)
	if err != nil {
		return err
	}
	return c.resolvePreRenderValuesFrom(ctx, hr, level)
}

func (c *execution) render(
	ctx context.Context,
	hr *manifest.HelmRelease,
	level manifest.DrainLevel,
	omitted []manifest.NamedResource,
) error {
	id := hr.Named()

	c.SetPendingUnlessReady(id, "resolving chart")
	hr, err := helm.Prepare(hr, helm.NewSources(&c.run.Reads).Declaration, c.data, c.Helm.ValuesCache())
	slices.SortFunc(omitted, manifest.NamedResource.Compare)
	omitted = slices.Compact(omitted)
	c.recordOmittedValues(id, omitted)
	if err != nil {
		return err
	}

	// Wait for the declared chart source to be Ready, BEFORE the fingerprint
	// dedup gate (a re-emit that finds the prior fingerprint cached must not
	// replay stale docs over a source that has since failed or soft-skipped
	// via --allow-missing-kinds Secret).
	if _, localErr := c.run.RequireRefresh(ctx, hr, manifest.GateArtifacts, level); localErr != nil {
		return localErr
	}

	var fp string
	var attempt store.Emission
	defer attempt.Finish()
	handled := false
	input := helm.RenderOptions{
		Work:                    c.run.Reads.Work().Scope().RenderWork(),
		Options:                 c.Options,
		DependencySubstitutions: c.DependencySubstitutions,
		Sources:                 helm.NewSources(&c.run.Reads),
	}
	input.Admit = func(key string) (bool, error) {
		var content any
		if key != "" {
			content = hr.Content(key, omitted)
		}
		var innerLocalErr error
		fingerprintDedupResult1, fingerprintDedupResult1Err := c.run.FingerprintDedup(ctx, id, content, &attempt)
		fp, handled, innerLocalErr = fingerprintDedupResult1.Fingerprint, fingerprintDedupResult1.Reused, fingerprintDedupResult1Err
		if handled || innerLocalErr != nil {
			return handled, innerLocalErr
		}
		attempt.Finish()
		_, _, attempt = c.run.Reads.GetEmission(id)
		return false, nil
	}
	docs, receipt, err := c.Helm.Template(ctx, hr, hr.Values, input)
	if err != nil {
		return err
	}
	if stale := c.Helm.StaleValuePaths(ctx, hr, hr.Values, &input); len(stale) > 0 {
		c.run.Reads.AddWarning(manifest.Warning{
			Resource: id,
			Category: manifest.WarnStaleValues,
			Message:  "values not used by the chart",
			Detail:   stale,
		})
	}
	if handled {
		return c.Helm.RecordRender(
			ctx,
			helm.RenderAcceptance{
				Owner:   id,
				Receipt: attempt.Render,
				Records: attempt.Footprint(),
				Sources: attempt.SourcePaths(),
				Work:    c.run.Reads.Work().Scope().RenderWork(),
			},
		)
	}
	hr.EmissionPolicy().Stamp(docs)

	parseChildrenResult2 := c.run.ParseChildren(
		hr,
		docs,
		manifest.ParseDocOptions{},
	)
	attempt.Children, attempt.Manifests, attempt.Failures = parseChildrenResult2.Children, parseChildrenResult2.Visible, parseChildrenResult2.Failures
	attempt.Fingerprint, attempt.Complete, attempt.Render = fp, true, receipt
	if err := c.run.Emit(ctx, id, &attempt, nil); err != nil {
		return err
	}
	return c.Helm.RecordRender(
		ctx,
		helm.RenderAcceptance{
			Owner:   id,
			Receipt: attempt.Render,
			Records: attempt.Footprint(),
			Sources: attempt.SourcePaths(),
			Work:    c.run.Reads.Work().Scope().RenderWork(),
		},
	)
}

// PreviousRender prepares the immutable candidate with captured values and admitted charts without runtime effects.
func (c *Controller) PreviousRender(
	ctx context.Context,
	obj manifest.BaseManifest,
	reads *store.ReadSet,
) (iter.Seq2[manifest.NamedResource, manifest.DiscoveryRecord], bool) {
	hr, ok := obj.(*manifest.HelmRelease)
	if !ok || ctx.Err() != nil {
		return nil, false
	}
	data := values.NewStoreProvider(reads)
	waiter := depwait.Waiter{Store: reads, Parent: reads.ID(), AllowMissingCRDs: c.MissingCRDsAllowed()}
	var omittedRefs []manifest.NamedResource
	for ref := range manifest.References(hr, manifest.ReferenceRelations) {
		if ref.Use != "valuesFrom" {
			continue
		}
		availability := waiter.Available(
			ctx,
			depwait.DataDependency{
				DependencyRef: ref.DependencyRef,
				Input:         data.Lookup(ref.NamedResource),
				Optional:      ref.Optional,
				AllowMissing:  slices.Contains(c.AllowMissingKinds, ref.Kind),
			},
			manifest.DrainNone,
		)
		if availability.Err != nil || availability.Kind != schedule.Terminal {
			return nil, false
		}
		if availability.Omit {
			omittedRefs = append(omittedRefs, ref.NamedResource)
		}
	}
	hr = omitValues(hr, omittedRefs)
	hr, err := helm.Prepare(hr, helm.NewSources(reads).Declaration, data, c.Helm.ValuesCache())
	if err != nil {
		return nil, false
	}
	records, found := c.Helm.PreviousRender(
		ctx,
		hr,
		hr.Values,
		helm.RenderOptions{
			Work:                    reads.Work().Scope().RenderWork(),
			Options:                 c.Options,
			DependencySubstitutions: c.DependencySubstitutions,
			Sources:                 helm.NewSources(reads),
			PreparationTasks:        c.Tasks,
		},
	)
	if !found {
		return nil, false
	}
	return records, true
}
