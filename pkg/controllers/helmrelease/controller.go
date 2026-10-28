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

	// WipeSecrets controls whether secrets are wiped from rendered
	// templates.
	WipeSecrets bool
}

type execution struct {
	*Controller
	run  base.Execution
	data interface {
		values.Provider
		Lookup(manifest.NamedResource) values.Input
	}
}

func (c *Controller) newExecution(id manifest.NamedResource) *execution {
	execution := &execution{Controller: c}
	execution.run.Init(c.Controller, id)
	execution.data = values.NewStoreProvider(&execution.run.Reads, c.WipeSecrets)
	return execution
}

// New constructs a HelmRelease controller.
func New(controller *base.Controller, h *helm.Client, opts helm.Options, wipeSecrets bool) *Controller {
	return &Controller{
		Controller:  controller,
		Helm:        h,
		Options:     opts,
		WipeSecrets: wipeSecrets,
	}
}

// ReconcileNode runs id's reconcile under the dag engine, returning the blocked
// dependency set (nil = terminalized). The
// orchestrator's scheduler Dispatcher calls this for HelmRelease nodes.
func (c *Controller) ReconcileNode(ctx context.Context, id manifest.NamedResource, level manifest.DrainLevel) *base.Execution {
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

func (c *execution) render(ctx context.Context, hr *manifest.HelmRelease, level manifest.DrainLevel, omitted []manifest.NamedResource) error {
	id := hr.Named()

	c.SetPendingUnlessReady(id, "resolving chart")
	hr, unavailable, err := helm.Prepare(hr, helm.NewSources(&c.run.Reads).Declaration, c.data, c.Helm.ValuesCache())
	omitted = append(omitted, unavailable...)
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
	if _, err := c.run.RequireRefresh(ctx, hr, manifest.GateArtifacts, level); err != nil {
		return err
	}

	var fp string
	var attempt store.Emission
	defer attempt.Finish()
	handled := false
	input := helm.RenderOptions{Options: c.Options, DependencySubstitutions: c.DependencySubstitutions, Sources: helm.NewSources(&c.run.Reads), WipeSecrets: c.WipeSecrets}
	input.Admit = func(key string) (bool, error) {
		var content any
		if key != "" {
			content = hr.Content(key, omitted)
		}
		var err error
		fp, handled, err = c.run.FingerprintDedup(ctx, id, content, &attempt)
		if handled || err != nil {
			return handled, err
		}
		attempt.Finish()
		_, _, attempt = c.run.Reads.GetEmission(id)
		return false, nil
	}
	docs, receipt, err := c.Helm.Template(ctx, hr, hr.Values, input)
	if err != nil {
		return err
	}
	if stale := c.Helm.StaleValuePaths(ctx, hr, hr.Values, input.Sources); len(stale) > 0 {
		c.run.Reads.AddWarning(manifest.Warning{
			Resource: id,
			Category: manifest.WarnStaleValues,
			Message:  "values not used by the chart",
			Detail:   stale,
		})
	}
	if handled {
		return c.Helm.RecordRender(ctx, id, attempt.Render, attempt.Footprint())
	}
	hr.EmissionPolicy().Stamp(docs)

	attempt.Children, attempt.Manifests, attempt.Failures = c.ParseChildren(hr, docs, manifest.ParseDocOptions{WipeSecrets: c.WipeSecrets})
	attempt.Fingerprint, attempt.Complete, attempt.Render = fp, true, receipt
	if err := c.run.Emit(ctx, id, &attempt, nil); err != nil {
		return err
	}
	return c.Helm.RecordRender(ctx, id, attempt.Render, attempt.Footprint())
}

// PreviousRender prepares the immutable candidate with captured values and admitted charts without runtime effects.
func (c *Controller) PreviousRender(ctx context.Context, obj manifest.BaseManifest, reads *store.ReadSet) (iter.Seq2[manifest.NamedResource, manifest.DiscoveryRecord], bool) {
	hr, ok := obj.(*manifest.HelmRelease)
	if !ok || ctx.Err() != nil {
		return nil, false
	}
	data := values.NewStoreProvider(reads, c.WipeSecrets)
	waiter := depwait.Waiter{Store: reads, Parent: reads.ID(), AllowMissingCRDs: c.MissingCRDsAllowed()}
	var omittedRefs []manifest.NamedResource
	for ref := range manifest.References(hr, manifest.ReferenceRelations) {
		if ref.Use != "valuesFrom" {
			continue
		}
		availability := waiter.Available(ctx, depwait.DataDependency{DependencyRef: ref.DependencyRef, Input: data.Lookup(ref.NamedResource), Optional: ref.Optional, AllowMissing: slices.Contains(c.AllowMissingKinds, ref.Kind)}, manifest.DrainNone)
		if availability.Err != nil || availability.Kind != schedule.Terminal {
			return nil, false
		}
		if availability.Omit {
			omittedRefs = append(omittedRefs, ref.NamedResource)
		}
	}
	hr = omitValues(hr, omittedRefs)
	hr, omitted, err := helm.Prepare(hr, helm.NewSources(reads).Declaration, data, c.Helm.ValuesCache())
	if err != nil || len(omitted) != 0 {
		return nil, false
	}
	records, found := c.Helm.PreviousRender(ctx, hr, hr.Values, helm.RenderOptions{Options: c.Options, DependencySubstitutions: c.DependencySubstitutions, Sources: helm.NewSources(reads), WipeSecrets: c.WipeSecrets})
	if !found {
		return nil, false
	}
	return records, true
}
