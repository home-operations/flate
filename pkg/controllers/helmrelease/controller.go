// Package helmrelease implements the HelmReleaseController.
package helmrelease

import (
	"context"
	"slices"

	"github.com/home-operations/flate/pkg/controllers/base"
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

	preparation helm.Preparation

	run  base.Execution
	data interface {
		values.Provider
		Lookup(manifest.NamedResource) store.ReferenceFacts
	}
}

func (c *Controller) newExecution(id manifest.NamedResource) *execution {
	execution := &execution{Controller: c}
	execution.run.Init(c.Controller, id)
	execution.run.Preparation = execution
	execution.data = values.NewStoreProvider(&execution.run.Reads)
	return execution
}

func (c *execution) Retire() {
	c.preparation.Retire()
	c.data = nil
}

func (c *execution) prepareRequest(declaration *manifest.HelmRelease, request *helm.RenderOptions) {
	if previous, ok := c.run.Preparation.(*execution); ok && previous != c {
		c.preparation.Take(&previous.preparation, c.Helm, declaration, request)
		previous.Retire()
	}
	c.run.Preparation = c
	request.Preparation = &c.preparation
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
	previous schedule.Result,
) *base.Execution {
	ex := c.newExecution(id)
	ex.run.Take(previous)
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

	var (
		fp      string
		attempt store.Emission
		handled bool
		replay  helm.RenderAdmission
	)
	defer attempt.Finish()
	input := helm.RenderOptions{
		Work:    c.run.Reads.Work().Scope().RenderWork(),
		Options: c.Options,
		Sources: helm.NewSources(&c.run.Reads),
		Omitted: omitted,
	}
	input.Admit = func(admission helm.RenderAdmission) (bool, error) {
		if c.run.AdmitQualification(admission.Possible) {
			return true, nil
		}
		attempt.Render = admission.Receipt
		var content any
		if admission.Key != "" {
			content = hr.Content(admission.Key, omitted)
		}
		fingerprintDedupResult1, innerLocalErr := c.run.FingerprintDedup(ctx, id, content, &attempt)
		fp, handled = fingerprintDedupResult1.Fingerprint, fingerprintDedupResult1.Reused
		if handled || innerLocalErr != nil {
			return handled, innerLocalErr
		}
		attempt.Finish()
		_, _, attempt = c.run.Reads.GetEmission(id)
		if admission.Accepted {
			replay = admission
			return true, nil
		}
		return false, nil
	}
	c.prepareRequest(hr, &input)
	docs, receipt, err := c.Helm.Template(ctx, hr, hr.Values, input)
	if err != nil {
		return err
	}
	if c.run.Disposition == store.AssessmentQualificationSettled {
		return nil
	}
	if replay.Accepted {
		docs, receipt = replay.Documents, replay.Receipt
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
		return c.recordRender(ctx, id, &attempt)
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
	return c.recordRender(ctx, id, &attempt)
}

func (c *execution) recordRender(ctx context.Context, id manifest.NamedResource, attempt *store.Emission) error {
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
	previous schedule.Result,
) base.History {
	hr, ok := obj.(*manifest.HelmRelease)
	if !ok || ctx.Err() != nil {
		return base.History{}
	}
	ex := &execution{Controller: c}
	ex.run.InitProbe(c.Controller, obj)
	ex.run.Preparation = ex
	ex.run.Take(previous)
	reads := &ex.run.Reads
	history := base.History{Assessment: &ex.run}
	data := values.NewStoreProvider(reads)
	ex.data = data
	inputs, err := ex.run.DataInputs(ctx, hr, data)
	if err != nil {
		return history
	}
	var omittedRefs []manifest.NamedResource
	if err = ex.run.AwaitData(ctx, base.DataWait{
		Inputs: inputs,
		Resume: func(_ context.Context, _ manifest.DrainLevel, omitted []manifest.NamedResource) error {
			omittedRefs = requiredValuesOmissions(hr, omitted)
			return nil
		},
	}, manifest.DrainNone); err != nil {
		return history
	}
	hr = omitValues(hr, omittedRefs)
	hr, err = helm.Prepare(hr, helm.NewSources(reads).Declaration, data, c.Helm.ValuesCache())
	if err != nil {
		return history
	}
	request := helm.RenderOptions{
		Work: reads.Work().Scope().RenderWork(), Options: c.Options,
		Sources: helm.NewSources(reads), PreparationTasks: c.Tasks,
		Omitted: omittedRefs,
	}
	ex.prepareRequest(hr, &request)
	records, found := c.Helm.PreviousRender(ctx, hr, hr.Values, request)
	if !found {
		return history
	}
	history.Records, history.Complete = records, true
	return history
}
