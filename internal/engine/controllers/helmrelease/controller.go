// Package helmrelease implements the HelmReleaseController.
package helmrelease

import (
	"context"
	"errors"

	"github.com/home-operations/flate/internal/engine/controllers/base"
	"github.com/home-operations/flate/internal/engine/helm"
	"github.com/home-operations/flate/internal/engine/schedule"
	"github.com/home-operations/flate/internal/engine/source/cache"
	"github.com/home-operations/flate/internal/engine/store"
	"github.com/home-operations/flate/internal/engine/values"
	"github.com/home-operations/flate/pkg/manifest"
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

	base.Preparation[helm.Preparation]

	run base.Execution
}

func (c *Controller) newExecution(id manifest.NamedResource) *execution {
	execution := &execution{Controller: c}
	execution.run.Init(c.Controller, id)
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
	inputs []values.Result,
) error {
	c.SetPendingUnlessReady(hr.Named(), "resolving chart")
	hr, err := helm.Prepare(hr, helm.NewSources(&c.run.Reads).Declaration, inputs, c.Helm.ValuesCache())
	omitted, unreadable := values.OmittedIDs(inputs), values.UnreadableValueReferences(inputs)
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
		fingerprintDedupResult1, innerLocalErr := c.run.FingerprintDedup(ctx, hr.Named(), content, &attempt)
		fp, handled = fingerprintDedupResult1.Fingerprint, fingerprintDedupResult1.Reused
		if handled || innerLocalErr != nil {
			return handled, innerLocalErr
		}
		attempt.Finish()
		_, _, attempt = c.run.Reads.GetEmission(hr.Named())
		if admission.Accepted {
			replay = admission
			return true, nil
		}
		return false, nil
	}
	input.Preparation = c.run.Prepare(&c.Preparation, nil, func(p *helm.Preparation) bool {
		return p.Matches(c.Helm, hr, &input)
	})
	docs, receipt, err := c.Helm.Template(ctx, hr, hr.Values, input)
	if err != nil {
		if _, rendering := errors.AsType[*helm.RenderError](err); rendering {
			return c.run.RenderFailure(ctx, err, nil, unreadable)
		}
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
			Resource: hr.Named(),
			Category: manifest.WarnStaleValues,
			Message:  "values not used by the chart",
			Detail:   stale,
		})
	}
	if handled {
		return c.recordRender(ctx, hr.Named(), &attempt)
	}

	hr.EmissionPolicy().Stamp(docs)

	parseChildrenResult2 := c.run.ParseChildren(
		hr,
		docs,
		manifest.ParseDocOptions{},
	)
	attempt.Children, attempt.Manifests, attempt.Failures =
		parseChildrenResult2.Children, parseChildrenResult2.Visible, parseChildrenResult2.Failures
	if err := c.run.RenderFailure(ctx, nil, attempt.Failures, unreadable); errors.Is(err, manifest.ErrSourceSkipped) {
		return err
	}
	attempt.Fingerprint, attempt.Complete, attempt.Render = fp, true, receipt
	if err := c.run.Emit(ctx, hr.Named(), &attempt, nil); err != nil {
		return err
	}
	return c.recordRender(ctx, hr.Named(), &attempt)
}

func (c *execution) recordRender(ctx context.Context, id manifest.NamedResource, attempt *store.Emission) error {
	return c.Cache.RecordRender(
		ctx,
		cache.RenderAcceptance{
			Owner:   id,
			Receipt: attempt.Render,
			Records: attempt.Footprint(),
			Reads:   attempt.Reads, OutputReads: attempt.OutputReads,
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
	var ex execution
	history, results, err := ex.run.Probe(ctx, c.Controller, obj, previous)
	if err != nil {
		return history
	}
	reads := &ex.run.Reads
	hr, err = helm.Prepare(hr, helm.NewSources(reads).Declaration, results, c.Helm.ValuesCache())
	if err != nil {
		return history
	}
	request := helm.RenderOptions{
		Work: reads.Work().Scope().RenderWork(), Options: c.Options,
		Sources: helm.NewSources(reads), PreparationTasks: c.Tasks,
		Omitted: values.OmittedIDs(results),
	}
	request.Preparation = ex.run.Prepare(&ex.Preparation, nil, func(p *helm.Preparation) bool {
		return p.Matches(c.Helm, hr, &request)
	})
	records, found := c.Helm.PreviousRender(ctx, hr, hr.Values, request)
	if !found {
		return history
	}
	history.Records, history.Complete = records, true
	return history
}
