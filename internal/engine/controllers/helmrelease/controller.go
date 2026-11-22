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
	index   *manifest.DiscoveryIndex
}

type execution struct {
	*Controller

	base.Preparation[helm.Preparation]

	run    base.Execution
	replay *helm.RenderAdmission
}

func (c *Controller) newExecution(id manifest.NamedResource) *execution {
	execution := &execution{Controller: c}
	execution.run.Init(c.Controller, id)
	return execution
}

// New requires that the index MUST remain immutable throughout controller dispatch.
func New(controller *base.Controller, h *helm.Client, opts helm.Options, index *manifest.DiscoveryIndex) *Controller {
	return &Controller{
		Controller: controller,
		Helm:       h,
		Options:    opts,
		index:      index,
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
	hr, input, err := c.renderRequest(ctx, hr, inputs, &level)
	if err != nil {
		return err
	}
	omitted, unreadable := input.Omitted, values.UnreadableValueReferences(inputs)

	var (
		fp      string
		attempt store.Emission
		handled bool
		replay  helm.RenderAdmission
	)
	defer attempt.Finish()
	input.Admit = func(admission helm.RenderAdmission) (bool, error) {
		if c.admitQualification(admission) {
			return true, nil
		}
		attempt.Render = admission.Receipt
		replay = admission
		if feedbackErr := c.run.CheckRenderFeedback(admission.Key); feedbackErr != nil {
			return false, feedbackErr
		}
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
		return admission.Accepted, nil
	}
	fetch, err := c.Helm.PrepareChart(ctx, hr, &input)
	if err != nil {
		return err
	}
	if fetch != nil {
		return c.run.Acquire(ctx, hr, base.Acquisition{
			Fetch:    fetch,
			Complete: c.chartDependencyCompletion(hr, inputs, input.Preparation),
		}, level)
	}
	input.Previous, c.replay = c.replay, nil
	docs, receipt, err := c.Helm.Template(ctx, hr, hr.Values, input)
	if _, rendering := errors.AsType[*helm.RenderError](err); rendering {
		return c.run.RenderFailure(ctx, err, nil, unreadable)
	}
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
			Resource: hr.Named(),
			Category: manifest.WarnStaleValues,
			Message:  "values not used by the chart",
			Detail:   stale,
		})
	}
	if !handled {
		c.parseRendered(hr, docs, &attempt)
		attempt.Fingerprint, attempt.Complete, attempt.Render = fp, true, receipt
		if emitErr := c.emitRendered(ctx, hr.Named(), &attempt, unreadable); emitErr != nil {
			return emitErr
		}
	}
	c.run.AcceptRenderFeedback(replay.Key, &attempt)
	return c.recordRender(ctx, hr.Named(), &attempt)
}

func (c *execution) parseRendered(hr *manifest.HelmRelease, docs []manifest.DocOccurrence, attempt *store.Emission) {
	base.PrepareDocuments(hr, docs)
	children := c.run.ParseChildren(hr, docs, manifest.ParseDocOptions{})
	attempt.Children, attempt.Manifests, attempt.Failures = children.Children, children.Visible, children.Failures
}

func (c *execution) emitRendered(
	ctx context.Context,
	id manifest.NamedResource,
	attempt *store.Emission,
	unreadable []manifest.Reference,
) error {
	if err := c.run.RenderFailure(ctx, nil, attempt.Failures, unreadable); errors.Is(err, manifest.ErrSourceSkipped) {
		return err
	}
	return c.run.Emit(ctx, id, attempt, nil)
}

func (c *execution) admitQualification(admission helm.RenderAdmission) bool {
	if c.run.EntryReplay && admission.Accepted {
		c.run.Disposition = store.AssessmentReused
	}
	return c.run.AdmitQualification(admission.Possible)
}

func (c *execution) renderRequest(
	ctx context.Context,
	hr *manifest.HelmRelease,
	inputs []values.Result,
	reconcileLevel *manifest.DrainLevel,
) (*manifest.HelmRelease, helm.RenderOptions, error) {
	if reconcileLevel != nil {
		c.SetPendingUnlessReady(hr.Named(), "resolving chart")
	}
	sources := helm.NewSources(&c.run.Reads, c.index)
	prepared, err := helm.Prepare(hr, sources.Declaration, inputs, c.Helm.ValuesCache())
	if err != nil {
		return nil, helm.RenderOptions{}, err
	}
	// Source readiness MUST precede fingerprint reuse to prevent replay from failed or skipped sources.
	if reconcileLevel != nil {
		if _, err := c.run.RequireRefresh(ctx, prepared, manifest.GateArtifacts, *reconcileLevel); err != nil {
			return nil, helm.RenderOptions{}, err
		}
	}
	request := helm.RenderOptions{
		Work:       c.run.Reads.Work().Scope().RenderWork(),
		Options:    c.Options,
		Sources:    sources,
		Omitted:    values.OmittedIDs(inputs),
		EntryReach: c.run.QualifiedEntryReach(),
	}
	// Replay history can run inside a reconcile task that already holds a worker slot.
	if reconcileLevel == nil && !c.run.EntryReplay {
		request.PreparationTasks = c.Tasks
	}
	request.Preparation = c.run.Prepare(&c.Preparation, nil, func(p *helm.Preparation) bool {
		return p.Matches(c.Helm, prepared, &request)
	})
	c.run.RetainPreparation = true
	return prepared, request, nil
}

func (c *execution) chartDependencyCompletion(
	hr *manifest.HelmRelease,
	inputs []values.Result,
	preparation *helm.Preparation,
) func(context.Context, *store.SourceArtifact, error, manifest.DrainLevel) error {
	return func(ctx context.Context, artifact *store.SourceArtifact, unavailable error, level manifest.DrainLevel) error {
		if unavailable != nil {
			return manifest.NewError(manifest.ErrSourceSkipped, unavailable, "")
		}
		if err := preparation.AcceptChartDependency(ctx, artifact); err != nil {
			return err
		}
		return c.render(ctx, hr, level, inputs)
	}
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
	ex := execution{Controller: c}
	history, results, err := ex.run.Probe(ctx, c.Controller, obj, previous)
	if ex.run.EntryReplay {
		ex.replay = new(helm.RenderAdmission)
		history.Replay = func(ctx context.Context, level manifest.DrainLevel) *base.Execution {
			ex.Controller = c
			ex.run.PromoteProbe()
			ex.run.Dispatch(ctx, level, &ex, (*execution).reconcile)
			return &ex.run
		}
	}
	if err != nil {
		return history
	}
	hr, request, err := ex.renderRequest(ctx, hr, results, nil)
	if err != nil {
		return history
	}
	request.Previous = ex.replay
	records, found := c.Helm.PreviousRender(ctx, hr, hr.Values, request)
	if !found {
		return history
	}
	history.Records, history.Complete = records, true
	return history
}
