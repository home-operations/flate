// Package helmrelease implements the HelmReleaseController.
package helmrelease

import (
	"context"
	"slices"

	"github.com/home-operations/flate/pkg/controllers/base"
	"github.com/home-operations/flate/pkg/helm"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source/helmchart"
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
	execution.data = values.NewStoreProvider(&execution.run.Reads, c.WipeSecrets, nil)
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
	hr, unavailable, err := helm.Prepare(hr, helm.NewSources(&c.run.Reads).HelmChart, c.data, c.Helm.ValuesCache())
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
	// via --allow-missing-secrets).
	if _, err := c.run.RequireRefresh(ctx, hr, manifest.GateArtifacts, level); err != nil {
		return err
	}
	// A HelmRepository is just a registry/index base — its chart has no
	// standalone source CR. Now that the declared source is Ready (so the
	// HelmRepository is in the Store and resolvable), repoint the chart to a
	// synthesized HelmChart the source controller fetches (retry + Store),
	// and wait for that fetch too. After this, render always resolves the
	// chart from the HelmChart artifact, never fetching inline. Waiting
	// first makes the repoint deterministic — no loop, no "absent → retry"
	// dance.
	hr, repointed, err := c.materializeHelmChartSource(id, hr)
	if err != nil {
		return err
	}
	if repointed {
		if _, err := c.run.RequireRefresh(ctx, hr, manifest.GateArtifacts, level); err != nil {
			return err
		}
	}

	var fp string
	var attempt store.Emission
	defer attempt.Finish()
	handled := false
	input := helm.RenderOptions{Options: c.Options, Sources: helm.NewSources(&c.run.Reads)}
	input.Admit = func(key string) (bool, error) {
		var content any
		if key != "" {
			content = hr.Content(key, omitted)
		}
		var err error
		fp, handled, err = c.run.FingerprintDedup(id, content)
		if handled || err != nil {
			return handled, err
		}
		attempt.Finish()
		_, _, attempt = c.run.Reads.GetEmission(id)
		return false, nil
	}
	docs, err := c.Helm.Template(ctx, hr, hr.Values, input)
	if handled {
		return err
	}
	if err != nil {
		// Best-effort rescue: if this HR's parent Kustomization couldn't read a
		// substituteFrom Secret offline, a required value the render needs may
		// have substituted to empty (e.g. cronjob.timeZone from ${CONFIG_TZ}).
		// Re-render with those empties filled by placeholders so the resource
		// stays VISIBLE in the diff instead of being dropped by the FAILED-HR
		// gate. Bounded: only failing HRs are touched, and only adopted if the
		// retry actually renders — a failure with a different cause (missing
		// container, chart 404) re-fails and falls through to the error below.
		if rescued, paths, ok := c.renderBestEffort(ctx, id, hr, input); ok {
			if handled {
				return nil
			}
			docs, err = rescued, nil
			c.run.Reads.AddWarning(id, manifest.Warning{
				Resource: id,
				Category: manifest.WarnUnresolvedSubstitution,
				Message:  "rendered best-effort: unresolved substitution values replaced with placeholders",
				Detail:   paths,
			})
		}
	}
	if err != nil {
		return err
	}
	// #744: flag top-level release values no chart template references — a key
	// renamed/removed when the chart was upgraded silently does nothing, so the
	// override is a stale no-op. Advisory only; rides Result.Warnings to the
	// footer + konflate. See StaleValuePaths.
	if stale := c.Helm.StaleValuePaths(ctx, hr, hr.Values, input.Sources); len(stale) > 0 {
		c.run.Reads.AddWarning(id, manifest.Warning{
			Resource: id,
			Category: manifest.WarnStaleValues,
			Message:  "values not used by the chart",
			Detail:   stale,
		})
	}
	base.Stamp(hr.EmissionPolicy(), docs)

	var synthetic []store.Child
	for _, child := range attempt.Children {
		if child.Doc == nil {
			synthetic = append(synthetic, child)
		}
	}
	attempt.Children, attempt.Manifests = c.ParseChildren(hr, docs, manifest.ParseDocOptions{WipeSecrets: c.WipeSecrets})
	attempt.Children = append(attempt.Children, synthetic...)
	attempt.Fingerprint, attempt.Complete = fp, true
	return c.run.Emit(id, &attempt, nil)
}

// renderBestEffort retries a failed chart render with empty values replaced by
// placeholders, scoped to HelmReleases whose parent Kustomization couldn't read
// a substituteFrom Secret offline — the signal that the failure is plausibly an
// unresolved secret-sourced value rather than a genuine misconfiguration. It
// adopts the retry only if it renders, returning the rescued docs and the
// filled value paths. ok=false when there's nothing to rescue (no unreadable
// parent secret, no empty leaves) or the retry still fails — the caller then
// surfaces the original error and the HR fails as before, so a non-empty cause
// (missing container, chart 404) is never masked.
func (c *execution) renderBestEffort(ctx context.Context, id manifest.NamedResource, hr *manifest.HelmRelease, input helm.RenderOptions) ([]map[string]any, []string, bool) {
	if !c.parentReadsUnreadableSecret(id) {
		return nil, nil, false
	}
	filled, paths := manifest.FillEmptyValueLeaves(hr.Values)
	if len(paths) == 0 {
		return nil, nil, false
	}
	docs, err := c.Helm.Template(ctx, hr, filled, input)
	if err != nil {
		return nil, nil, false
	}
	return docs, paths, true
}

func (c *execution) parentReadsUnreadableSecret(id manifest.NamedResource) bool {
	for _, p := range c.run.Reads.Productions(id, nil) {
		if p.Owner == (manifest.NamedResource{}) || p.Owner == id {
			continue
		}
		emission, _ := c.run.Reads.GetArtifact(p.Owner).(*store.Emission)
		if emission == nil {
			continue
		}
		for _, ref := range emission.Unreadable {
			if c.SubstituteFrom[ref] == nil {
				return true
			}
		}
	}
	return false
}

func (c *execution) materializeHelmChartSource(id manifest.NamedResource, hr *manifest.HelmRelease) (*manifest.HelmRelease, bool, error) {
	if !hr.Chart.Materializes() {
		return hr, false, nil
	}
	r := helm.NewSources(&c.run.Reads).HelmRepository(hr.Chart.Source.Namespace, hr.Chart.Source.Name)
	if r == nil {
		return hr, false, nil
	}
	syn := helmchart.Synthesize(r, hr.Chart.Name, hr.Chart.Version)
	// The incomplete generic publication makes C available before H waits for acquisition.
	_, _, attempt := c.run.Reads.GetEmission(id)
	attempt.Children = []store.Child{{Obj: syn}}
	defer attempt.Finish()
	if err := c.run.Emit(id, &attempt, nil); err != nil {
		return hr, false, err
	}
	hr.Chart.Source = syn.Named()
	return hr, true, nil
}
