// Package helmrelease implements the HelmReleaseController.
package helmrelease

import (
	"context"
	"fmt"
	"slices"

	"github.com/home-operations/flate/pkg/controllers/base"
	"github.com/home-operations/flate/pkg/depwait"
	"github.com/home-operations/flate/pkg/helm"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source/helmchart"
	"github.com/home-operations/flate/pkg/store"
	"github.com/home-operations/flate/pkg/values"
)

// Controller orchestrates HelmRelease reconciliation. Reconcile-shaping
// state (Filter, ParentOf) flows in via Configure exactly once before Start.
type Controller struct {
	*base.Controller

	Helm *helm.Client

	// Options applied to every template call.
	Options helm.Options

	// WipeSecrets controls whether secrets are wiped from rendered
	// templates.
	WipeSecrets bool

	// allowMissingSecrets extends the source-controller flag to
	// HelmRelease valuesFrom refs that cannot be resolved offline.
	allowMissingSecrets bool
	substitutionSources map[manifest.NamedResource]manifest.BaseManifest
}

// ReconcileOptions carries the post-bootstrap state the orchestrator wires
// onto the controller. base.Options holds the config common to every render
// controller (Filter / ParentOf — here resolving each HR to its enclosing KS,
// which reconcile depwaits on so cluster-KS spec patches land before the first
// helm.Template call — Existence / PreflightFailure);
// AllowMissingSecrets and SubstituteFrom are HelmRelease-specific.
type ReconcileOptions struct {
	base.Options
	// AllowMissingSecrets omits non-optional valuesFrom refs that point
	// at known live-cluster/generated data or fail to materialize offline.
	AllowMissingSecrets bool
	// SubstituteFrom is the immutable bootstrap-approved postBuild source snapshot.
	SubstituteFrom map[manifest.NamedResource]manifest.BaseManifest
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

// Configure installs the post-bootstrap state. Panics if called after
// Start — encodes the invariant that reconcile-shaping config is
// read-only once dispatch begins.
func (c *Controller) Configure(opts ReconcileOptions) {
	c.Controller.Configure(opts.Options)
	c.allowMissingSecrets = opts.AllowMissingSecrets
	c.substitutionSources = opts.SubstituteFrom
}

// Start starts the common controller lifecycle.
func (c *Controller) Start(_ context.Context) {
	c.StartLifecycle()
}

// ReconcileNode runs id's reconcile under the dag engine, returning the blocked
// dependency set (nil = terminalized). The
// orchestrator's scheduler Dispatcher calls this for HelmRelease nodes.
func (c *Controller) ReconcileNode(ctx context.Context, id manifest.NamedResource, level manifest.DrainLevel) *base.Execution {
	execution := c.newExecution(id)
	execution.run.Dispatch(ctx, level, execution.reconcile)
	return &execution.run
}

func (c *execution) reconcile(ctx context.Context, hr *manifest.HelmRelease, level manifest.DrainLevel) error {
	id := hr.Named()
	if err := c.run.PreflightError(id); err != nil {
		return err
	}

	for _, production := range c.run.Reads.Productions(id, nil) {
		parent := production.Owner
		if parent == (manifest.NamedResource{}) || parent == id {
			continue
		}
		c.SetPendingUnlessReady(id, "waiting for parent KS")
		err := c.run.Require(ctx, []manifest.DependencyRef{{NamedResource: parent}}, level)
		if err != nil {
			return err
		}
	}
	if err := c.run.PreflightError(id); err != nil {
		return err
	}

	if deps := c.collectDeps(hr); len(deps) > 0 {
		c.SetPendingUnlessReady(id, "resolving dependencies")
		err := c.run.Require(ctx, deps, level)
		if err != nil {
			return err
		}
	}
	if err := c.run.PreflightError(id); err != nil {
		return err
	}

	return c.resolvePreRenderValuesFrom(ctx, hr, level)
}

func (c *execution) render(ctx context.Context, hr *manifest.HelmRelease, level manifest.DrainLevel, omitted []manifest.NamedResource) error {
	id := hr.Named()
	if err := c.run.PreflightError(id); err != nil {
		return err
	}

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
	if err := c.awaitChartSource(ctx, hr, level); err != nil {
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
	hr, repointed := c.materializeHelmChartSource(id, hr)
	if repointed {
		if err := c.awaitChartSource(ctx, hr, level); err != nil {
			return err
		}
	}
	if err := c.run.PreflightError(id); err != nil {
		return err
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
		_, attempt = c.run.Reads.GetEmission(id)
		c.Store.UpdateStatus(id, store.StatusPending, "", "rendering chart")
		if err := c.run.PreflightError(id); err != nil {
			handled = true
			return true, err
		}
		return false, nil
	}
	raw, err := c.Helm.Template(ctx, hr, hr.Values, input)
	if handled {
		return err
	}
	var docs []map[string]any
	if err == nil {
		docs, err = manifest.SplitDocs([]byte(raw))
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
	// Render-output pipeline: flatten List wrappers first so the
	// post-render passes stamp the items, then apply commonMetadata and
	// origin labels (origin last so it wins on key collisions), then drop
	// skipped kinds — mirroring helm-controller's order.
	docs = manifest.FlattenLists(docs)
	helm.ApplyHRCommonMetadata(docs, hr.CommonMetadata)
	helm.ApplyHROriginLabels(docs, hr)
	docs = manifest.DropKinds(docs, c.Options.SkipResourceKinds())

	c.Store.UpdateStatus(id, store.StatusPending, "", fmt.Sprintf("applying %d objects", len(docs)))
	if err := c.run.PreflightError(id); err != nil {
		return err
	}
	var synthetic []store.Child
	for _, child := range attempt.Children {
		if child.Doc == nil {
			synthetic = append(synthetic, child)
		}
	}
	attempt.Children, attempt.Manifests = base.ParseChildren(id, docs, manifest.ParseDocOptions{WipeSecrets: c.WipeSecrets, DefaultNamespace: hr.ReleaseNamespace()})
	attempt.Children = append(attempt.Children, synthetic...)
	attempt.Fingerprint, attempt.Complete = fp, true
	c.run.Emit(id, &attempt)
	return nil
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
	raw, err := c.Helm.Template(ctx, hr, filled, input)
	if err != nil {
		return nil, nil, false
	}
	docs, err := manifest.SplitDocs([]byte(raw))
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
			if c.substitutionSources[ref] == nil {
				return true
			}
		}
	}
	return false
}

// collectDeps selects ordering gates in one walk, pruning only known unchanged refs.
func (c *execution) collectDeps(hr *manifest.HelmRelease) []manifest.DependencyRef {
	var deps []manifest.DependencyRef
	if len(hr.DependsOn) > 0 {
		deps = make([]manifest.DependencyRef, 0, len(hr.DependsOn))
	}
	manifest.References(hr)(func(ref manifest.Reference) bool {
		if ref.Use == "dependsOn" && c.run.KeepOrderingDep(ref.NamedResource) {
			deps = append(deps, ref.DependencyRef)
		}
		return true
	})
	return deps
}

// awaitChartSource prefers the resolved source to the authored chart and propagates
// source soft skips. Require parks on fetch completion before chart lookup.
func (c *execution) awaitChartSource(ctx context.Context, hr *manifest.HelmRelease, level manifest.DrainLevel) error {
	var deps []manifest.DependencyRef
	var chart manifest.DependencyRef
	for ref := range manifest.References(hr) {
		switch ref.Use {
		case "source":
			deps = append(deps, ref.DependencyRef)
		case "chart":
			chart = ref.DependencyRef
		}
	}
	if len(deps) == 0 {
		deps = append(deps, chart)
	}
	if err := c.run.Require(ctx, deps, level); err != nil {
		return err
	}
	for _, dep := range deps {
		if err := c.run.SourceSkipped(dep.NamedResource, "chart source"); err != nil {
			return err
		}
	}
	for _, dep := range deps {
		if dep.Group == manifest.SourceDomain && dep.Kind == manifest.KindHelmRepository {
			continue
		}
		art, _ := c.run.Reads.GetArtifact(dep.NamedResource).(*store.SourceArtifact)
		if art == nil || art.FS == nil {
			if level == manifest.DrainNone {
				return &depwait.ErrBlocked{Deps: []manifest.NamedResource{dep.NamedResource}}
			}
			return fmt.Errorf("%w: chart source %s artifact content unavailable", manifest.ErrObjectNotFound, dep.NamedResource)
		}
	}
	return nil
}

// materializeHelmChartSource handles a HelmRelease whose chart source is a
// HelmRepository (HTTP or OCI). A HelmRepository is only a registry/index
// base; the chart name+version live on the HelmRelease, so there's no
// standalone HelmChart CR for the source controller to have fetched. We
// synthesize one, register it (so the source controller fetches the chart —
// with retry — into the Store), and repoint hr.Chart at it. After this, the
// chart-source depwait and chart loading route through the HelmChart path; the
// chart pull happens once through the single source path rather than inline.
//
// Returns (hr, true) when it repointed the chart to a synthetic HelmChart;
// (hr, false) with hr unchanged otherwise (the source isn't a resolvable
// HelmRepository). hr is the post-Prepare clone, so mutating its Chart is
// local to this reconcile and never touches the Store's object. Re-running on
// a later reconcile is idempotent: the source controller short-circuits an
// already-fetched artifact and AddObject of the same synthetic id is a no-op
// re-emit.
func (c *execution) materializeHelmChartSource(id manifest.NamedResource, hr *manifest.HelmRelease) (*manifest.HelmRelease, bool) {
	// RepoKind "" defaults to HelmRepository (see chart source dispatch).
	if hr.Chart.Source.Group != manifest.SourceDomain || hr.Chart.Source.Kind != "" && hr.Chart.Source.Kind != manifest.KindHelmRepository {
		return hr, false
	}
	r := helm.NewSources(&c.run.Reads).HelmRepository(hr.Chart.Source.Namespace, hr.Chart.Source.Name)
	if r == nil {
		return hr, false
	}
	syn := helmchart.Synthesize(r, hr.Chart.Name, hr.Chart.Version)
	// The incomplete generic publication makes C available before H waits for acquisition.
	_, attempt := c.run.Reads.GetEmission(id)
	attempt.Children = []store.Child{{Obj: syn}}
	c.run.Emit(id, &attempt)
	attempt.Finish()
	hr.Chart.Source = syn.Named()
	return hr, true
}
