// Package helmrelease implements the HelmReleaseController.
//
// It listens for new HelmRelease objects and renders them via the helm
// SDK. The controller also maintains a chart-source index by listening
// for HelmRepository, OCIRepository, and GitRepository events: when an
// upstream source becomes Ready the helm client is told about it so
// subsequent template calls can resolve charts.
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
// AllowMissingSecrets, Producers and SubstituteFrom are HelmRelease-specific.
type ReconcileOptions struct {
	base.Options
	// AllowMissingSecrets omits non-optional valuesFrom refs that point
	// at known live-cluster/generated data or fail to materialize offline.
	AllowMissingSecrets bool
	// SubstituteFrom is the immutable bootstrap-approved postBuild source snapshot.
	SubstituteFrom map[manifest.NamedResource]manifest.BaseManifest
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

// Start registers the listeners. The controller runs until Close.
// The HR controller only listens for HelmRelease and HelmChartSource
// (the chart-ref index) — source-kind events (HelmRepository,
// OCIRepository, GitRepository, Bucket, ExternalArtifact) are now
// consumed lazily by helm.Client through its SourceResolver against
// the canonical Store. One fewer push-registry to keep in sync.
func (c *Controller) Start(_ context.Context) {
	c.StartLifecycle()
}

// ReconcileNode runs id's reconcile under the dag engine, returning the blocked
// dependency set (nil = terminalized). The
// orchestrator's scheduler Dispatcher calls this for HelmRelease nodes.
func (c *Controller) ReconcileNode(ctx context.Context, id manifest.NamedResource, level manifest.DrainLevel) []manifest.NamedResource {
	return c.DispatchNode(ctx, id, level, c.reconcile)
}

func (c *Controller) reconcile(ctx context.Context, hr *manifest.HelmRelease, level manifest.DrainLevel) error {
	id := hr.Named()
	if err := c.PreflightError(id); err != nil {
		return err
	}

	// Parent gate: if this HR was file-loaded under a Flux KS's
	// spec.path, wait for that KS to finish reconciling before
	// rendering. The KS may apply patches / replacements / postBuild
	// substitutions that mutate spec — without the wait, the first
	// render uses stale (pre-patch) values and a second render
	// follows once the KS controller emits the patched copy via
	// AddObject, doubling helm-template work for every HR under a
	// parent-patching chain (tholinka/home-ops's cluster KS applies
	// driftDetection / install.crds / upgrade strategy / rollback to
	// every HR, so all of them were hit by this).
	for _, production := range c.Store.Productions(id, nil) {
		parent := production.Owner
		if parent == (manifest.NamedResource{}) || parent == id {
			continue
		}
		c.Store.UpdateStatus(id, store.StatusPending, "", "waiting for parent KS")
		// The parent's render may have replaced this HR in the store
		// with a patched copy; re-read so the rest of reconcile uses
		// the canonical spec instead of the pre-patch snapshot we
		// were dispatched with.
		fresh, ok, err := c.RequireRefresh[*manifest.HelmRelease](
			ctx, id, []manifest.DependencyRef{{NamedResource: parent}}, level)
		if err != nil {
			return err
		}
		if ok {
			hr = fresh
		}
	}
	if err := c.PreflightError(id); err != nil {
		return err
	}

	// Honor spec.dependsOn — HR-to-HR ordering. Flux gates rendering on
	// each dependency reaching Ready before this HR reconciles.
	if deps := c.collectDeps(hr); len(deps) > 0 {
		c.Store.UpdateStatus(id, store.StatusPending, "", "resolving dependencies")
		// RequireRefresh fuses the dependsOn gate with the load-bearing
		// re-read: the parent KS may have re-rendered (e.g. its own
		// dependsOn cleared in the meantime, freeing up parent-render-time
		// substitutions that mutate our spec). Without that refresh, an HR
		// with explicit spec.dependsOn but no structural parent — or one
		// whose parent re-emitted us after the parent-gate cleared — keeps
		// the pre-mutation snapshot through chart resolution.
		fresh, ok, err := c.RequireRefresh[*manifest.HelmRelease](
			ctx, id, deps, level)
		if err != nil {
			return err
		}
		if ok {
			hr = fresh
		}
	}
	if err := c.PreflightError(id); err != nil {
		return err
	}

	hr, omitted, err := c.resolvePreRenderValuesFrom(ctx, hr, level)
	if err != nil {
		return err
	}
	if err := c.PreflightError(id); err != nil {
		return err
	}

	// SetPendingUnlessReady, not a raw UpdateStatus: this pre-dedup progress
	// write must not transiently downgrade an already-Ready HR on a no-op
	// re-run (re-emitted by its parent KS render — HR retains labels, so a
	// stamped re-emit re-runs). An HR that dependsOn this one has a
	// quiescence-bound wait that could re-read the transient Pending at a
	// transient pool drain and drop. The genuine-render downgrade at "rendering
	// chart" below (after the fingerprint dedup) stays unconditional. See
	// base.SetPendingUnlessReady (#657/#658).
	c.SetPendingUnlessReady(id, "resolving chart")
	hr, err = helm.Prepare(hr, c.Helm.Resolver().HelmChart, values.NewStoreProvider(c.Store, c.WipeSecrets), c.Helm.ValuesCache())
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
	if err := c.PreflightError(id); err != nil {
		return err
	}

	var sourceContent any
	if hr.UsesDirectOCIRepository() {
		source := c.Helm.Resolver().LocalSourceArtifact(hr.ChartSource())
		if source == nil {
			return fmt.Errorf("resolve oci chart identity: %w: OCIRepository %s artifact not available",
				manifest.ErrObjectNotFound, hr.Chart.RepoFullName())
		}
		sourceContent = struct{ Digest, Revision string }{source.Digest, source.Revision}
	}
	fp, handled, err := c.FingerprintDedup(id, hr.Content(sourceContent, omitted))
	if handled {
		return err
	}

	_, attempt := c.Store.GetEmission(id)
	defer attempt.Finish()
	c.Store.UpdateStatus(id, store.StatusPending, "", "rendering chart")
	if err := c.PreflightError(id); err != nil {
		return err
	}
	raw, err := c.Helm.Template(ctx, hr, hr.Values, c.Options)
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
		if rescued, paths, ok := c.renderBestEffort(ctx, id, hr); ok {
			docs, err = rescued, nil
			c.Store.AddWarning(id, manifest.Warning{
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
	if stale := c.Helm.StaleValuePaths(ctx, hr, hr.Values); len(stale) > 0 {
		c.Store.AddWarning(id, manifest.Warning{
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
	if err := c.PreflightError(id); err != nil {
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
	c.Emit(id, &attempt)
	c.recordOmittedValues(id, omitted)
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
func (c *Controller) renderBestEffort(ctx context.Context, id manifest.NamedResource, hr *manifest.HelmRelease) ([]map[string]any, []string, bool) {
	if !c.parentReadsUnreadableSecret(id) {
		return nil, nil, false
	}
	filled, paths := manifest.FillEmptyValueLeaves(hr.Values)
	if len(paths) == 0 {
		return nil, nil, false
	}
	raw, err := c.Helm.Template(ctx, hr, filled, c.Options)
	if err != nil {
		return nil, nil, false
	}
	docs, err := manifest.SplitDocs([]byte(raw))
	if err != nil {
		return nil, nil, false
	}
	return docs, paths, true
}

// parentReadsUnreadableSecret checks each contributing Kustomization for a
// substituteFrom Secret flate could not read offline — the
// signal that an empty value in id's render likely came from an unresolved
// ${VAR} rather than a genuine empty-field mistake.
func (c *Controller) parentReadsUnreadableSecret(id manifest.NamedResource) bool {
	provider := values.NewStoreProvider(c.Store, c.WipeSecrets)
	for _, p := range c.Store.Productions(id, nil) {
		if p.Owner == (manifest.NamedResource{}) || p.Owner == id {
			continue
		}
		ks, ok := c.Store.GetObject(p.Owner).(*manifest.Kustomization)
		if !ok {
			continue
		}
		if len(c.substitutionSources) > 0 && ks.PostBuild != nil {
			var positions []int
			for ref := range manifest.References(ks) {
				if ref.Use == "substituteFrom" && c.substitutionSources[ref.NamedResource] != nil {
					positions = append(positions, ref.Position)
				}
			}
			if len(positions) > 0 {
				owned := ks.Clone()
				for _, position := range slices.Backward(positions) {
					owned.PostBuild.SubstituteFrom = slices.Delete(owned.PostBuild.SubstituteFrom, position, position+1)
				}
				ks = owned
			}
		}
		if len(values.UnreadableSubstituteSecrets(ks, provider)) > 0 {
			return true
		}
	}
	return false
}

// collectDeps selects ordering gates in one walk, pruning only known unchanged refs.
func (c *Controller) collectDeps(hr *manifest.HelmRelease) []manifest.DependencyRef {
	var deps []manifest.DependencyRef
	if len(hr.DependsOn) > 0 {
		deps = make([]manifest.DependencyRef, 0, len(hr.DependsOn))
	}
	manifest.References(hr)(func(ref manifest.Reference) bool {
		if ref.Use == "dependsOn" && c.KeepOrderingDep(ref.NamedResource) {
			deps = append(deps, ref.DependencyRef)
		}
		return true
	})
	return deps
}

// awaitChartSource prefers the resolved source to the authored chart and propagates
// source soft skips. Require parks on fetch completion before chart lookup.
func (c *Controller) awaitChartSource(ctx context.Context, hr *manifest.HelmRelease, level manifest.DrainLevel) error {
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
	if err := c.Require(ctx, hr.Named(), deps, level); err != nil {
		return err
	}
	for _, dep := range deps {
		if err := c.SourceSkipped(dep.NamedResource, "chart source"); err != nil {
			return err
		}
	}
	for _, dep := range deps {
		if dep.Group == manifest.SourceDomain && dep.Kind == manifest.KindHelmRepository {
			continue
		}
		art, _ := c.Store.GetArtifact(dep.NamedResource).(*store.SourceArtifact)
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
func (c *Controller) materializeHelmChartSource(id manifest.NamedResource, hr *manifest.HelmRelease) (*manifest.HelmRelease, bool) {
	// RepoKind "" defaults to HelmRepository (see chart source dispatch).
	if hr.Chart.RepoGroup != manifest.SourceDomain || hr.Chart.RepoKind != "" && hr.Chart.RepoKind != manifest.KindHelmRepository {
		return hr, false
	}
	r := c.Helm.Resolver().HelmRepository(hr.Chart.RepoNamespace, hr.Chart.RepoName)
	if r == nil {
		return hr, false
	}
	syn := helmchart.Synthesize(r, hr.Chart.Name, hr.Chart.Version)
	// The incomplete generic publication makes C available before H waits for acquisition.
	_, attempt := c.Store.GetEmission(id)
	attempt.Children = []store.Child{{Obj: syn}}
	c.Emit(id, &attempt)
	attempt.Finish()
	hr.Chart.RepoKind = manifest.KindHelmChart
	hr.Chart.RepoGroup = syn.Named().Group
	hr.Chart.RepoNamespace = syn.Namespace
	hr.Chart.RepoName = syn.Name
	return hr, true
}

// preparePrereqs gates synchronous Prepare lookups against concurrent parent emits.
// Optional values are excluded; chart sources needing fetch readiness are gated later.
func preparePrereqs(hr *manifest.HelmRelease) []manifest.DependencyRef {
	var out []manifest.DependencyRef
	for ref := range manifest.References(hr) {
		switch ref.Use {
		case "chart", "source":
			if ref.Kind == manifest.KindHelmChart && hr.Chart.Version == "" {
				out = append(out, ref.DependencyRef)
			}
		case "valuesFrom":
			if !ref.Optional {
				out = append(out, ref.DependencyRef)
			}
		}
	}
	return out
}
