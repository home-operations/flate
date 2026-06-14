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
	"github.com/home-operations/flate/pkg/task"
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

	// producers is the orchestrator-owned producer index, shared with the
	// source controller. It maps a target Secret to the in-repo
	// ExternalSecret / SealedSecret that declares it. A valuesFrom ref
	// whose Secret only materializes live (a declared producer exists) is
	// omitted from the offline render rather than waited on — even without
	// --allow-missing-secrets. Seeded at discovery and augmented by
	// onRawProducerAdded as KS renders emit ES/SS with post-transform
	// names. Nil-safe: an absent index reports no producers. See
	// manifest.ProducerIndex.
	producers *manifest.ProducerIndex
}

// ReconcileOptions carries the post-bootstrap state the orchestrator wires
// onto the controller. base.Options holds the config common to every render
// controller (Filter / ParentOf — here resolving each HR to its enclosing KS,
// which reconcile depwaits on so cluster-KS spec patches land before the first
// helm.Template call — RenderTracker / Existence / PreflightFailure);
// AllowMissingSecrets and Producers are HelmRelease-specific.
type ReconcileOptions struct {
	base.Options
	// AllowMissingSecrets omits non-optional valuesFrom refs that point
	// at known live-cluster/generated data or fail to materialize offline.
	AllowMissingSecrets bool
	// Producers is the orchestrator-owned producer index (ExternalSecret /
	// SealedSecret target → producer), shared with the source controller. A
	// valuesFrom Secret with a declared producer is omitted even without
	// AllowMissingSecrets. Nil is OK — no producer is ever known.
	Producers *manifest.ProducerIndex
}

// New constructs a HelmRelease controller.
func New(s *store.Store, t *task.Service, h *helm.Client, opts helm.Options, wipeSecrets bool) *Controller {
	return &Controller{
		Controller:  base.New(s, t, "helmrelease"),
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
	c.producers = opts.Producers
}

// Start registers the listeners. The controller runs until Close.
// The HR controller only listens for HelmRelease and HelmChartSource
// (the chart-ref index) — source-kind events (HelmRepository,
// OCIRepository, GitRepository, Bucket, ExternalArtifact) are now
// consumed lazily by helm.Client through its SourceResolver against
// the canonical Store. One fewer push-registry to keep in sync.
func (c *Controller) Start(_ context.Context) {
	c.StartLifecycle()
	// The producer index is read by the reconcile body (shouldOmitValuesRef),
	// not a dispatch trigger, so this listener stays wired even though the
	// scheduler owns dispatch (via ReconcileNode) — no dispatch listener here.
	c.AddListener(store.EventObjectAdded, c.onRawProducerAdded())
}

// ReconcileNode runs id's reconcile under the dag engine, returning the blocked
// dependency set (nil = terminalized) and whether id ended Ready. The
// orchestrator's scheduler Dispatcher calls this for HelmRelease nodes.
func (c *Controller) ReconcileNode(ctx context.Context, id manifest.NamedResource, drainLevel int) []manifest.NamedResource {
	return base.DispatchNode(ctx, c.Controller, id, drainLevel,
		func(hr *manifest.HelmRelease) bool { return hr.Suspend },
		c.reconcile)
}

// onRawProducerAdded returns a listener that indexes RawObject producers
// (ExternalSecret, SealedSecret, ObjectBucketClaim) into the shared producer
// index. Registered with flush=true (via AddListener) so the index picks up any
// objects already in the store at Start time and stays current as KS renders
// emit more — with the post-kustomize-transform names a discovery-time file
// scan can't see.
//
// The index maps each target → producer.Named() via manifest.ProducerTargets so
// the valuesFrom producer lookup answers in O(1) without a full-store scan.
func (c *Controller) onRawProducerAdded() store.Listener {
	return func(_ manifest.NamedResource, payload any) {
		raw, ok := payload.(*manifest.RawObject)
		if !ok {
			return
		}
		for _, target := range manifest.ProducerTargets(raw) {
			c.producers.Record(target.NamedResource, raw.Named())
		}
	}
}

func (c *Controller) reconcile(ctx context.Context, hr *manifest.HelmRelease) error {
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
	if parent, ok := c.LookupParent(id); ok {
		// The parent's render may have replaced this HR in the store
		// with a patched copy; re-read so the rest of reconcile uses
		// the canonical spec instead of the pre-patch snapshot we
		// were dispatched with.
		fresh, ok, err := base.RequireRefresh[*manifest.HelmRelease](
			ctx, c.Controller, id, hr.Timeout,
			[]manifest.DependencyRef{{NamedResource: parent}},
			"waiting for parent KS",
			func(sum depwait.Summary) error {
				return fmt.Errorf("%w: parent Kustomization %s not ready: %s",
					manifest.ErrObjectNotFound, parent.String(), sum.Messages[parent])
			})
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
	if deps := c.collectHRDeps(hr); len(deps) > 0 {
		// RequireRefresh fuses the dependsOn gate with the load-bearing
		// re-read: the parent KS may have re-rendered (e.g. its own
		// dependsOn cleared in the meantime, freeing up parent-render-time
		// substitutions that mutate our spec). Without that refresh, an HR
		// with explicit spec.dependsOn but no structural parent — or one
		// whose parent re-emitted us after the parent-gate cleared — keeps
		// the pre-mutation snapshot through chart resolution.
		fresh, ok, err := base.RequireRefresh[*manifest.HelmRelease](
			ctx, c.Controller, id, hr.Timeout, deps,
			"resolving dependencies", base.DepFailed(id))
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

	hr, err := c.resolvePreRenderValuesFrom(ctx, id, hr)
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
	// helm.Prepare clones hr, resolves chartRef, and expands values —
	// the same pre-render dance an embedder calling TemplateDocs
	// directly performs. Keeping one canonical implementation here
	// means changes to the contract only land in one place.
	hr, err = helm.Prepare(hr, c.Helm.Resolver().HelmChart, values.NewStoreProvider(c.Store), c.Helm.ValuesCache())
	if err != nil {
		return err
	}

	// Wait for the declared chart source to be Ready, BEFORE the fingerprint
	// dedup gate (a re-emit that finds the prior fingerprint cached must not
	// replay stale docs over a source that has since failed or soft-skipped
	// via --allow-missing-secrets).
	if err := c.awaitChartSource(ctx, id, hr); err != nil {
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
		if err := c.awaitChartSource(ctx, id, hr); err != nil {
			return err
		}
	}
	if err := c.PreflightError(id); err != nil {
		return err
	}

	// Fingerprint dedup (helm.TemplateDocs is the hot path): skip the
	// re-render and replay the cached artifact's side-effects when the
	// effective spec is byte-identical to the cached artifact — the common
	// trigger is a parent KS re-emitting this HR with stamped ownership
	// labels. The shared helper publishes nothing (publish=false; an
	// HR-emitted source CR re-emit is a DeepEqual no-op anyway) — see its doc
	// for the rationale (#657–#660). fp is reused for SetArtifact below.
	fp := helmReleaseFingerprint(hr)
	if handled, err := c.FingerprintDedup(id, fp, func(docs []map[string]any) {
		c.emitRenderedChildren(id, docs, false)
	}); handled {
		return err
	}

	c.Store.UpdateStatus(id, store.StatusPending, "rendering chart")
	if err := c.PreflightError(id); err != nil {
		return err
	}
	docs, err := c.Helm.TemplateDocs(ctx, hr, hr.Values, c.Options)
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
			c.Store.AddWarning(manifest.Warning{
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
		c.Store.AddWarning(manifest.Warning{
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
	// Default namespace-less rendered objects to the release namespace, like
	// helm-controller does on apply — otherwise a chart that omits
	// metadata.namespace renders objects with no namespace, and any version
	// that toggles the explicit field shows up as a spurious delete+add in
	// diff. Cluster-scoped kinds and explicit namespaces are left untouched.
	manifest.StampNamespaces(docs, hr.ReleaseNamespace())
	docs = manifest.DropKinds(docs, c.Options.SkipResourceKinds())

	c.Store.UpdateStatus(id, store.StatusPending, fmt.Sprintf("applying %d objects", len(docs)))
	if err := c.PreflightError(id); err != nil {
		return err
	}
	c.emitRenderedChildren(id, docs, true)

	c.Store.SetArtifact(id, &store.HelmReleaseArtifact{Manifests: docs, Fingerprint: fp})
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
	docs, err := c.Helm.TemplateDocs(ctx, hr, filled, c.Options)
	if err != nil {
		return nil, nil, false
	}
	return docs, paths, true
}

// parentReadsUnreadableSecret reports whether id's structural parent
// Kustomization lists a substituteFrom Secret flate couldn't read offline — the
// signal that an empty value in id's render likely came from an unresolved
// ${VAR} rather than a genuine empty-field mistake.
func (c *Controller) parentReadsUnreadableSecret(id manifest.NamedResource) bool {
	parent, ok := c.LookupParent(id)
	if !ok {
		return false
	}
	ks, ok := c.Store.GetObject(parent).(*manifest.Kustomization)
	if !ok {
		return false
	}
	return len(values.UnreadableSubstituteSecrets(ks, values.NewStoreProvider(c.Store))) > 0
}

// chartSourceID is the resource identity of the HelmRelease's current chart
// source (the thing its render reads from).
func chartSourceID(hr *manifest.HelmRelease) manifest.NamedResource {
	return manifest.NamedResource{
		Kind: hr.Chart.RepoKind, Namespace: hr.Chart.RepoNamespace, Name: hr.Chart.RepoName,
	}
}

// awaitChartSource gates hr on its current chart source reaching Ready, then
// propagates a soft-skip (--allow-missing-secrets on the source's auth marks it
// Ready but writes no artifact, so fail here rather than letting the render fail
// later with a confusing chart-not-found).
//
// The chart source is a status-bearing dependency that flate itself fetches
// (synthetic HelmChart, declared OCIRepository/GitRepository/HelmChart). It is
// gated via Require: an unready source classifies as blocked, so the scheduler
// parks this node and re-runs it when the fetch completes — a slow fetch is
// waited for by OUTCOME instead of dropped.
func (c *Controller) awaitChartSource(ctx context.Context, id manifest.NamedResource, hr *manifest.HelmRelease) error {
	srcID := chartSourceID(hr)
	if err := c.Require(ctx, id, hr.Timeout,
		[]manifest.DependencyRef{{NamedResource: srcID}},
		"", // status already set by the caller
		func(sum depwait.Summary) error {
			return fmt.Errorf("%w: chart source %s not ready: %s",
				manifest.ErrObjectNotFound, srcID.String(), sum.Messages[srcID])
		}); err != nil {
		return err
	}
	if info, ok := c.Store.GetStatus(srcID); ok && store.IsSkipped(info) {
		return fmt.Errorf("%w: chart source %s %s",
			manifest.ErrSourceSkipped, srcID.String(), info.Message)
	}
	return nil
}

// materializeHelmChartSource handles a HelmRelease whose chart source is a
// HelmRepository (HTTP or OCI). A HelmRepository is only a registry/index
// base; the chart name+version live on the HelmRelease, so there's no
// standalone HelmChart CR for the source controller to have fetched. We
// synthesize one, register it (so the source controller fetches the chart —
// with retry — into the Store), and repoint hr.Chart at it. After this, the
// chart-source depwait and LocateChart route through the HelmChart path; the
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
	// RepoKind "" defaults to HelmRepository (see LocateChart dispatch).
	if hr.Chart.RepoKind != "" && hr.Chart.RepoKind != manifest.KindHelmRepository {
		return hr, false
	}
	r := c.Helm.Resolver().HelmRepository(hr.Chart.RepoNamespace, hr.Chart.RepoName)
	if r == nil {
		return hr, false
	}
	syn := helmchart.Synthesize(r, hr.Chart.Name, hr.Chart.Version)
	synID := syn.Named()
	// keep-emit BEFORE AddObject so the synchronous source-controller
	// listener sees the extended changed-only keep set (mirrors
	// emitRenderedChildren).
	c.KeepEmitted(id, syn)
	c.Store.AddObject(syn)
	// Seed a Pending status ONLY on first materialization — when no status
	// exists yet — to close the window between AddObject (which dispatches an
	// async source reconcile) and that reconcile setting the status, so the
	// chart-source depwait below never observes the object as absent. On a
	// re-materialization the status already exists; a byte-identical AddObject
	// is a no-op that fires no EventObjectAdded, so an unconditional re-seed
	// would flap an already-Ready synthetic back to Pending with nothing to
	// re-Ready it (a parent-KS re-emit, or a second HelmRelease that shares the
	// same synthesized chart id), wedging awaitChartSource until timeout.
	// Guarding on absence keeps this idempotent and also lets a previously
	// Failed synthetic surface its real error fast instead of re-Pending.
	if _, ok := c.Store.GetStatus(synID); !ok {
		c.Store.UpdateStatus(synID, store.StatusPending, "fetching chart")
	}
	hr.Chart.RepoKind = manifest.KindHelmChart
	hr.Chart.RepoNamespace = syn.Namespace
	hr.Chart.RepoName = syn.Name
	return hr, true
}

// emitRenderedChildren parses each rendered doc and lands it in the
// store. Source CRs flow through AddObject (their controllers must
// pick them up), other kinds via AddRendered (chart's final
// manifests; nothing else reconciles them). Called both from the
// fresh-render path and the fingerprint-dedup replay path so the
// per-doc side-effects fire on every reconcile pass.
//
// publish gates the event-firing Store.AddObject of source CRs. The
// fresh-render path passes true. The fingerprint-dedup replay passes
// FALSE: the sources were already published byte-identically by the
// render that set the cached artifact (the source controller writes only
// artifact + status, never the stored object), so re-AddObject-ing them
// is a Store.AddObject DeepEqual no-op anyway. The idempotent side-effects
// the replay exists for — KeepEmitted (keep-set) + ReportRendered
// (provenance) — are event-free and still run on both paths. Mirrors the
// KS dedup-replay publish gate (#657).
//
// A single-pass loop is safe here because isFluxSourceKind restricts
// AddObject dispatch to pure source kinds (HelmRepository,
// OCIRepository, GitRepository, Bucket, HelmChartSource,
// ExternalArtifact). None of those fire a reconcile that would race
// a "data first" ordering constraint — unlike KS's two-pass strategy
// which guards leaf KS/HR controllers that read ConfigMap/Secret
// substituteFrom data emitted in the same batch. HR-emitting-HR or
// HR-emitting-KS is deliberately excluded from this controller.
func (c *Controller) emitRenderedChildren(id manifest.NamedResource, docs []map[string]any, publish bool) {
	opts := manifest.ParseDocOptions{WipeSecrets: c.WipeSecrets}
	// Accumulate every source-kind child id so the renderedSet write
	// flushes through MarkRenderedBatch in a single lock acquisition.
	// Charts that emit multiple source CRs (rare today but
	// tofu-controller-style HRs already hit this) used to pay N r.mu
	// acquisitions; one batched write replaces all of them.
	var rendered []manifest.NamedResource
	for _, doc := range docs {
		if manifest.IsEncryptedSecret(doc) {
			name, ns := manifest.DocMetadata(doc)
			c.Logger().Debug("SOPS-encrypted resource wiped to placeholder",
				"id", id.String(), "ref", manifest.DocKind(doc)+" "+ns+"/"+name)
		}
		obj, err := manifest.ParseDoc(doc, opts)
		if err != nil {
			c.Logger().Debug("skipped doc", "id", id.String(), "err", err)
			continue
		}
		if manifest.IsKustomizeBuildDirective(obj) {
			continue // build input, not a cluster resource — never store it
		}
		// docs is retained on the HelmReleaseArtifact.Manifests; do
		// NOT return any entry to the decoded-map pool here.
		if isFluxSourceKind(obj) {
			// keepEmitted BEFORE AddObject: the listener that fires
			// synchronously during AddObject must see the extended
			// keep set when it invokes PreGate. Mirrors the ordering
			// in kustomization.emitRenderedChildren.
			childID := obj.Named()
			c.KeepEmitted(id, obj)
			if publish {
				c.Store.AddObject(obj)
			}
			rendered = append(rendered, childID)
		} else {
			c.Store.AddRendered(obj)
		}
	}
	c.ReportRendered(id, rendered)
}

// collectHRDeps returns hr's typed dependsOn entries (carrying any
// ReadyExpr) for the depwait Waiter. dependsOn on a HelmRelease
// references other HelmReleases only (per Flux spec).
func (c *Controller) collectHRDeps(hr *manifest.HelmRelease) []manifest.DependencyRef {
	if len(hr.DependsOn) == 0 {
		return nil
	}
	deps := slices.Clone(hr.DependsOn)
	// Changed-only mode: a dependsOn target outside the keep-set is
	// unchanged, so its producing Kustomization is skipped and the target
	// HR is never render-emitted into the Store — depwait would report
	// "dependency not found" for a dep that's simply unchanged. Drop it:
	// an unchanged dep is satisfied for a delta check, mirroring how a
	// skipped in-Store resource resolves Ready via base.PreGate. dependsOn
	// is pure reconcile ordering and never affects offline render content
	// (see change.transitiveDeps). Unlike KS deps, HRs have no file-loaded
	// Store object to carry that Ready, hence the prune here. See #517.
	if f := c.Filter(); f != nil && f.Enabled() {
		deps = slices.DeleteFunc(deps, func(d manifest.DependencyRef) bool {
			return !f.ShouldReconcile(d.NamedResource)
		})
	}
	return deps
}

// preparePrereqs collects refs that helm.Prepare reads from the live
// Store via synchronous lookup. The Store is mutated by other
// controllers (parent KS emit, sibling KS render), so a HR observed
// before its referenced HelmChart CRD / valuesFrom CM/Secret would
// hard-fail in Prepare with ErrObjectNotFound. Pre-await each ref's
// existence so the lookup is guaranteed to land.
//
// Excluded: Optional valuesFrom refs (Prepare tolerates their
// absence) and the chart source itself (already awaited explicitly
// after Prepare since it needs Ready, not just exists).
func preparePrereqs(hr *manifest.HelmRelease) []manifest.DependencyRef {
	var out []manifest.DependencyRef

	// chartRef → HelmChart CRD: the unresolved Name=="" placeholder
	// shape (chartFromHelmRelease leaves Name empty for HelmChart
	// chartRefs); Prepare's ResolveChartRef would synchronously read
	// the HelmChartSource from the store to materialize it.
	if hr.Chart.RepoKind == manifest.KindHelmChart && hr.Chart.Version == "" {
		out = append(out, manifest.DependencyRef{NamedResource: manifest.NamedResource{
			Kind: manifest.KindHelmChart, Namespace: hr.Chart.RepoNamespace, Name: hr.Chart.RepoName,
		}})
	}

	for _, ref := range hr.ValuesFrom {
		if ref.Optional {
			continue
		}
		out = append(out, manifest.DependencyRef{NamedResource: manifest.NamedResource{
			Kind: ref.Kind, Namespace: hr.Namespace, Name: ref.Name,
		}})
	}

	return out
}
