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
	"log/slog"
	"sync"

	"github.com/home-operations/flate/pkg/change"
	"github.com/home-operations/flate/pkg/controllers/base"
	"github.com/home-operations/flate/pkg/depwait"
	"github.com/home-operations/flate/pkg/helm"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
	"github.com/home-operations/flate/pkg/task"
	"github.com/home-operations/flate/pkg/values"
)

// RenderTracker is the seam the controller uses to report
// "this child id was emitted by THIS parent HR's render" to the
// orchestrator. Nil is OK — the controller no-ops.
//
// Mirrors kustomization.RenderTracker; the parent linkage feeds
// detectOrphans, the parent resolver, and ResourceSet extension
// attribution for charts that render source CRs (tofu-controller's
// OCIRepository pattern, ESO's HelmRepository fallback).
type RenderTracker interface {
	MarkRendered(parent, child manifest.NamedResource)
}

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

	// renderTracker receives every source-kind child emitted during render.
	renderTracker RenderTracker

	// rawProducerIndex is an in-memory index populated by the store
	// listener in Start. It maps the target NamedResource (the Secret
	// that a producer generates) to the producer's own NamedResource
	// (the ExternalSecret or SealedSecret in the store). This replaces
	// the O(N) Store.ListObjects("") scan in generatedValuesProducer with
	// an O(1) sync.Map lookup, eliminating the per-reconcile read-lock
	// contention on the store when --allow-missing-secrets is active.
	//
	// Key:   manifest.NamedResource — the generated Secret's identity
	//        (Kind=KindSecret, Namespace, Name)
	// Value: manifest.NamedResource — the producer's identity
	rawProducerIndex sync.Map
}

// ReconcileOptions carries the post-bootstrap state the orchestrator
// wires onto the controller. Filter narrows reconciliation to changed
// HelmReleases (and their referenced sources/values) in changed-only
// mode. ParentOf resolves each HR to its enclosing KS at lookup time
// (combines the file-loaded path-prefix index with the runtime
// renderedSet); reconcile depwaits on the parent before rendering so
// spec patches (driftDetection / upgrade strategy / CRD policy at the
// cluster KS level, post-build substitutions, kustomize replacements)
// land before the first helm.Template call.
type ReconcileOptions struct {
	Filter   *change.Filter
	ParentOf func(manifest.NamedResource) (manifest.NamedResource, bool)
	// RenderTracker receives every source-kind child emitted during HR
	// render. Mirrors kustomization.Options.RenderTracker; feeds the
	// orchestrator's parent-provenance index (detectOrphans, parent
	// resolver, ResourceSet attribution). Nil is OK — no-op.
	RenderTracker RenderTracker
	// Existence is the file-existence lookup the orchestrator wires
	// against the loader's ExistenceIndex. depwait uses it to lazy-
	// promote file-indexed deps (HelmRepository, OCIRepository,
	// HelmChart, …) and to distinguish render-only deps from typo'd
	// ones at the missing-dep grace boundary. See
	// depwait.ExistenceLookup for the decision matrix. Forwarded
	// to every Waiter built during reconcile.
	Existence depwait.ExistenceLookup
	// Renders is the quiescence signal the orchestrator wires
	// against task.Service.ActiveCount. depwait's step-2 long wait
	// short-circuits to "dependency not found" once Renders reports
	// no other reconcile is in flight.
	Renders depwait.RenderInflight
	// PreflightFailure reports dependency-graph errors discovered by the
	// orchestrator before reconcile. When set for an id, the controller
	// marks the resource Failed and does not render it.
	PreflightFailure func(manifest.NamedResource) (string, bool)
	// AllowMissingSecrets omits non-optional valuesFrom refs that point
	// at known live-cluster/generated data or fail to materialize offline.
	AllowMissingSecrets bool
}

// New constructs a HelmRelease controller.
func New(s *store.Store, t *task.Service, h *helm.Client, opts helm.Options, wipeSecrets bool) *Controller {
	return &Controller{
		Controller:  base.New(s, t),
		Helm:        h,
		Options:     opts,
		WipeSecrets: wipeSecrets,
	}
}

// Configure installs the post-bootstrap state. Panics if called after
// Start — encodes the invariant that reconcile-shaping config is
// read-only once dispatch begins.
func (c *Controller) Configure(opts ReconcileOptions) {
	c.SetFilter(opts.Filter)
	c.SetDepwait(opts.Existence, opts.Renders)
	c.SetPreflight(opts.PreflightFailure)
	c.SetParentOf(opts.ParentOf)
	c.allowMissingSecrets = opts.AllowMissingSecrets
	c.renderTracker = opts.RenderTracker
}

// Start registers the listeners. The controller runs until Close.
// The HR controller only listens for HelmRelease and HelmChartSource
// (the chart-ref index) — source-kind events (HelmRepository,
// OCIRepository, GitRepository, Bucket, ExternalArtifact) are now
// consumed lazily by helm.Client through its SourceResolver against
// the canonical Store. One fewer push-registry to keep in sync.
func (c *Controller) Start(ctx context.Context) {
	c.StartLifecycle("helmrelease")
	c.AddListener(store.EventObjectAdded, c.onRawProducerAdded())
	c.AddListener(store.EventObjectAdded, c.onObjectAdded(ctx))
}

// onRawProducerAdded returns a listener that indexes RawObject producers
// (ExternalSecret, SealedSecret) into rawProducerIndex. Registered with
// flush=true (via AddListener) so the index is populated with any objects
// already in the store at Start time and kept current as new objects arrive.
//
// The index maps targetID → producer.Named(), mirroring the classification
// logic in rawProducesValuesRef so generatedValuesProducer can be answered
// in O(1) without a full-store scan.
func (c *Controller) onRawProducerAdded() store.Listener {
	return func(_ manifest.NamedResource, payload any) {
		raw, ok := payload.(*manifest.RawObject)
		if !ok {
			return
		}
		targetID, ok := rawProducerTargetID(raw)
		if !ok {
			return
		}
		c.rawProducerIndex.Store(targetID, raw.Named())
	}
}

// rawProducerTargetID returns the NamedResource of the Secret that raw
// will produce, or (zero, false) when raw is not a recognised producer kind.
// This mirrors the classification in rawProducesValuesRef but returns the
// target identity rather than a boolean, so the index can be keyed by it.
func rawProducerTargetID(raw *manifest.RawObject) (manifest.NamedResource, bool) {
	switch raw.Kind {
	case "ExternalSecret":
		target, _ := raw.Spec["target"].(map[string]any)
		targetName, _ := target["name"].(string)
		if targetName == "" {
			targetName = raw.Name
		}
		return manifest.NamedResource{
			Kind:      manifest.KindSecret,
			Namespace: raw.Namespace,
			Name:      targetName,
		}, true
	case "SealedSecret":
		targetName := raw.Name
		if tmpl, _ := raw.Spec["template"].(map[string]any); tmpl != nil {
			if metadata, _ := tmpl["metadata"].(map[string]any); metadata != nil {
				if name, _ := metadata["name"].(string); name != "" {
					targetName = name
				}
			}
		}
		return manifest.NamedResource{
			Kind:      manifest.KindSecret,
			Namespace: raw.Namespace,
			Name:      targetName,
		}, true
	default:
		return manifest.NamedResource{}, false
	}
}

func (c *Controller) onObjectAdded(ctx context.Context) store.Listener {
	return func(id manifest.NamedResource, payload any) {
		if id.Kind != manifest.KindHelmRelease {
			return
		}
		hr, ok := payload.(*manifest.HelmRelease)
		if !ok {
			return
		}
		if c.PreGate(id, hr.Suspend) {
			return
		}
		if msg, failed := c.PreflightFailure(id); failed {
			c.Store.UpdateStatus(id, store.StatusFailed, msg)
			return
		}
		c.Submit(ctx, id, func(ctx context.Context) {
			base.RunWithStatus(ctx, c.Store, id, "helmrelease", c.reconcile)
		})
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
		err := c.Await(ctx, id, c.NewWaiter(id, hr.Timeout),
			[]manifest.DependencyRef{{NamedResource: parent}},
			"waiting for parent KS",
			func(sum depwait.Summary) error {
				return fmt.Errorf("%w: parent Kustomization %s not ready: %s",
					manifest.ErrObjectNotFound, parent.String(), sum.Messages[parent])
			})
		if err != nil {
			return err
		}
		// The parent's render may have replaced this HR in the store
		// with a patched copy; re-read so the rest of reconcile uses
		// the canonical spec instead of the pre-patch snapshot we
		// were dispatched with.
		if obj, ok := store.Get[*manifest.HelmRelease](c.Store, id); ok {
			hr = obj
		}
	}
	if err := c.PreflightError(id); err != nil {
		return err
	}

	// Honor spec.dependsOn — HR-to-HR ordering. Flux gates rendering on
	// each dependency reaching Ready before this HR reconciles.
	if deps := c.collectHRDeps(hr); len(deps) > 0 {
		err := c.Await(ctx, id, c.NewWaiter(id, hr.Timeout), deps,
			"resolving dependencies",
			func(sum depwait.Summary) error {
				return &manifest.DependencyFailedError{
					Parent:  id,
					Failed:  sum.Failed,
					Reasons: sum.Messages,
				}
			})
		if err != nil {
			return err
		}
		// Re-read the HR after the dependsOn wait: the parent KS may
		// have re-rendered (e.g. its own dependsOn cleared in the
		// meantime, freeing up parent-render-time substitutions that
		// mutate our spec). Without this refresh, an HR with explicit
		// spec.dependsOn but no structural parent — or one whose
		// parent re-emitted us after the parent-gate cleared — keeps
		// the pre-mutation snapshot through chart resolution.
		if obj, ok := store.Get[*manifest.HelmRelease](c.Store, id); ok {
			hr = obj
		}
	}
	if err := c.PreflightError(id); err != nil {
		return err
	}

	if c.allowMissingSecrets {
		hr = c.omitGeneratedValuesFrom(hr)
	}

	// Pre-Prepare existence waits: helm.Prepare reads from the live
	// Store synchronously, returning ErrObjectNotFound for missing
	// chartRef-HelmChart CRDs and non-optional valuesFrom refs. A
	// legitimate load order — HR observed before the HelmChart CR
	// it references, or before a sibling KS emits its valuesFrom CM —
	// would hard-fail here without waiting. Collect the existence-
	// only deps (no Ready semantics needed; they just have to be in
	// the store before Prepare reads through them) and Await them.
	omittedValuesRefs := map[manifest.NamedResource]struct{}{}
	for {
		preDeps := preparePrereqs(hr)
		if len(preDeps) == 0 {
			break
		}
		var preSum depwait.Summary
		if err := c.Await(ctx, id, c.NewWaiter(id, hr.Timeout), preDeps,
			"awaiting pre-render references",
			func(sum depwait.Summary) error {
				preSum = sum
				return &manifest.DependencyFailedError{
					Parent: id, Failed: sum.Failed, Reasons: sum.Messages,
				}
			}); err != nil {
			if c.allowMissingSecrets {
				if next, ok := c.omitFailedValuesFrom(hr, preSum.Failed); ok {
					for _, omitted := range omittedValuesRefIDs(hr, next) {
						omittedValuesRefs[omitted] = struct{}{}
					}
					hr = next
					continue
				}
			}
			return err
		}
		if obj, ok := store.Get[*manifest.HelmRelease](c.Store, id); ok {
			hr = obj
		}
		if len(omittedValuesRefs) > 0 {
			hr = removeValuesRefs(hr, omittedValuesRefs)
		}
		if c.allowMissingSecrets {
			hr = c.omitGeneratedValuesFrom(hr)
		}
		break
	}
	if err := c.PreflightError(id); err != nil {
		return err
	}

	c.Store.UpdateStatus(id, store.StatusPending, "resolving chart")
	// helm.Prepare clones hr, resolves chartRef, and expands values —
	// the same pre-render dance an embedder calling TemplateDocs
	// directly performs. Keeping one canonical implementation here
	// means changes to the contract only land in one place.
	hr, err := helm.Prepare(hr, c.Helm.Resolver().HelmChart, values.NewStoreProvider(c.Store))
	if err != nil {
		return err
	}

	// Wait for chart source (HelmRepository / OCIRepository / GitRepository)
	// to be ready BEFORE the fingerprint dedup gate. A re-emit that
	// finds the prior fingerprint cached must not replay stale docs
	// when the upstream source has since transitioned to Failed or
	// soft-skipped (--allow-missing-secrets newly applied) — the
	// dedup-replay path otherwise publishes outputs the fresh path
	// would have refused with ErrSourceSkipped.
	srcID := manifest.NamedResource{
		Kind: hr.Chart.RepoKind, Namespace: hr.Chart.RepoNamespace, Name: hr.Chart.RepoName,
	}
	if err := c.Await(ctx, id, c.NewWaiter(id, hr.Timeout),
		[]manifest.DependencyRef{{NamedResource: srcID}},
		"", // status already set above
		func(sum depwait.Summary) error {
			return fmt.Errorf("%w: chart source %s not ready: %s",
				manifest.ErrObjectNotFound, srcID.String(), sum.Messages[srcID])
		}); err != nil {
		return err
	}
	// A chart source that soft-skipped (--allow-missing-secrets on its
	// auth) marks Ready but writes no artifact and almost certainly
	// can't be pulled anonymously either. Propagate the skip instead
	// of letting TemplateDocs fail at the registry.
	if info, ok := c.Store.GetStatus(srcID); ok && store.IsSkipped(info) {
		return fmt.Errorf("%w: chart source %s %s",
			manifest.ErrSourceSkipped, srcID.String(), info.Message)
	}
	if err := c.PreflightError(id); err != nil {
		return err
	}

	// Fingerprint dedup: when the same HR id gets re-AddObject'd with
	// the same effective spec (e.g. the parent KS render stamps
	// kustomize.toolkit.fluxcd.io/{name,namespace} labels onto a
	// previously-loaded HR and re-emits it via Store.AddObject), skip
	// the helm render — its output would be byte-identical. Without
	// this, flate runs helm.Template twice for every HR a parent KS
	// owns, which surfaces as duplicate "warning: cannot overwrite
	// table..." log lines from helm's coalescer and roughly doubles
	// the HR-render time on real-world trees.
	fp := helmReleaseFingerprint(hr)
	if existing, ok := c.Store.GetArtifact(id).(*store.HelmReleaseArtifact); ok && existing.Fingerprint != "" && existing.Fingerprint == fp {
		if err := c.PreflightError(id); err != nil {
			return err
		}
		slog.Debug("helmrelease: skipped re-render (fingerprint unchanged)", "id", id.String())
		// Skip helm.TemplateDocs (the expensive bit), but replay the
		// cached docs through the dispatch loop so any source CRs
		// rendered by the chart (tofu-controller's OCIRepository,
		// crossplane's Provider) re-fire EventObjectAdded for any
		// listener that joined after the original render. Matches the
		// KS-side dedup-replay pattern.
		c.emitRenderedChildren(id, existing.Manifests)
		return nil
	}

	c.Store.UpdateStatus(id, store.StatusPending, "rendering chart")
	if err := c.PreflightError(id); err != nil {
		return err
	}
	docs, err := c.Helm.TemplateDocs(ctx, hr, hr.Values, c.Options)
	if err != nil {
		return err
	}

	c.Store.UpdateStatus(id, store.StatusPending, fmt.Sprintf("applying %d objects", len(docs)))
	if err := c.PreflightError(id); err != nil {
		return err
	}
	c.emitRenderedChildren(id, docs)

	c.Store.SetArtifact(id, &store.HelmReleaseArtifact{Manifests: docs, Fingerprint: fp})
	return nil
}

// emitRenderedChildren parses each rendered doc and lands it in the
// store. Source CRs flow through AddObject (their controllers must
// pick them up), other kinds via AddRendered (chart's final
// manifests; nothing else reconciles them). Called both from the
// fresh-render path and the fingerprint-dedup replay path so the
// per-doc side-effects fire on every reconcile pass.
//
// A single-pass loop is safe here because isFluxSourceKind restricts
// AddObject dispatch to pure source kinds (HelmRepository,
// OCIRepository, GitRepository, Bucket, HelmChartSource,
// ExternalArtifact). None of those fire a reconcile that would race
// a "data first" ordering constraint — unlike KS's two-pass strategy
// which guards leaf KS/HR controllers that read ConfigMap/Secret
// substituteFrom data emitted in the same batch. HR-emitting-HR or
// HR-emitting-KS is deliberately excluded from this controller.
func (c *Controller) emitRenderedChildren(id manifest.NamedResource, docs []map[string]any) {
	opts := manifest.ParseDocOptions{WipeSecrets: c.WipeSecrets}
	for _, doc := range docs {
		if manifest.IsEncryptedSecret(doc) {
			name, ns := manifest.DocMetadata(doc)
			slog.Debug("helmrelease: SOPS-encrypted resource wiped to placeholder",
				"id", id.String(), "ref", manifest.DocKind(doc)+" "+ns+"/"+name)
		}
		obj, err := manifest.ParseDoc(doc, opts)
		if err != nil {
			slog.Debug("helmrelease: skipped doc", "id", id.String(), "err", err)
			continue
		}
		if isFluxSourceKind(obj) {
			// keepEmitted BEFORE AddObject: the listener that fires
			// synchronously during AddObject must see the extended
			// keep set when it invokes PreGate. Mirrors the ordering
			// in kustomization.emitRenderedChildren.
			c.keepEmitted(id, obj.Named())
			c.Store.AddObject(obj)
			c.markRendered(id, obj.Named())
		} else {
			c.Store.AddRendered(obj)
		}
	}
}

// keepEmitted extends the change filter's keep set so render-emitted
// source children pass the changed-only-mode PreGate check. Without
// this, a chart that renders a source CR (tofu-controller's
// OCIRepository, ESO's HelmRepository) would silently drop that child
// from the diff comparison in changed-only mode. Mirrors
// kustomization.keepEmitted (issue #260/#308).
//
// Called BEFORE Store.AddObject so the listener that fires
// synchronously during AddObject sees the extended keep set.
func (c *Controller) keepEmitted(parent, child manifest.NamedResource) {
	if f := c.Filter(); f != nil {
		f.AddEmitted(parent, child)
	}
}

// markRendered reports a parent→child render emission to the
// orchestrator's tracker if one is wired; no-op otherwise.
// Centralizes the nil-check so the reconcile body stays readable.
// Mirrors kustomization.markRendered.
func (c *Controller) markRendered(parent, child manifest.NamedResource) {
	if c.renderTracker != nil {
		c.renderTracker.MarkRendered(parent, child)
	}
}

// collectHRDeps returns hr's typed dependsOn entries (carrying any
// ReadyExpr) for the depwait Waiter. dependsOn on a HelmRelease
// references other HelmReleases only (per Flux spec).
func (c *Controller) collectHRDeps(hr *manifest.HelmRelease) []manifest.DependencyRef {
	if len(hr.DependsOn) == 0 {
		return nil
	}
	return append([]manifest.DependencyRef(nil), hr.DependsOn...)
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
