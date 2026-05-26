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
	"errors"
	"fmt"
	"log/slog"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/home-operations/flate/pkg/change"
	"github.com/home-operations/flate/pkg/controllers/base"
	"github.com/home-operations/flate/pkg/depwait"
	"github.com/home-operations/flate/pkg/helm"
	"github.com/home-operations/flate/pkg/manifest"
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

	// parentOf resolves each HR to its enclosing Flux KS at lookup
	// time. The closure unifies two parent sources:
	//   - file-loaded HRs: pre-built path-prefix index from
	//     loader.BuildParentIndexForKind.
	//   - render-emitted HRs (chart-of-charts, KS-substituted copies):
	//     the orchestrator's renderedSet.ParentOf, populated when the
	//     parent KS's emitRenderedChildren fires.
	// Nil means "no parent enforcement"; matches pre-#221 behavior.
	parentOf  func(manifest.NamedResource) (manifest.NamedResource, bool)
	existence depwait.ExistenceLookup
	renders   depwait.RenderInflight
	preflight func(manifest.NamedResource) (string, bool)
	// allowMissingSecrets extends the source-controller flag to
	// HelmRelease valuesFrom refs that cannot be resolved offline.
	allowMissingSecrets bool
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
	c.parentOf = opts.ParentOf
	c.existence = opts.Existence
	c.renders = opts.Renders
	c.preflight = opts.PreflightFailure
	c.allowMissingSecrets = opts.AllowMissingSecrets
}

// lookupParent reports the structural parent KS of id via the
// configured resolver, or (zero, false) when no parent exists or no
// resolver was configured.
func (c *Controller) lookupParent(id manifest.NamedResource) (manifest.NamedResource, bool) {
	if c.parentOf == nil {
		return manifest.NamedResource{}, false
	}
	return c.parentOf(id)
}

// newWaiter constructs a depwait.Waiter pre-wired with the
// controller's Store and lazy-promotion fallback, parented to id and
// budgeted from timeout (typically hr.Timeout). Centralizes the
// boilerplate so the parent-KS gate, dependsOn wait, and chart-
// source wait don't drift apart.
func (c *Controller) newWaiter(id manifest.NamedResource, timeout *metav1.Duration) *depwait.Waiter {
	return &depwait.Waiter{
		Store:     c.Store,
		Parent:    id,
		Timeout:   depwait.TimeoutFromSpec(timeout),
		Existence: c.existence,
		Renders:   c.renders,
	}
}

// Start registers the listeners. The controller runs until Close.
// The HR controller only listens for HelmRelease and HelmChartSource
// (the chart-ref index) — source-kind events (HelmRepository,
// OCIRepository, GitRepository, Bucket, ExternalArtifact) are now
// consumed lazily by helm.Client through its SourceResolver against
// the canonical Store. One fewer push-registry to keep in sync.
func (c *Controller) Start(ctx context.Context) {
	c.StartLifecycle("helmrelease")
	c.AddListener(store.EventObjectAdded, c.onObjectAdded(ctx))
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
		if msg, failed := c.preflightFailure(id); failed {
			c.Store.UpdateStatus(id, store.StatusFailed, msg)
			return
		}
		c.Submit(ctx, id, func(ctx context.Context) {
			base.RunWithStatus(ctx, c.Store, id, "helmrelease", c.reconcile)
		})
	}
}

func (c *Controller) preflightFailure(id manifest.NamedResource) (string, bool) {
	if c.preflight == nil {
		return "", false
	}
	return c.preflight(id)
}

func (c *Controller) preflightError(id manifest.NamedResource) error {
	if msg, failed := c.preflightFailure(id); failed {
		return errors.New(msg)
	}
	return nil
}

func (c *Controller) reconcile(ctx context.Context, hr *manifest.HelmRelease) error {
	id := hr.Named()
	if err := c.preflightError(id); err != nil {
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
	if parent, ok := c.lookupParent(id); ok {
		err := c.Await(ctx, id, c.newWaiter(id, hr.Timeout),
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
	if err := c.preflightError(id); err != nil {
		return err
	}

	// Honor spec.dependsOn — HR-to-HR ordering. Flux gates rendering on
	// each dependency reaching Ready before this HR reconciles.
	if deps := c.collectHRDeps(hr); len(deps) > 0 {
		err := c.Await(ctx, id, c.newWaiter(id, hr.Timeout), deps,
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
	if err := c.preflightError(id); err != nil {
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
		if err := c.Await(ctx, id, c.newWaiter(id, hr.Timeout), preDeps,
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
	if err := c.preflightError(id); err != nil {
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
	if err := c.Await(ctx, id, c.newWaiter(id, hr.Timeout),
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
	if err := c.preflightError(id); err != nil {
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
		if err := c.preflightError(id); err != nil {
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
	if err := c.preflightError(id); err != nil {
		return err
	}
	docs, err := c.Helm.TemplateDocs(ctx, hr, hr.Values, c.Options)
	if err != nil {
		return err
	}

	c.Store.UpdateStatus(id, store.StatusPending, fmt.Sprintf("applying %d objects", len(docs)))
	if err := c.preflightError(id); err != nil {
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
			c.Store.AddObject(obj)
		} else {
			c.Store.AddRendered(obj)
		}
	}
}

func (c *Controller) omitGeneratedValuesFrom(hr *manifest.HelmRelease) *manifest.HelmRelease {
	return c.omitValuesFrom(hr, nil, true)
}

func (c *Controller) omitFailedValuesFrom(hr *manifest.HelmRelease, failed []manifest.NamedResource) (*manifest.HelmRelease, bool) {
	failedSet := make(map[manifest.NamedResource]struct{}, len(failed))
	for _, id := range failed {
		failedSet[id] = struct{}{}
	}
	next := c.omitValuesFrom(hr, failedSet, false)
	return next, next != hr
}

func (c *Controller) omitValuesFrom(
	hr *manifest.HelmRelease,
	failed map[manifest.NamedResource]struct{},
	requireProducer bool,
) *manifest.HelmRelease {
	if hr == nil || len(hr.ValuesFrom) == 0 {
		return hr
	}
	filtered := make([]manifest.ValuesReference, 0, len(hr.ValuesFrom))
	omitted := false
	for _, ref := range hr.ValuesFrom {
		id, ok := valuesRefID(hr, ref)
		if !ok {
			filtered = append(filtered, ref)
			continue
		}
		if failed != nil {
			if _, wasFailed := failed[id]; !wasFailed {
				filtered = append(filtered, ref)
				continue
			}
		}
		if c.valuesRefExists(id) {
			filtered = append(filtered, ref)
			continue
		}
		if c.existence != nil && c.existence.IsFileIndexed(id) {
			filtered = append(filtered, ref)
			continue
		}
		producer, hasProducer := c.generatedValuesProducer(id)
		if requireProducer && !hasProducer {
			filtered = append(filtered, ref)
			continue
		}
		omitted = true
		args := []any{"id", hr.Named().String(), "ref", id.String()}
		if hasProducer {
			args = append(args, "producer", producer.String())
		}
		slog.Debug("helmrelease: omitted unavailable valuesFrom ref", args...)
	}
	if !omitted {
		return hr
	}
	out := hr.Clone()
	out.ValuesFrom = filtered
	return out
}

func (c *Controller) valuesRefExists(id manifest.NamedResource) bool {
	return c.Store.GetByName(id.Kind, id.Namespace, id.Name) != nil
}

func (c *Controller) generatedValuesProducer(id manifest.NamedResource) (manifest.NamedResource, bool) {
	for _, obj := range c.Store.ListObjects("") {
		raw, ok := obj.(*manifest.RawObject)
		if !ok || raw.Namespace != id.Namespace {
			continue
		}
		if rawProducesValuesRef(raw, id) {
			return raw.Named(), true
		}
	}
	return manifest.NamedResource{}, false
}

func rawProducesValuesRef(raw *manifest.RawObject, id manifest.NamedResource) bool {
	switch raw.Kind {
	case "ExternalSecret":
		if id.Kind != manifest.KindSecret {
			return false
		}
		target, _ := raw.Spec["target"].(map[string]any)
		targetName, _ := target["name"].(string)
		if targetName == "" {
			targetName = raw.Name
		}
		return targetName == id.Name
	case "SealedSecret":
		if id.Kind != manifest.KindSecret {
			return false
		}
		targetName := raw.Name
		if tmpl, _ := raw.Spec["template"].(map[string]any); tmpl != nil {
			if metadata, _ := tmpl["metadata"].(map[string]any); metadata != nil {
				if name, _ := metadata["name"].(string); name != "" {
					targetName = name
				}
			}
		}
		return targetName == id.Name
	default:
		return false
	}
}

func valuesRefID(hr *manifest.HelmRelease, ref manifest.ValuesReference) (manifest.NamedResource, bool) {
	if ref.Optional || ref.Name == "" {
		return manifest.NamedResource{}, false
	}
	switch ref.Kind {
	case manifest.KindSecret, manifest.KindConfigMap:
		return manifest.NamedResource{Kind: ref.Kind, Namespace: hr.Namespace, Name: ref.Name}, true
	default:
		return manifest.NamedResource{}, false
	}
}

func omittedValuesRefIDs(before, after *manifest.HelmRelease) []manifest.NamedResource {
	if before == nil || after == nil {
		return nil
	}
	kept := make(map[manifest.NamedResource]struct{}, len(after.ValuesFrom))
	for _, ref := range after.ValuesFrom {
		if id, ok := valuesRefID(after, ref); ok {
			kept[id] = struct{}{}
		}
	}
	var out []manifest.NamedResource
	for _, ref := range before.ValuesFrom {
		id, ok := valuesRefID(before, ref)
		if !ok {
			continue
		}
		if _, ok := kept[id]; !ok {
			out = append(out, id)
		}
	}
	return out
}

func removeValuesRefs(hr *manifest.HelmRelease, ids map[manifest.NamedResource]struct{}) *manifest.HelmRelease {
	if hr == nil || len(ids) == 0 || len(hr.ValuesFrom) == 0 {
		return hr
	}
	filtered := make([]manifest.ValuesReference, 0, len(hr.ValuesFrom))
	omitted := false
	for _, ref := range hr.ValuesFrom {
		id, ok := valuesRefID(hr, ref)
		if ok {
			if _, drop := ids[id]; drop {
				omitted = true
				continue
			}
		}
		filtered = append(filtered, ref)
	}
	if !omitted {
		return hr
	}
	out := hr.Clone()
	out.ValuesFrom = filtered
	return out
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
