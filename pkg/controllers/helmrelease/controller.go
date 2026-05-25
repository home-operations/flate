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
	parentOf       func(manifest.NamedResource) (manifest.NamedResource, bool)
	resolveMissing func(manifest.NamedResource) bool
	isFileIndexed  func(manifest.NamedResource) bool
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
	// ResolveMissing, when non-nil, is forwarded to every depwait
	// Waiter built during reconcile. The orchestrator wires this
	// against the loader's ExistenceIndex so file-indexed sources
	// (HelmRepository, OCIRepository, HelmChart) can be lazy-loaded
	// the moment the HR's chartRef gate needs them.
	ResolveMissing func(manifest.NamedResource) bool
	// IsFileIndexed reports whether id has a file-existence record. The
	// orchestrator wires this so depwait distinguishes "dep is
	// render-only and the producing chain hasn't finished yet"
	// (no file record → keep waiting on per-dep ctx) from "dep is
	// file-indexed but promote failed" (file record → fail fast).
	IsFileIndexed func(manifest.NamedResource) bool
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
	c.resolveMissing = opts.ResolveMissing
	c.isFileIndexed = opts.IsFileIndexed
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
		Store:          c.Store,
		Parent:         id,
		Timeout:        depwait.TimeoutFromSpec(timeout),
		ResolveMissing: c.resolveMissing,
		IsFileIndexed:  c.isFileIndexed,
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
		c.Submit(ctx, id, func(ctx context.Context) {
			base.RunWithStatus(ctx, c.Store, id, "helmrelease", c.reconcile)
		})
	}
}

func (c *Controller) reconcile(ctx context.Context, hr *manifest.HelmRelease) error {
	id := hr.Named()

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
		c.Store.UpdateStatus(id, store.StatusPending, "waiting for parent KS")
		var sum depwait.Summary
		c.Tasks.YieldSlot(func() {
			sum = depwait.WaitAll(c.newWaiter(id, hr.Timeout).Watch(ctx, []manifest.DependencyRef{{NamedResource: parent}}))
		})
		if sum.AnyFailed() {
			return fmt.Errorf("%w: parent Kustomization %s not ready: %s",
				manifest.ErrObjectNotFound, parent.String(), sum.Messages[parent])
		}
		// The parent's render may have replaced this HR in the store
		// with a patched copy; re-read so the rest of reconcile uses
		// the canonical spec instead of the pre-patch snapshot we
		// were dispatched with.
		if obj, ok := store.Get[*manifest.HelmRelease](c.Store, id); ok {
			hr = obj
		}
	}

	// Honor spec.dependsOn — HR-to-HR ordering. Flux gates rendering on
	// each dependency reaching Ready before this HR reconciles.
	// YieldSlot releases the worker-pool slot during the wait so the
	// dependencies can themselves acquire one.
	if deps := c.collectHRDeps(hr); len(deps) > 0 {
		c.Store.UpdateStatus(id, store.StatusPending, "resolving dependencies")
		var sum depwait.Summary
		c.Tasks.YieldSlot(func() {
			sum = depwait.WaitAll(c.newWaiter(id, hr.Timeout).Watch(ctx, deps))
		})
		if sum.AnyFailed() {
			return &manifest.DependencyFailedError{
				Parent:  id,
				Failed:  sum.Failed,
				Reasons: sum.Messages,
			}
		}
		// Re-read the HR after the dependsOn wait: while we were
		// yielding our slot, the parent KS could have re-rendered
		// (e.g. its own dependsOn cleared in the meantime, freeing
		// up parent-render-time substitutions that mutate our
		// spec). Without this refresh, an HR with explicit
		// spec.dependsOn but no structural parent — or one whose
		// parent re-emitted us after the parent-gate cleared —
		// keeps the pre-mutation snapshot through chart
		// resolution. Mirrors the KS controller's single
		// refresh after its combined dep wait.
		if obj, ok := store.Get[*manifest.HelmRelease](c.Store, id); ok {
			hr = obj
		}
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
		slog.Debug("helmrelease: skipped re-render (fingerprint unchanged)", "id", id.String())
		return nil
	}

	// Wait for chart source (HelmRepository / OCIRepository / GitRepository)
	// to be ready. For HelmRepository we wait by existence rather than
	// status — there's no controller updating HelmRepository status.
	srcID := manifest.NamedResource{
		Kind: hr.Chart.RepoKind, Namespace: hr.Chart.RepoNamespace, Name: hr.Chart.RepoName,
	}
	var sum depwait.Summary
	c.Tasks.YieldSlot(func() {
		sum = depwait.WaitAll(c.newWaiter(id, hr.Timeout).Watch(ctx, []manifest.DependencyRef{{NamedResource: srcID}}))
	})
	if sum.AnyFailed() {
		return fmt.Errorf("%w: chart source %s not ready: %s",
			manifest.ErrObjectNotFound, srcID.String(), sum.Messages[srcID])
	}
	// A chart source that soft-skipped (--allow-missing-secrets on its
	// auth) marks Ready but writes no artifact and almost certainly
	// can't be pulled anonymously either. Propagate the skip instead
	// of letting TemplateDocs fail at the registry.
	if info, ok := c.Store.GetStatus(srcID); ok && store.IsSkipped(info) {
		return fmt.Errorf("%w: chart source %s %s",
			manifest.ErrSourceSkipped, srcID.String(), info.Message)
	}

	c.Store.UpdateStatus(id, store.StatusPending, "rendering chart")
	docs, err := c.Helm.TemplateDocs(ctx, hr, hr.Values, c.Options)
	if err != nil {
		return err
	}

	c.Store.UpdateStatus(id, store.StatusPending, fmt.Sprintf("applying %d objects", len(docs)))
	opts := manifest.ParseDocOptions{WipeSecrets: c.WipeSecrets}
	for _, doc := range docs {
		if manifest.IsEncryptedSecret(doc) {
			// SOPS-encrypted Secret in chart output — let ParseSecret
			// wipe its values to PLACEHOLDER below, same as cleartext
			// Secret data when --wipe-secrets is on. flate has no SOPS
			// keys, so the placeholder is the honest rendered value.
			name, ns := manifest.DocMetadata(doc)
			slog.Debug("helmrelease: SOPS-encrypted resource wiped to placeholder",
				"id", id.String(), "ref", manifest.DocKind(doc)+" "+ns+"/"+name)
		}
		obj, err := manifest.ParseDoc(doc, opts)
		if err != nil {
			slog.Debug("helmrelease: skipped doc", "id", id.String(), "err", err)
			continue
		}
		// Source CRs rendered by a chart (e.g. tofu-controller emits an
		// OCIRepository for its TF state bundle) need to flow through
		// the source controller's listener so they actually get
		// fetched and produce a Ready status. Without AddObject the
		// rendered source sits in the Store with no status and
		// `flate test` reports it as "FAILED (no status reported)".
		// Real Flux reconciles these the same way.
		//
		// Other kinds — Deployments, Services, ConfigMaps emitted as
		// chart workload output — go via AddRendered: they're the
		// chart's final cluster manifests, not anything flate
		// reconciles further.
		if isFluxSourceKind(obj) {
			c.Store.AddObject(obj)
		} else {
			c.Store.AddRendered(obj)
		}
	}

	c.Store.SetArtifact(id, &store.HelmReleaseArtifact{Manifests: docs, Fingerprint: fp})
	return nil
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
