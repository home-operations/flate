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
// state (Filter) flows in via Configure exactly once before Start.
type Controller struct {
	*base.Controller

	Helm *helm.Client

	// Options applied to every template call.
	Options helm.Options

	// WipeSecrets controls whether secrets are wiped from rendered
	// templates.
	WipeSecrets bool

}

// ReconcileOptions carries the post-bootstrap state the orchestrator
// wires onto the controller. Filter narrows reconciliation to changed
// HelmReleases (and their referenced sources/values) in changed-only
// mode.
type ReconcileOptions struct {
	Filter *change.Filter
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

	// Honor spec.dependsOn — HR-to-HR ordering. Flux gates rendering on
	// each dependency reaching Ready before this HR reconciles.
	// YieldSlot releases the worker-pool slot during the wait so the
	// dependencies can themselves acquire one.
	if deps := c.collectHRDeps(hr); len(deps) > 0 {
		c.Store.UpdateStatus(id, store.StatusPending, "resolving dependencies")
		var sum depwait.Summary
		c.Tasks.YieldSlot(func() {
			w := &depwait.Waiter{
				Store:   c.Store,
				Parent:  id,
				Timeout: depwait.TimeoutFromSpec(hr.Timeout),
			}
			sum = depwait.WaitAll(w.Watch(ctx, deps))
		})
		if sum.AnyFailed() {
			return &manifest.DependencyFailedError{
				Parent:  id,
				Failed:  sum.Failed,
				Reasons: sum.Messages,
			}
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

	// Wait for chart source (HelmRepository / OCIRepository / GitRepository)
	// to be ready. For HelmRepository we wait by existence rather than
	// status — there's no controller updating HelmRepository status.
	srcID := manifest.NamedResource{
		Kind: hr.Chart.RepoKind, Namespace: hr.Chart.RepoNamespace, Name: hr.Chart.RepoName,
	}
	var sum depwait.Summary
	c.Tasks.YieldSlot(func() {
		w := &depwait.Waiter{
			Store:   c.Store,
			Parent:  id,
			Timeout: depwait.TimeoutFromSpec(hr.Timeout),
		}
		sum = depwait.WaitAll(w.Watch(ctx, []manifest.DependencyRef{{NamedResource: srcID}}))
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
		c.Store.AddRendered(obj)
	}

	c.Store.SetArtifact(id, &store.HelmReleaseArtifact{Manifests: docs})
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

