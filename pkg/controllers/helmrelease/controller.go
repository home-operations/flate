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
	"maps"
	"sync"
	"sync/atomic"

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
	Store *store.Store
	Tasks *task.Service
	Helm  *helm.Client

	// Options applied to every template call.
	Options helm.Options

	// WipeSecrets controls whether secrets are wiped from rendered
	// templates.
	WipeSecrets bool

	// Set via Configure() — see Options.
	filter *change.Filter

	started atomic.Bool
	unsub   []store.Unsubscribe
	coal    *task.Coalescer[manifest.NamedResource]

	chartSourcesMu sync.RWMutex
	chartSources   map[string]*manifest.HelmChartSource
}

// ReconcileOptions carries the post-bootstrap state the orchestrator
// wires onto the controller. Filter narrows reconciliation to changed
// HelmReleases (and their referenced sources/values) in changed-only
// mode.
type ReconcileOptions struct {
	Filter *change.Filter
}

// Configure installs the post-bootstrap state. Panics if called after
// Start — encodes the invariant that reconcile-shaping config is
// read-only once dispatch begins.
func (c *Controller) Configure(opts ReconcileOptions) {
	if c.started.Load() {
		panic("helmrelease controller: Configure called after Start")
	}
	c.filter = opts.Filter
}

// Start registers the listeners. The controller runs until Close.
func (c *Controller) Start(ctx context.Context) {
	c.started.Store(true)
	c.coal = task.NewCoalescer[manifest.NamedResource](c.Tasks)
	c.chartSources = map[string]*manifest.HelmChartSource{}
	c.unsub = append(c.unsub,
		c.Store.AddListener(store.EventObjectAdded, c.onObjectAdded(ctx), true),
		c.Store.AddListener(store.EventArtifactUpdated, c.onArtifactUpdated, true),
	)
}

// Close removes listeners.
func (c *Controller) Close() {
	for _, u := range c.unsub {
		u()
	}
	c.unsub = nil
}

func (c *Controller) onObjectAdded(ctx context.Context) store.Listener {
	return func(id manifest.NamedResource, payload any) {
		// Source-kind listeners re-read from the Store rather than
		// trusting payload, because s.fire dispatches AFTER s.mu is
		// released — two concurrent AddObject calls for the same id
		// could deliver listener payloads in reverse of write order.
		// The store always reflects the latest write under its own
		// lock, so a fresh GetObject is authoritative.
		switch id.Kind {
		case manifest.KindHelmRepository:
			_ = payload
			if r, ok := c.Store.GetObject(id).(*manifest.HelmRepository); ok {
				c.Helm.AddRepo(r)
			}
		case manifest.KindOCIRepository:
			_ = payload
			if r, ok := c.Store.GetObject(id).(*manifest.OCIRepository); ok {
				c.Helm.AddOCIRepo(r)
			}
		case manifest.KindHelmChart:
			_ = payload
			if s, ok := c.Store.GetObject(id).(*manifest.HelmChartSource); ok {
				c.chartSourcesMu.Lock()
				c.chartSources[s.ResourceFullName()] = s
				c.chartSourcesMu.Unlock()
			}
		case manifest.KindHelmRelease:
			hr, ok := payload.(*manifest.HelmRelease)
			if !ok {
				return
			}
			if hr.Suspend {
				c.Store.UpdateStatus(id, store.StatusReady, "suspended")
				return
			}
			if c.filter.Enabled() && !c.filter.ShouldReconcile(id) {
				c.Store.UpdateStatus(id, store.StatusReady, "unchanged")
				return
			}
			c.coal.Submit(ctx, "helmrelease/"+id.String(), id, func(ctx context.Context) {
				base.RunWithStatus(ctx, c.Store, id, "helmrelease", c.reconcile)
			})
		}
	}
}

// onArtifactUpdated registers fetched-artifact sources (GitRepository,
// Bucket, ExternalArtifact) with the helm client so charts referenced
// via the corresponding sourceRef.kind can be loaded from disk.
func (c *Controller) onArtifactUpdated(id manifest.NamedResource, payload any) {
	switch id.Kind {
	case manifest.KindGitRepository, manifest.KindBucket, manifest.KindExternalArtifact:
	default:
		return
	}
	art, ok := payload.(*store.SourceArtifact)
	if !ok || art.LocalPath == "" {
		return
	}
	c.Helm.AddLocalSource(helm.LocalSource{
		Name:      id.Name,
		Namespace: id.Namespace,
		Artifact:  art,
	})
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

	// Clone before mutating: ResolveChartRef rewrites hr.Chart in place
	// and ExpandValueReferences writes hr.Values. The store-owned HR
	// stays canonical so concurrent readers (e.g. dependsOn watchers
	// reading hr.Chart for a sibling reconcile) never see torn state.
	hr = hr.Clone()
	c.Store.UpdateStatus(id, store.StatusPending, "resolving chart")

	helmCharts := c.gatherHelmChartSources()
	if err := hr.ResolveChartRef(helmCharts); err != nil {
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

	c.Store.UpdateStatus(id, store.StatusPending, "resolving values")
	provider := values.NewStoreProvider(c.Store)
	if err := values.ExpandValueReferences(hr, provider); err != nil {
		return err
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

// gatherHelmChartSources returns a snapshot of the HelmChart lookup
// map. The cache is maintained incrementally by onObjectAdded — every
// HelmChart added to the store (initial parse phase or later, e.g. via
// a Kustomization render) flows through the same listener.
func (c *Controller) gatherHelmChartSources() map[string]*manifest.HelmChartSource {
	c.chartSourcesMu.RLock()
	defer c.chartSourcesMu.RUnlock()
	out := make(map[string]*manifest.HelmChartSource, len(c.chartSources))
	maps.Copy(out, c.chartSources)
	return out
}
